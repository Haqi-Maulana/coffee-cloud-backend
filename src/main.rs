//! CoffeeSense IoT – Cloud Backend
//!
//! * Axum REST API for the dashboard and OTA firmware distribution
//! * MQTT subscriber that ingests ESP32-S3 telemetry automatically
//! * Persistence: Microsoft Azure SQL Database (via `tiberius`)

use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::Context;
use axum::{
    extract::{DefaultBodyLimit, Multipart, Path, Query, State},
    http::{header, HeaderName, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Local, Utc};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use tiberius::{Client, Config, Query as SqlQuery, Row};
use tokio::{net::TcpStream, sync::Mutex};
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use tracing::{error, info, warn};

// ════════════════════════════════════════════════════════════
// Azure SQL connection (auto-reconnecting single connection)
// ════════════════════════════════════════════════════════════

type SqlClient = Client<Compat<TcpStream>>;

#[derive(Clone)]
struct Db {
    conn_str: Arc<String>,
    client: Arc<Mutex<Option<SqlClient>>>,
}

async fn open_connection(config: Config) -> Result<SqlClient, tiberius::error::Error> {
    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    Client::connect(config, tcp.compat_write()).await
}

impl Db {
    fn new(conn_str: String) -> Self {
        Self {
            conn_str: Arc::new(conn_str),
            client: Arc::new(Mutex::new(None)),
        }
    }

    async fn connect(&self) -> anyhow::Result<SqlClient> {
        let config = Config::from_ado_string(&self.conn_str)
            .context("AZURE_SQL_CONNECTION_STRING is not a valid ADO.NET connection string")?;

        match open_connection(config.clone()).await {
            Ok(client) => Ok(client),
            // Azure SQL gateway may redirect to the actual database node.
            Err(tiberius::error::Error::Routing { host, port }) => {
                let mut redirected = config;
                redirected.host(&host);
                redirected.port(port);
                Ok(open_connection(redirected).await?)
            }
            Err(e) => Err(e).context("failed to connect to Azure SQL Database"),
        }
    }

    async fn session(&self) -> anyhow::Result<tokio::sync::MutexGuard<'_, Option<SqlClient>>> {
        let mut guard = self.client.lock().await;
        if guard.is_none() {
            *guard = Some(self.connect().await?);
            info!("Connected to Azure SQL Database");
        }
        Ok(guard)
    }

    /// Runs a query and returns all rows of the first result set.
    async fn rows(&self, query: SqlQuery<'_>) -> anyhow::Result<Vec<Row>> {
        let mut guard = self.session().await?;
        let client = guard.as_mut().expect("session established");
        let result = async { query.query(client).await?.into_first_result().await }.await;
        if result.is_err() {
            *guard = None; // force reconnect on next call
        }
        Ok(result?)
    }

    /// Executes a statement and returns the number of affected rows.
    async fn execute(&self, query: SqlQuery<'_>) -> anyhow::Result<u64> {
        let mut guard = self.session().await?;
        let client = guard.as_mut().expect("session established");
        let result = query.execute(client).await.map(|r| r.total());
        if result.is_err() {
            *guard = None;
        }
        Ok(result?)
    }

    async fn ping(&self) -> anyhow::Result<()> {
        self.rows(SqlQuery::new("SELECT 1 AS ok")).await.map(|_| ())
    }

    async fn init_schema(&self) -> anyhow::Result<()> {
        let sql = "IF OBJECT_ID(N'dbo.coffee_predictions', N'U') IS NULL \
            BEGIN \
                CREATE TABLE dbo.coffee_predictions ( \
                    id UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_cp_id DEFAULT NEWSEQUENTIALID() CONSTRAINT PK_coffee_predictions PRIMARY KEY, \
                    device_id NVARCHAR(64) NOT NULL, \
                    session_id NVARCHAR(64) NULL, \
                    predicted_class NVARCHAR(32) NOT NULL, \
                    confidence FLOAT NOT NULL, \
                    temperature FLOAT NULL, \
                    humidity FLOAT NULL, \
                    co2 FLOAT NULL, \
                    voc FLOAT NULL, \
                    nh3 FLOAT NULL, \
                    c6h6 FLOAT NULL, \
                    validation_status NVARCHAR(32) NULL, \
                    ground_truth NVARCHAR(32) NULL, \
                    received_at DATETIMEOFFSET NOT NULL CONSTRAINT DF_cp_received DEFAULT SYSDATETIMEOFFSET() \
                ); \
            END; \
            IF OBJECT_ID(N'dbo.ota_updates', N'U') IS NULL \
            BEGIN \
                CREATE TABLE dbo.ota_updates ( \
                    id INT IDENTITY(1,1) CONSTRAINT PK_ota_updates PRIMARY KEY, \
                    firmware_version NVARCHAR(32) NOT NULL CONSTRAINT UQ_ota_version UNIQUE, \
                    target_device NVARCHAR(64) NULL, \
                    binary_data VARBINARY(MAX) NOT NULL, \
                    checksum_sha256 NVARCHAR(64) NOT NULL, \
                    file_size_bytes BIGINT NOT NULL, \
                    release_notes NVARCHAR(MAX) NULL, \
                    is_active BIT NOT NULL CONSTRAINT DF_ota_active DEFAULT 0, \
                    created_at DATETIMEOFFSET NOT NULL CONSTRAINT DF_ota_created DEFAULT SYSDATETIMEOFFSET() \
                ); \
            END;";
        self.execute(SqlQuery::new(sql)).await?;
        Ok(())
    }
}

// ════════════════════════════════════════════════════════════
// Models
// ════════════════════════════════════════════════════════════

#[derive(Debug, Clone, Serialize)]
struct Prediction {
    id: String,
    device_id: String,
    session_id: Option<String>,
    predicted_class: String,
    confidence: f64,
    temperature: Option<f64>,
    humidity: Option<f64>,
    co2: Option<f64>,
    voc: Option<f64>,
    nh3: Option<f64>,
    c6h6: Option<f64>,
    validation_status: Option<String>,
    ground_truth: Option<String>,
    received_at: DateTime<Utc>,
}

/// JSON published by the ESP32-S3 (MQTT) or POSTed to `/api/telemetry`.
#[derive(Debug, Deserialize)]
struct TelemetryPayload {
    device_id: String,
    predicted_class: String,
    confidence: f64,
    session_id: Option<String>,
    temperature: Option<f64>,
    humidity: Option<f64>,
    co2: Option<f64>,
    voc: Option<f64>,
    nh3: Option<f64>,
    c6h6: Option<f64>,
    validation_status: Option<String>,
    ground_truth: Option<String>,
}

#[derive(Debug, Serialize)]
struct OtaMeta {
    id: i32,
    firmware_version: String,
    target_device: Option<String>,
    checksum_sha256: String,
    file_size_bytes: i64,
    release_notes: Option<String>,
    is_active: bool,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct RangeQuery {
    minutes: Option<i32>,
    limit: Option<i32>,
    device_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DeviceQuery {
    device_id: Option<String>,
}

#[derive(Clone)]
struct AppState {
    db: Db,
}

// ════════════════════════════════════════════════════════════
// Error handling
// ════════════════════════════════════════════════════════════

struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "error": self.1 }))).into_response()
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        error!("{e:#}");
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"))
    }
}

fn bad_request(e: impl std::fmt::Display) -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, e.to_string())
}

fn not_found(what: &str) -> ApiError {
    ApiError(StatusCode::NOT_FOUND, format!("{what} not found"))
}

type ApiResult<T> = Result<T, ApiError>;

// ════════════════════════════════════════════════════════════
// Data access
// ════════════════════════════════════════════════════════════

const PREDICTION_COLS: &str = "CONVERT(NVARCHAR(36), id) AS id, device_id, session_id, \
     predicted_class, confidence, temperature, humidity, co2, voc, nh3, c6h6, \
     validation_status, ground_truth, received_at";

const OTA_COLS: &str = "id, firmware_version, target_device, checksum_sha256, \
     file_size_bytes, release_notes, is_active, created_at";

fn get_str(row: &Row, col: &str) -> anyhow::Result<Option<String>> {
    Ok(row.try_get::<&str, _>(col)?.map(str::to_owned))
}

fn get_f64(row: &Row, col: &str) -> anyhow::Result<Option<f64>> {
    Ok(row.try_get::<f64, _>(col)?)
}

fn row_to_prediction(row: &Row) -> anyhow::Result<Prediction> {
    Ok(Prediction {
        id: get_str(row, "id")?.unwrap_or_default(),
        device_id: get_str(row, "device_id")?.unwrap_or_default(),
        session_id: get_str(row, "session_id")?,
        predicted_class: get_str(row, "predicted_class")?.unwrap_or_default(),
        confidence: get_f64(row, "confidence")?.unwrap_or(0.0),
        temperature: get_f64(row, "temperature")?,
        humidity: get_f64(row, "humidity")?,
        co2: get_f64(row, "co2")?,
        voc: get_f64(row, "voc")?,
        nh3: get_f64(row, "nh3")?,
        c6h6: get_f64(row, "c6h6")?,
        validation_status: get_str(row, "validation_status")?,
        ground_truth: get_str(row, "ground_truth")?,
        received_at: row
            .try_get::<DateTime<Utc>, _>("received_at")?
            .unwrap_or_else(Utc::now),
    })
}

fn row_to_ota(row: &Row) -> anyhow::Result<OtaMeta> {
    Ok(OtaMeta {
        id: row.try_get::<i32, _>("id")?.unwrap_or_default(),
        firmware_version: get_str(row, "firmware_version")?.unwrap_or_default(),
        target_device: get_str(row, "target_device")?,
        checksum_sha256: get_str(row, "checksum_sha256")?.unwrap_or_default(),
        file_size_bytes: row.try_get::<i64, _>("file_size_bytes")?.unwrap_or_default(),
        release_notes: get_str(row, "release_notes")?,
        is_active: row.try_get::<bool, _>("is_active")?.unwrap_or(false),
        created_at: row
            .try_get::<DateTime<Utc>, _>("created_at")?
            .unwrap_or_else(Utc::now),
    })
}

fn default_session_id() -> String {
    format!("DAQ-{}", Local::now().format("%Y-%m%d"))
}

async fn insert_telemetry(db: &Db, p: TelemetryPayload) -> anyhow::Result<()> {
    anyhow::ensure!(!p.device_id.trim().is_empty(), "device_id must not be empty");
    anyhow::ensure!(!p.predicted_class.trim().is_empty(), "predicted_class must not be empty");

    // Accept both 0..1 and 0..100 confidence formats.
    let confidence = if p.confidence > 1.0 { p.confidence / 100.0 } else { p.confidence }.clamp(0.0, 1.0);
    let session = p
        .session_id
        .map(|s| s.trim().trim_start_matches('#').to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(default_session_id);

    let mut q = SqlQuery::new(
        "INSERT INTO dbo.coffee_predictions \
         (device_id, session_id, predicted_class, confidence, temperature, humidity, \
          co2, voc, nh3, c6h6, validation_status, ground_truth) \
         VALUES (@P1, @P2, @P3, @P4, @P5, @P6, @P7, @P8, @P9, @P10, @P11, @P12)",
    );
    q.bind(p.device_id);
    q.bind(session);
    q.bind(p.predicted_class);
    q.bind(confidence);
    q.bind(p.temperature);
    q.bind(p.humidity);
    q.bind(p.co2);
    q.bind(p.voc);
    q.bind(p.nh3);
    q.bind(p.c6h6);
    q.bind(p.validation_status);
    q.bind(p.ground_truth);
    db.execute(q).await?;
    Ok(())
}

async fn fetch_latest(db: &Db, n: i32, device: Option<String>) -> anyhow::Result<Vec<Prediction>> {
    let mut q = SqlQuery::new(format!(
        "SELECT TOP (@P1) {PREDICTION_COLS} FROM dbo.coffee_predictions \
         WHERE (@P2 IS NULL OR device_id = @P2) ORDER BY received_at DESC"
    ));
    q.bind(n);
    q.bind(device);
    db.rows(q).await?.iter().map(row_to_prediction).collect()
}

async fn fetch_range(
    db: &Db,
    minutes: i32,
    limit: i32,
    device: Option<String>,
) -> anyhow::Result<Vec<Prediction>> {
    let mut q = SqlQuery::new(format!(
        "SELECT TOP (@P1) {PREDICTION_COLS} FROM dbo.coffee_predictions \
         WHERE received_at >= DATEADD(MINUTE, -@P2, SYSDATETIMEOFFSET()) \
           AND (@P3 IS NULL OR device_id = @P3) \
         ORDER BY received_at DESC"
    ));
    q.bind(limit);
    q.bind(minutes);
    q.bind(device);
    db.rows(q).await?.iter().map(row_to_prediction).collect()
}

// ════════════════════════════════════════════════════════════
// Telemetry handlers
// ════════════════════════════════════════════════════════════

async fn health(State(st): State<AppState>) -> Json<serde_json::Value> {
    let db_ok = match st.db.ping().await {
        Ok(()) => true,
        Err(e) => {
            warn!("health check: {e:#}");
            false
        }
    };
    Json(json!({
        "status": if db_ok { "healthy" } else { "degraded" },
        "database": if db_ok { "azure-sql: connected" } else { "azure-sql: unreachable" },
        "timestamp": Utc::now().to_rfc3339(),
    }))
}

async fn post_telemetry(
    State(st): State<AppState>,
    Json(payload): Json<TelemetryPayload>,
) -> ApiResult<(StatusCode, Json<serde_json::Value>)> {
    insert_telemetry(&st.db, payload).await?;
    Ok((StatusCode::CREATED, Json(json!({ "status": "stored" }))))
}

async fn latest_prediction(
    State(st): State<AppState>,
    Query(q): Query<DeviceQuery>,
) -> ApiResult<Json<Prediction>> {
    fetch_latest(&st.db, 1, q.device_id)
        .await?
        .into_iter()
        .next()
        .map(Json)
        .ok_or_else(|| not_found("prediction"))
}

async fn list_predictions(
    State(st): State<AppState>,
    Query(q): Query<RangeQuery>,
) -> ApiResult<Json<Vec<Prediction>>> {
    let minutes = q.minutes.unwrap_or(60).clamp(1, 525_600);
    let limit = q.limit.unwrap_or(500).clamp(1, 5_000);
    Ok(Json(fetch_range(&st.db, minutes, limit, q.device_id).await?))
}

/// One round-trip for the dashboard: 2 latest rows (for deltas) + range data (chart/logs).
async fn dashboard(
    State(st): State<AppState>,
    Query(q): Query<RangeQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    let minutes = q.minutes.unwrap_or(5).clamp(1, 1_440);
    let limit = q.limit.unwrap_or(1_200).clamp(1, 5_000);
    let latest = fetch_latest(&st.db, 2, q.device_id.clone()).await?;
    let readings = fetch_range(&st.db, minutes, limit, q.device_id).await?;
    Ok(Json(json!({
        "latest": latest,
        "readings": readings,
        "server_time": Utc::now(),
    })))
}

fn csv_field(v: &str) -> String {
    if v.contains([',', '"', '\n']) {
        format!("\"{}\"", v.replace('"', "\"\""))
    } else {
        v.to_string()
    }
}

fn csv_num(v: Option<f64>) -> String {
    v.map(|x| format!("{x:.2}")).unwrap_or_default()
}

async fn export_csv(
    State(st): State<AppState>,
    Query(q): Query<RangeQuery>,
) -> ApiResult<Response> {
    let minutes = q.minutes.unwrap_or(1_440).clamp(1, 525_600);
    let rows = fetch_range(&st.db, minutes, 100_000, q.device_id).await?;

    let mut csv = String::from(
        "received_at,device_id,session_id,predicted_class,confidence,temperature,humidity,\
         co2,voc,nh3,c6h6,validation_status,ground_truth\n",
    );
    for p in rows {
        csv.push_str(&format!(
            "{},{},{},{},{:.4},{},{},{},{},{},{},{},{}\n",
            p.received_at.to_rfc3339(),
            csv_field(&p.device_id),
            csv_field(p.session_id.as_deref().unwrap_or("")),
            csv_field(&p.predicted_class),
            p.confidence,
            csv_num(p.temperature),
            csv_num(p.humidity),
            csv_num(p.co2),
            csv_num(p.voc),
            csv_num(p.nh3),
            csv_num(p.c6h6),
            csv_field(p.validation_status.as_deref().unwrap_or("")),
            csv_field(p.ground_truth.as_deref().unwrap_or("")),
        ));
    }

    let filename = format!("coffeesense_{}.csv", Local::now().format("%Y%m%d_%H%M%S"));
    Ok((
        [
            (header::CONTENT_TYPE, "text/csv; charset=utf-8".to_string()),
            (header::CONTENT_DISPOSITION, format!("attachment; filename=\"{filename}\"")),
        ],
        csv,
    )
        .into_response())
}

// ════════════════════════════════════════════════════════════
// OTA handlers
// ════════════════════════════════════════════════════════════

async fn ota_upload(
    State(st): State<AppState>,
    mut multipart: Multipart,
) -> ApiResult<(StatusCode, Json<OtaMeta>)> {
    let (mut version, mut target, mut notes, mut binary) = (None, None, None, None);

    while let Some(field) = multipart.next_field().await.map_err(bad_request)? {
        let name = field.name().unwrap_or_default().to_string();
        match name.as_str() {
            "version" => version = Some(field.text().await.map_err(bad_request)?),
            "target_device" => target = Some(field.text().await.map_err(bad_request)?),
            "release_notes" => notes = Some(field.text().await.map_err(bad_request)?),
            "firmware" => binary = Some(field.bytes().await.map_err(bad_request)?.to_vec()),
            _ => {}
        }
    }

    let version = version
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| bad_request("field 'version' is required"))?;
    let binary: Vec<u8> = binary
        .filter(|b| !b.is_empty())
        .ok_or_else(|| bad_request("field 'firmware' (binary file) is required"))?;
    let target = target.map(|t| t.trim().to_string()).filter(|t| !t.is_empty());

    let checksum = hex::encode(Sha256::digest(&binary));
    let size = binary.len() as i64;

    let mut q = SqlQuery::new(format!(
        "INSERT INTO dbo.ota_updates \
         (firmware_version, target_device, binary_data, checksum_sha256, file_size_bytes, release_notes) \
         OUTPUT {} VALUES (@P1, @P2, @P3, @P4, @P5, @P6)",
        OTA_COLS
            .split(", ")
            .map(|c| format!("INSERTED.{c}"))
            .collect::<Vec<_>>()
            .join(", ")
    ));
    q.bind(version.clone());
    q.bind(target);
    q.bind(binary);
    q.bind(checksum);
    q.bind(size);
    q.bind(notes);

    let meta = st
        .db
        .rows(q)
        .await?
        .first()
        .map(row_to_ota)
        .transpose()?
        .ok_or_else(|| ApiError(StatusCode::INTERNAL_SERVER_ERROR, "insert returned no row".into()))?;

    info!(version = %version, size, "firmware uploaded");
    Ok((StatusCode::CREATED, Json(meta)))
}

async fn ota_list(State(st): State<AppState>) -> ApiResult<Json<Vec<OtaMeta>>> {
    let q = SqlQuery::new(format!("SELECT {OTA_COLS} FROM dbo.ota_updates ORDER BY created_at DESC"));
    let list = st.db.rows(q).await?.iter().map(row_to_ota).collect::<anyhow::Result<_>>()?;
    Ok(Json(list))
}

async fn ota_latest(
    State(st): State<AppState>,
    Query(q): Query<DeviceQuery>,
) -> ApiResult<Json<OtaMeta>> {
    let mut sql = SqlQuery::new(format!(
        "SELECT TOP 1 {OTA_COLS} FROM dbo.ota_updates \
         WHERE is_active = 1 AND (@P1 IS NULL OR target_device IS NULL OR target_device = @P1) \
         ORDER BY created_at DESC"
    ));
    sql.bind(q.device_id);
    st.db
        .rows(sql)
        .await?
        .first()
        .map(row_to_ota)
        .transpose()?
        .map(Json)
        .ok_or_else(|| not_found("active firmware"))
}

async fn ota_download(
    State(st): State<AppState>,
    Path(version): Path<String>,
) -> ApiResult<Response> {
    let mut q = SqlQuery::new(
        "SELECT binary_data, checksum_sha256 FROM dbo.ota_updates WHERE firmware_version = @P1",
    );
    q.bind(version.clone());
    let rows = st.db.rows(q).await?;
    let row = rows.first().ok_or_else(|| not_found("firmware"))?;

    let data = row
        .try_get::<&[u8], _>("binary_data")
        .map_err(anyhow::Error::from)?
        .map(<[u8]>::to_vec)
        .unwrap_or_default();
    let checksum = get_str(row, "checksum_sha256")?.unwrap_or_default();

    Ok((
        [
            (header::CONTENT_TYPE, "application/octet-stream".to_string()),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"firmware-{version}.bin\""),
            ),
            (HeaderName::from_static("x-checksum-sha256"), checksum),
        ],
        data,
    )
        .into_response())
}

async fn ota_activate(
    State(st): State<AppState>,
    Path(version): Path<String>,
) -> ApiResult<Json<serde_json::Value>> {
    // Single atomic statement: activate target, deactivate everything else.
    let mut q = SqlQuery::new(
        "IF EXISTS (SELECT 1 FROM dbo.ota_updates WHERE firmware_version = @P1) \
         UPDATE dbo.ota_updates \
            SET is_active = CASE WHEN firmware_version = @P1 THEN 1 ELSE 0 END \
          WHERE is_active = 1 OR firmware_version = @P1",
    );
    q.bind(version.clone());
    if st.db.execute(q).await? == 0 {
        return Err(not_found("firmware"));
    }
    info!(version = %version, "firmware activated");
    Ok(Json(json!({ "status": "activated", "version": version })))
}

// ════════════════════════════════════════════════════════════
// MQTT subscriber
// ════════════════════════════════════════════════════════════

async fn run_mqtt(db: Db, host: String, port: u16, topic: String) {
    let client_id = format!("coffeesense-backend-{}", Utc::now().timestamp_millis());
    let mut opts = MqttOptions::new(client_id, host.clone(), port);
    opts.set_keep_alive(Duration::from_secs(30));
    if let (Ok(user), Ok(pass)) = (std::env::var("MQTT_USERNAME"), std::env::var("MQTT_PASSWORD")) {
        opts.set_credentials(user, pass);
    }

    let (client, mut eventloop) = AsyncClient::new(opts, 64);

    loop {
        match eventloop.poll().await {
            // (Re)subscribe on every successful (re)connect.
            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                info!("MQTT connected to {host}:{port}, subscribing to '{topic}'");
                if let Err(e) = client.subscribe(topic.clone(), QoS::AtLeastOnce).await {
                    error!("MQTT subscribe failed: {e}");
                }
            }
            Ok(Event::Incoming(Packet::Publish(msg))) => {
                match serde_json::from_slice::<TelemetryPayload>(&msg.payload) {
                    Ok(payload) => {
                        let db = db.clone();
                        tokio::spawn(async move {
                            if let Err(e) = insert_telemetry(&db, payload).await {
                                error!("failed to store MQTT telemetry: {e:#}");
                            }
                        });
                    }
                    Err(e) => warn!("invalid MQTT payload on '{}': {e}", msg.topic),
                }
            }
            Ok(_) => {}
            Err(e) => {
                warn!("MQTT connection error: {e} – retrying in 5s");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

// ════════════════════════════════════════════════════════════
// Entry point
// ════════════════════════════════════════════════════════════

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "coffee_cloud_backend=info,tower_http=info".into()),
        )
        .init();

    let conn_str = std::env::var("AZURE_SQL_CONNECTION_STRING")
        .context("AZURE_SQL_CONNECTION_STRING must be set (ADO.NET connection string from Azure Portal)")?;
    let db = Db::new(conn_str);

    if let Err(e) = db.ping().await {
        warn!("Azure SQL not reachable yet ({e:#}); will retry on each request");
    } else {
        if let Err(e) = db.init_schema().await {
            warn!("Failed to auto-initialize schema ({e:#})");
        } else {
            info!("Azure SQL database schema verified / initialized successfully");
        }
    }

    match std::env::var("MQTT_HOST") {
        Ok(host) if !host.trim().is_empty() => {
            let port = std::env::var("MQTT_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(1883);
            let topic = std::env::var("MQTT_TOPIC").unwrap_or_else(|_| "coffee/telemetry".into());
            tokio::spawn(run_mqtt(db.clone(), host, port, topic));
        }
        _ => info!("MQTT_HOST not set – MQTT subscriber disabled (use POST /api/telemetry)"),
    }

    let cors = CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any);

    let app = Router::new()
        .route("/api/health", get(health))
        .route("/api/telemetry", post(post_telemetry))
        .route("/api/prediction/latest", get(latest_prediction))
        .route("/api/predictions", get(list_predictions))
        .route("/api/dashboard", get(dashboard))
        .route("/api/export/csv", get(export_csv))
        .route("/api/ota", get(ota_list))
        .route(
            "/api/ota/upload",
            post(ota_upload).layer(DefaultBodyLimit::max(16 * 1024 * 1024)),
        )
        .route("/api/ota/latest", get(ota_latest))
        .route("/api/ota/download/:version", get(ota_download))
        .route("/api/ota/activate/:version", post(ota_activate))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(AppState { db });

    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".into());
    let default_addr = format!("0.0.0.0:{}", port);
    let addr: SocketAddr = std::env::var("BIND_ADDR")
        .unwrap_or(default_addr)
        .parse()
        .context("invalid BIND_ADDR")?;

    info!("CoffeeSense backend listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}