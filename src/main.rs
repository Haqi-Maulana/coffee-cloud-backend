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
    routing::{get, patch, post},
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
    services::{ServeDir, ServeFile},
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

#[derive(Debug, Deserialize)]
struct UpdateQcPayload {
    validation_status: String,
    ground_truth: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeviceCommand {
    id: i64,
    device_id: String,
    command: String,
    params: Option<serde_json::Value>,
    status: String,
    created_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
struct SendCommandPayload {
    device_id: String,
    command: String,
    params: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct AckCommandPayload {
    command_id: i64,
    status: Option<String>,
}

#[derive(Clone)]
struct AppState {
    db: Db,
    commands: Arc<Mutex<Vec<DeviceCommand>>>,
    mqtt_tx: Option<AsyncClient>,
    devices: DeviceMap,
    started: std::time::Instant,
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
    let mut patch = JsonMap::new();
    patch.insert(
        "live".into(),
        json!({ "v": [null,null,null,null,null,null,null,null], "temp_c": payload.temperature, "rh_pct": payload.humidity }),
    );
    let dev_id = payload.device_id.clone();
    insert_telemetry(&st.db, payload).await?;
    touch_device(&st.devices, &dev_id, patch).await;
    Ok((StatusCode::CREATED, Json(json!({ "status": "stored" }))))
}

async fn telemetry_info() -> impl IntoResponse {
    Json(json!({
        "status": "online",
        "service": "CoffeeSense IoT - Cloud Telemetry Ingestion API",
        "method": "POST",
        "instructions": "Send an HTTP POST request to this endpoint with a JSON body from your ESP32-S3.",
        "example_payload": {
            "device_id": "ESP32-S3-01",
            "predicted_class": "Arabica",
            "confidence": 0.94,
            "temperature": 28.5,
            "humidity": 62.0,
            "co2": 415.0,
            "voc": 110.0,
            "nh3": 12.0,
            "c6h6": 4.2
        },
        "links": {
            "web_dashboard": "/",
            "health_check": "/api/health",
            "latest_prediction": "/api/prediction/latest",
            "dashboard_metrics": "/api/dashboard"
        }
    }))
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

async fn update_prediction_qc(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Json(payload): Json<UpdateQcPayload>,
) -> ApiResult<Json<serde_json::Value>> {
    let mut q = SqlQuery::new(
        "UPDATE dbo.coffee_predictions \
         SET validation_status = @P1, ground_truth = @P2 \
         WHERE CONVERT(NVARCHAR(36), id) = @P3",
    );
    q.bind(payload.validation_status.clone());
    q.bind(payload.ground_truth.clone());
    q.bind(id.clone());
    let affected = st.db.execute(q).await?;
    info!(id = %id, status = %payload.validation_status, affected, "QC updated");
    Ok(Json(json!({
        "status": "updated",
        "id": id,
        "validation_status": payload.validation_status,
        "ground_truth": payload.ground_truth,
        "affected_rows": affected
    })))
}

async fn post_device_command(
    State(st): State<AppState>,
    Json(payload): Json<SendCommandPayload>,
) -> ApiResult<(StatusCode, Json<DeviceCommand>)> {
    let cmd = DeviceCommand {
        id: Utc::now().timestamp_millis(),
        device_id: payload.device_id.clone(),
        command: payload.command.clone(),
        params: payload.params.clone(),
        status: "pending".into(),
        created_at: Utc::now(),
    };

    // Publish via MQTT if client is available
    if let Some(ref mqtt) = st.mqtt_tx {
        let cmd_topic = std::env::var("MQTT_COMMAND_TOPIC")
            .unwrap_or_else(|_| format!("coffee/command/{}", payload.device_id));
        if let Ok(bytes) = serde_json::to_vec(&cmd) {
            let mqtt_clone = mqtt.clone();
            tokio::spawn(async move {
                let _ = mqtt_clone.publish(cmd_topic, QoS::AtLeastOnce, false, bytes).await;
            });
        }
    }

    let mut guard = st.commands.lock().await;
    guard.push(cmd.clone());
    if guard.len() > 200 {
        guard.remove(0);
    }

    info!(device_id = %cmd.device_id, command = %cmd.command, "Device command queued");
    Ok((StatusCode::CREATED, Json(cmd)))
}

async fn get_device_command(
    State(st): State<AppState>,
    Query(q): Query<DeviceQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    let mut guard = st.commands.lock().await;
    let target_dev = q.device_id.as_deref().unwrap_or("");
    let mut pending = Vec::new();

    for c in guard.iter_mut() {
        if c.status == "pending"
            && (target_dev.is_empty() || c.device_id == target_dev || c.device_id == "ALL")
        {
            c.status = "dispatched".into();
            pending.push(c.clone());
        }
    }

    let recent: Vec<DeviceCommand> = guard.iter().rev().take(15).cloned().collect();
    Ok(Json(json!({
        "pending_commands": pending,
        "recent_history": recent,
        "server_time": Utc::now(),
    })))
}

async fn ack_device_command(
    State(st): State<AppState>,
    Json(payload): Json<AckCommandPayload>,
) -> ApiResult<Json<serde_json::Value>> {
    let mut guard = st.commands.lock().await;
    let new_status = payload.status.unwrap_or_else(|| "executed".into());
    for c in guard.iter_mut() {
        if c.id == payload.command_id {
            c.status = new_status.clone();
        }
    }
    Ok(Json(json!({
        "status": "acknowledged",
        "command_id": payload.command_id,
        "new_status": new_status
    })))
}

// ════════════════════════════════════════════════════════════
// MQTT subscriber
// ════════════════════════════════════════════════════════════

fn start_mqtt(db: Db, host: String, port: u16, topic: String) -> AsyncClient {
    let client_id = format!("coffeesense-backend-{}", Utc::now().timestamp_millis());
    let mut opts = MqttOptions::new(client_id, host.clone(), port);
    opts.set_keep_alive(Duration::from_secs(30));
    if let (Ok(user), Ok(pass)) = (std::env::var("MQTT_USERNAME"), std::env::var("MQTT_PASSWORD")) {
        opts.set_credentials(user, pass);
    }

    let (client, mut eventloop) = AsyncClient::new(opts, 64);
    let sub_client = client.clone();

    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                // (Re)subscribe on every successful (re)connect.
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    info!("MQTT connected to {host}:{port}, subscribing to '{topic}'");
                    if let Err(e) = sub_client.subscribe(topic.clone(), QoS::AtLeastOnce).await {
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
    });

    client
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

    let mqtt_tx = match std::env::var("MQTT_HOST") {
        Ok(host) if !host.trim().is_empty() => {
            let port = std::env::var("MQTT_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(1883);
            let topic = std::env::var("MQTT_TOPIC").unwrap_or_else(|_| "coffee/telemetry".into());
            Some(start_mqtt(db.clone(), host, port, topic))
        }
        _ => {
            info!("MQTT_HOST not set – MQTT subscriber disabled (use POST /api/telemetry)");
            None
        }
    };

    let static_dir = std::env::var("STATIC_DIR").unwrap_or_else(|_| "static".into());
    let static_service = ServeDir::new(&static_dir)
        .not_found_service(ServeFile::new(format!("{static_dir}/index.html")));

    let cors = CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any);

    let app = Router::new()
        .route("/api/status", get(api_status))
        .route("/api/auth/check", get(auth_check))
        .route("/api/devices", get(api_devices))
        .route("/api/devices/:id/live", get(device_live))
        .route("/api/devices/:id/commands", post(device_post_command).get(device_poll_command))
        .route("/api/devices/:id/heartbeat", post(device_heartbeat))
        .route("/api/devices/:id/ack", post(device_ack))
        .route("/api/measurements", get(api_measurements))
        .route("/api/measurements/:id", get(api_measurement_one))
        .route("/api/measurements/:id/qc", patch(api_patch_qc))
        .route("/api/stats", get(api_stats))
        .route("/api/ota/packages", get(api_ota_packages))
        .route("/api/ota/assign", post(api_ota_assign))
        .route("/api/ota/devices/:id", get(api_ota_device))
        .route("/api/export/metadata.csv", get(export_csv))
        .route("/api/export/measurements.json", get(api_export_json))
        .route("/api/health", get(health))
        .route("/api/telemetry", post(post_telemetry).get(telemetry_info))
        .route("/api/prediction/latest", get(latest_prediction))
        .route("/api/predictions", get(list_predictions))
        .route("/api/predictions/:id/qc", post(update_prediction_qc))
        .route("/api/dashboard", get(dashboard))
        .route("/api/export/csv", get(export_csv))
        .route("/api/device/command", post(post_device_command).get(get_device_command))
        .route("/api/device/command/ack", post(ack_device_command))
        .route("/api/ota", get(ota_list))
        .route(
            "/api/ota/upload",
            post(ota_upload).layer(DefaultBodyLimit::max(16 * 1024 * 1024)),
        )
        .route("/api/ota/latest", get(ota_latest))
        .route("/api/ota/download/:version", get(ota_download))
        .route("/api/ota/activate/:version", post(ota_activate))
        .with_state(AppState {
            db,
            commands: Arc::new(Mutex::new(Vec::new())),
            mqtt_tx,
            devices: Arc::new(Mutex::new(HashMap::new())),
            started: std::time::Instant::now(),
        })
        .fallback_service(static_service)
        .layer(cors)
        .layer(TraceLayer::new_for_http());

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

// ════════════════════════════════════════════════════════════
// Reference-compatible API (Smart Coffee E-Nose contract)
//
//   GET  /api/status
//   GET  /api/devices                      GET  /api/devices/:id/live?since=
//   POST /api/devices/:id/commands         {type: zero_baseline|check_ota|reboot|set_config|stop, ...}
//   POST /api/devices/:id/heartbeat        (ESP32-S3 → cloud; returns {command})
//   GET  /api/devices/:id/commands         (ESP32-S3 polls pending command)
//   POST /api/devices/:id/ack              {command_id, status, message}
//   GET  /api/measurements[?limit&offset]  GET  /api/measurements/:id
//   PATCH /api/measurements/:id/qc         {qc_flag, notes}
//   GET  /api/stats                        GET  /api/auth/check
//   GET  /api/ota/packages                 POST /api/ota/assign   GET /api/ota/devices/:id
//   GET  /api/export/metadata.csv          GET  /api/export/measurements.json
// ════════════════════════════════════════════════════════════

use std::collections::HashMap;

type JsonMap = serde_json::Map<String, serde_json::Value>;

#[derive(Default)]
struct DeviceRec {
    data: JsonMap,
    first_seen: Option<DateTime<Utc>>,
    last_seen: Option<DateTime<Utc>>,
    pending: Option<serde_json::Value>,
    last_ack: Option<serde_json::Value>,
    live: Vec<serde_json::Value>,
}

type DeviceMap = Arc<Mutex<HashMap<String, DeviceRec>>>;

const ONLINE_WINDOW_S: f64 = 20.0;

fn device_json(id: &str, rec: &DeviceRec) -> serde_json::Value {
    let now = Utc::now();
    let secs = rec
        .last_seen
        .map(|t| (now - t).num_milliseconds() as f64 / 1000.0)
        .unwrap_or(f64::MAX);
    let mut m: JsonMap = JsonMap::new();
    m.insert("fw_title".into(), json!("smart-coffee-enose"));
    m.insert("fw_version".into(), json!(null));
    m.insert("state".into(), json!("idle"));
    m.insert("ip".into(), json!(null));
    m.insert("mac".into(), json!(null));
    m.insert("rssi".into(), json!(null));
    m.insert("free_heap".into(), json!(null));
    m.insert("model_ready".into(), json!(false));
    m.insert("sample_period_ms".into(), json!(1000));
    m.insert("samples_done".into(), json!(0));
    m.insert("samples_total".into(), json!(0));
    m.insert("measurement_id".into(), json!(null));
    m.insert("zero_baseline".into(), json!(null));
    m.insert("zero_baseline_at".into(), json!(null));
    m.insert("sensors".into(), json!({"ads_a": false, "ads_b": false, "dht": false}));
    m.insert("ota".into(), json!({"state": "IDLE", "progress": 0.0, "message": null}));
    m.insert(
        "live".into(),
        json!({"rh_pct": null, "temp_c": null, "v": [null,null,null,null,null,null,null,null]}),
    );
    for (k, v) in rec.data.iter() {
        m.insert(k.clone(), v.clone());
    }
    m.insert("device_id".into(), json!(id));
    m.insert("first_seen".into(), json!(rec.first_seen));
    m.insert("last_seen".into(), json!(rec.last_seen));
    m.insert("online".into(), json!(secs < ONLINE_WINDOW_S));
    m.insert(
        "seconds_since_seen".into(),
        if secs.is_finite() && secs < 1e12 { json!(secs) } else { json!(null) },
    );
    m.insert("pending_command".into(), rec.pending.clone().unwrap_or(json!(null)));
    m.insert("last_ack".into(), rec.last_ack.clone().unwrap_or(json!(null)));
    serde_json::Value::Object(m)
}

/// Merge a heartbeat / telemetry snapshot into the device registry.
async fn touch_device(devices: &DeviceMap, id: &str, patch: JsonMap) {
    let mut g = devices.lock().await;
    let rec = g.entry(id.to_string()).or_default();
    let now = Utc::now();
    rec.first_seen.get_or_insert(now);
    rec.last_seen = Some(now);
    for (k, v) in patch {
        if k == "live" {
            if let Some(obj) = v.as_object() {
                rec.live.push(json!({
                    "t": now.timestamp_millis(),
                    "v": obj.get("v").cloned().unwrap_or(json!([])),
                    "temp_c": obj.get("temp_c").cloned().unwrap_or(json!(null)),
                    "rh_pct": obj.get("rh_pct").cloned().unwrap_or(json!(null)),
                }));
                if rec.live.len() > 600 {
                    rec.live.remove(0);
                }
            }
        }
        rec.data.insert(k, v);
    }
}

fn operator_key_ok(headers: &axum::http::HeaderMap) -> bool {
    match std::env::var("OPERATOR_KEY") {
        Ok(k) if !k.trim().is_empty() => headers
            .get("x-operator-key")
            .and_then(|v| v.to_str().ok())
            .map(|v| v == k)
            .unwrap_or(false),
        _ => true, // no key configured → open (demo mode)
    }
}

async fn auth_check(headers: axum::http::HeaderMap) -> Json<serde_json::Value> {
    let configured = std::env::var("OPERATOR_KEY").map(|k| !k.trim().is_empty()).unwrap_or(false);
    Json(json!({ "ok": operator_key_ok(&headers), "required": configured }))
}

async fn api_status(State(st): State<AppState>) -> Json<serde_json::Value> {
    let t0 = std::time::Instant::now();
    let db_ok = st.db.ping().await.is_ok();
    let db_ms = t0.elapsed().as_secs_f64() * 1000.0;
    let readings = if db_ok {
        fetch_range(&st.db, 525_600, 100_000, None).await.map(|r| r.len()).unwrap_or(0)
    } else {
        0
    };
    let devices: Vec<serde_json::Value> = {
        let g = st.devices.lock().await;
        let mut v: Vec<_> = g.iter().map(|(id, r)| device_json(id, r)).collect();
        v.sort_by(|a, b| a["device_id"].as_str().cmp(&b["device_id"].as_str()));
        v
    };
    Json(json!({
        "backend": {
            "ok": true,
            "latency_ms": 0.0,
            "server_time": Utc::now().to_rfc3339(),
            "uptime_s": st.started.elapsed().as_secs(),
            "version": env!("CARGO_PKG_VERSION"),
        },
        "database": {
            "kind": "azure-sql",
            "ok": db_ok,
            "latency_ms": db_ms,
            "readings": readings,
            "samples": 0,
            "sessions": readings,
        },
        "devices": devices,
        "fw_title": "smart-coffee-enose",
        "model": {
            "name": "Edge Impulse (on-device TinyML)",
            "version": "none",
            "confidence_threshold": 0.6,
            "metric": null,
            "validation": null,
        },
        "mqtt": { "configured": st.mqtt_tx.is_some() },
    }))
}

async fn api_devices(State(st): State<AppState>) -> Json<serde_json::Value> {
    let g = st.devices.lock().await;
    let mut v: Vec<_> = g.iter().map(|(id, r)| device_json(id, r)).collect();
    v.sort_by(|a, b| a["device_id"].as_str().cmp(&b["device_id"].as_str()));
    Json(json!(v))
}

#[derive(Debug, Deserialize)]
struct LiveQuery {
    since: Option<i64>,
}

async fn device_live(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<LiveQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    let g = st.devices.lock().await;
    let rec = g.get(&id).ok_or_else(|| not_found("device"))?;
    let since = q.since.unwrap_or(0);
    let samples: Vec<_> = rec
        .live
        .iter()
        .filter(|s| s["t"].as_i64().unwrap_or(0) > since)
        .cloned()
        .collect();
    Ok(Json(json!({ "device": device_json(&id, rec), "samples": samples })))
}

/// Dashboard → device command (zero_baseline | check_ota | reboot | set_config | stop).
async fn device_post_command(
    State(st): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> ApiResult<(StatusCode, Json<serde_json::Value>)> {
    if !operator_key_ok(&headers) {
        return Err(ApiError(StatusCode::UNAUTHORIZED, "operator key required".into()));
    }
    let ctype = body
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| bad_request("field 'type' is required"))?
        .to_string();
    let cmd_id = Utc::now().timestamp_millis();
    let mut cmd = body.as_object().cloned().unwrap_or_default();
    cmd.insert("id".into(), json!(cmd_id));
    cmd.insert("device_id".into(), json!(id));
    cmd.insert("type".into(), json!(ctype));
    cmd.insert("status".into(), json!("pending"));
    cmd.insert("created_at".into(), json!(Utc::now()));
    let cmd = serde_json::Value::Object(cmd);

    {
        let mut g = st.devices.lock().await;
        let rec = g.entry(id.clone()).or_default();
        rec.pending = Some(cmd.clone());
    }
    // legacy history list + MQTT publish
    {
        let mut h = st.commands.lock().await;
        h.push(DeviceCommand {
            id: cmd_id,
            device_id: id.clone(),
            command: ctype.clone(),
            params: Some(cmd.clone()),
            status: "pending".into(),
            created_at: Utc::now(),
        });
        if h.len() > 200 {
            h.remove(0);
        }
    }
    if let Some(mqtt) = st.mqtt_tx.clone() {
        let topic = std::env::var("MQTT_COMMAND_TOPIC")
            .unwrap_or_else(|_| format!("coffee/command/{id}"));
        if let Ok(bytes) = serde_json::to_vec(&cmd) {
            tokio::spawn(async move {
                let _ = mqtt.publish(topic, QoS::AtLeastOnce, false, bytes).await;
            });
        }
    }
    info!(device = %id, command = %ctype, "command queued");
    Ok((StatusCode::ACCEPTED, Json(cmd)))
}

/// ESP32-S3 → cloud heartbeat. Body may contain: state, fw_version, rssi, ip, mac,
/// free_heap, sensors{}, live{v[8],temp_c,rh_pct}, samples_done, samples_total, ota{}, zero_baseline...
/// Response carries the pending command (if any), which is then marked dispatched.
async fn device_heartbeat(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let patch = body.as_object().cloned().unwrap_or_default();
    touch_device(&st.devices, &id, patch).await;
    let cmd = {
        let mut g = st.devices.lock().await;
        g.get_mut(&id).and_then(|r| r.pending.take())
    };
    if let Some(c) = &cmd {
        let cid = c["id"].as_i64().unwrap_or(0);
        let mut h = st.commands.lock().await;
        for x in h.iter_mut() {
            if x.id == cid {
                x.status = "dispatched".into();
            }
        }
    }
    Json(json!({ "ok": true, "command": cmd, "server_time": Utc::now() }))
}

async fn device_poll_command(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> Json<serde_json::Value> {
    let cmd = {
        let mut g = st.devices.lock().await;
        let rec = g.entry(id.clone()).or_default();
        rec.last_seen = Some(Utc::now());
        rec.first_seen.get_or_insert(Utc::now());
        rec.pending.take()
    };
    Json(json!({ "command": cmd }))
}

async fn device_ack(
    State(st): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let cid = body.get("command_id").and_then(|v| v.as_i64()).unwrap_or(0);
    let status = body.get("status").and_then(|v| v.as_str()).unwrap_or("executed").to_string();
    {
        let mut g = st.devices.lock().await;
        let rec = g.entry(id.clone()).or_default();
        rec.last_ack = Some(json!({
            "command_id": cid, "status": status,
            "message": body.get("message").cloned().unwrap_or(json!(null)),
            "at": Utc::now(),
        }));
        if let Some(zb) = body.get("zero_baseline") {
            rec.data.insert("zero_baseline".into(), zb.clone());
            rec.data.insert("zero_baseline_at".into(), json!(Utc::now()));
        }
    }
    let mut h = st.commands.lock().await;
    for x in h.iter_mut() {
        if x.id == cid {
            x.status = status.clone();
        }
    }
    Json(json!({ "ok": true }))
}

// ── measurements (mapped from dbo.coffee_predictions) ─────────────────

fn species_of(class: &str) -> &'static str {
    let c = class.to_lowercase();
    if c.contains("arab") { "Arabica" }
    else if c.contains("robu") { "Robusta" }
    else if c.contains("libe") { "Liberica" }
    else { "?" }
}

fn measurement_json(p: &Prediction) -> serde_json::Value {
    json!({
        "measurement_id": p.id,
        "session_id": p.session_id,
        "device_id": p.device_id,
        "daq_id": p.device_id,
        "coffee_id": null,
        "batch_id": null,
        "species": species_of(&p.predicted_class),
        "status": "completed",
        "qc_flag": p.validation_status.clone().unwrap_or_else(|| "OK".into()),
        "notes": p.ground_truth,
        "duration_s": 500,
        "dry_run": false,
        "created_at": p.received_at,
        "finished_at": p.received_at,
        "result": {
            "predicted_class": p.predicted_class,
            "confidence": p.confidence,
            "temperature": p.temperature,
            "humidity": p.humidity,
            "co2": p.co2, "voc": p.voc, "nh3": p.nh3, "c6h6": p.c6h6,
        }
    })
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    limit: Option<i32>,
    offset: Option<usize>,
    device_id: Option<String>,
}

async fn api_measurements(
    State(st): State<AppState>,
    Query(q): Query<ListQuery>,
) -> ApiResult<Json<serde_json::Value>> {
    let all = fetch_range(&st.db, 525_600, 5_000, q.device_id).await?;
    let total = all.len();
    let off = q.offset.unwrap_or(0);
    let lim = q.limit.unwrap_or(200).clamp(1, 5_000) as usize;
    let items: Vec<_> = all.iter().skip(off).take(lim).map(measurement_json).collect();
    Ok(Json(json!({ "items": items, "total": total })))
}

async fn api_measurement_one(
    State(st): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<serde_json::Value>> {
    let mut q = SqlQuery::new(format!(
        "SELECT {PREDICTION_COLS} FROM dbo.coffee_predictions WHERE CONVERT(NVARCHAR(36), id) = @P1"
    ));
    q.bind(id);
    let rows = st.db.rows(q).await?;
    let p = rows.first().map(row_to_prediction).transpose()?.ok_or_else(|| not_found("measurement"))?;
    Ok(Json(measurement_json(&p)))
}

#[derive(Debug, Deserialize)]
struct PatchQc {
    qc_flag: String,
    notes: Option<String>,
}

async fn api_patch_qc(
    State(st): State<AppState>,
    headers: axum::http::HeaderMap,
    Path(id): Path<String>,
    Json(b): Json<PatchQc>,
) -> ApiResult<Json<serde_json::Value>> {
    if !operator_key_ok(&headers) {
        return Err(ApiError(StatusCode::UNAUTHORIZED, "operator key required".into()));
    }
    let flag = b.qc_flag.to_uppercase();
    if !["OK", "SUSPECT", "REJECT"].contains(&flag.as_str()) {
        return Err(bad_request("qc_flag must be OK, SUSPECT or REJECT"));
    }
    let mut q = SqlQuery::new(
        "UPDATE dbo.coffee_predictions SET validation_status = @P1, ground_truth = @P2 \
         WHERE CONVERT(NVARCHAR(36), id) = @P3",
    );
    q.bind(flag.clone());
    q.bind(b.notes.clone());
    q.bind(id.clone());
    if st.db.execute(q).await? == 0 {
        return Err(not_found("measurement"));
    }
    Ok(Json(json!({ "measurement_id": id, "qc_flag": flag, "notes": b.notes })))
}

async fn api_stats(State(st): State<AppState>) -> ApiResult<Json<serde_json::Value>> {
    let all = fetch_range(&st.db, 525_600, 5_000, None).await?;
    let mut by_class: HashMap<String, u32> = HashMap::new();
    let mut by_qc: HashMap<String, u32> = HashMap::new();
    let mut conf_sum = 0.0;
    for p in &all {
        *by_class.entry(p.predicted_class.clone()).or_default() += 1;
        *by_qc.entry(p.validation_status.clone().unwrap_or_else(|| "OK".into())).or_default() += 1;
        conf_sum += p.confidence;
    }
    Ok(Json(json!({
        "total": all.len(),
        "by_predicted_class": by_class,
        "by_qc": by_qc,
        "by_status": { "completed": all.len() },
        "latest": all.first().map(measurement_json),
        "mean_confidence": if all.is_empty() { None } else { Some(conf_sum / all.len() as f64) },
        "mean_inference_ms": null,
    })))
}

// ── OTA (mapped from dbo.ota_updates) ────────────────────────────────

async fn api_ota_packages(State(st): State<AppState>) -> ApiResult<Json<serde_json::Value>> {
    let q = SqlQuery::new(format!("SELECT {OTA_COLS} FROM dbo.ota_updates ORDER BY created_at DESC"));
    let list = st.db.rows(q).await?.iter().map(row_to_ota).collect::<anyhow::Result<Vec<_>>>()?;
    let out: Vec<_> = list
        .iter()
        .map(|o| {
            json!({
                "id": o.firmware_version,
                "version": o.firmware_version,
                "title": "smart-coffee-enose",
                "file_name": format!("firmware-{}.bin", o.firmware_version),
                "data_size": o.file_size_bytes,
                "checksum": o.checksum_sha256,
                "created_time": o.created_at.timestamp_millis(),
                "has_data": true,
                "is_active": o.is_active,
                "release_notes": o.release_notes,
            })
        })
        .collect();
    Ok(Json(json!(out)))
}

#[derive(Debug, Deserialize)]
struct OtaAssign {
    device_id: String,
    package_id: String,
}

/// Activate package + tell the device to check OTA.
async fn api_ota_assign(
    State(st): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(b): Json<OtaAssign>,
) -> ApiResult<Json<serde_json::Value>> {
    if !operator_key_ok(&headers) {
        return Err(ApiError(StatusCode::UNAUTHORIZED, "operator key required".into()));
    }
    let res = ota_activate(State(st.clone()), Path(b.package_id.clone())).await?;
    let cmd = json!({
        "id": Utc::now().timestamp_millis(), "device_id": b.device_id,
        "type": "check_ota", "version": b.package_id, "status": "pending",
        "created_at": Utc::now(),
    });
    st.devices.lock().await.entry(b.device_id.clone()).or_default().pending = Some(cmd);
    Ok(Json(json!({ "ok": true, "activated": res.0, "device_id": b.device_id })))
}

async fn api_ota_device(State(st): State<AppState>, Path(id): Path<String>) -> Json<serde_json::Value> {
    let g = st.devices.lock().await;
    let ota = g
        .get(&id)
        .map(|r| device_json(&id, r)["ota"].clone())
        .unwrap_or(json!({"state": "IDLE", "progress": 0.0, "message": null}));
    Json(json!({ "device_id": id, "ota": ota }))
}

async fn api_export_json(State(st): State<AppState>) -> ApiResult<Json<serde_json::Value>> {
    let all = fetch_range(&st.db, 525_600, 100_000, None).await?;
    Ok(Json(json!(all.iter().map(measurement_json).collect::<Vec<_>>())))
}