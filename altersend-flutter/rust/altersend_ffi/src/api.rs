use std::path::PathBuf;
use std::sync::Arc;

use altersend_domain::SelectedFile;
use altersend_engine::AlterSendEngine;
use flutter_rust_bridge::frb;
use tokio::sync::Mutex;

static ENGINE: Mutex<Option<Arc<AlterSendEngine>>> = Mutex::const_new(None);

async fn engine() -> Result<Arc<AlterSendEngine>, String> {
    let guard = ENGINE.lock().await;
    guard
        .as_ref()
        .cloned()
        .ok_or_else(|| "Call init_engine first".to_string())
}

#[frb(init)]
pub fn init_app() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info")
        .try_init();
}

#[frb]
pub async fn init_engine(storage_path: String) -> Result<(), String> {
    let engine = Arc::new(AlterSendEngine::new(PathBuf::from(storage_path))?);
    engine.boot().await?;
    *ENGINE.lock().await = Some(engine);
    Ok(())
}

/// JSON-encoded [`altersend_domain::TransferUiSnapshot`].
#[frb]
pub async fn get_session_state_json() -> Result<String, String> {
    engine().await?.session_state_json().await
}

#[frb]
pub async fn get_ui_snapshot_json() -> Result<String, String> {
    let snapshot = engine().await?.snapshot().await;
    serde_json::to_string(&snapshot).map_err(|e| e.to_string())
}

#[frb]
pub async fn add_selected_files(
    paths: Vec<String>,
    names: Vec<String>,
    sizes: Vec<u64>,
) -> Result<(), String> {
    if paths.len() != names.len() || paths.len() != sizes.len() {
        return Err("paths, names, sizes length mismatch".into());
    }
    let files: Vec<SelectedFile> = paths
        .into_iter()
        .zip(names)
        .zip(sizes)
        .map(|((path, name), size)| SelectedFile { path, name, size })
        .collect();
    engine().await?.add_selected_files(files).await;
    Ok(())
}

#[frb]
pub async fn continue_share() -> Result<(), String> {
    engine().await?.continue_share().await
}

#[frb]
pub async fn join_session(join_code: String) -> Result<(), String> {
    engine().await?.join(&join_code).await
}

#[frb]
pub async fn download_all_files() -> Result<(), String> {
    engine().await?.download_all().await
}

#[frb]
pub async fn remove_selected_file(path: String) -> Result<(), String> {
    engine().await?.remove_selected_file(path).await;
    Ok(())
}

#[frb]
pub async fn clear_session() -> Result<(), String> {
    engine().await?.disconnect().await
}

#[frb]
pub fn is_valid_join_code(code: String) -> bool {
    altersend_domain::is_valid_join_code(&code)
}

#[frb]
pub fn extract_join_code(text: String) -> Option<String> {
    altersend_domain::extract_join_code(&text)
}

#[frb]
pub fn build_join_url(topic: String) -> String {
    altersend_domain::build_join_url(&topic)
}

#[frb]
pub async fn can_join_from_deep_link(code: String) -> bool {
    engine()
        .await
        .map(|e| e.can_join_from_deep_link(&code))
        .unwrap_or(false)
}
