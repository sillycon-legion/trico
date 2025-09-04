use std::{
    collections::HashMap,
    ffi::OsStr,
    io::ErrorKind,
    path::PathBuf,
    process::{ExitStatus, Stdio},
    sync::Arc,
    time::Duration,
};

#[cfg(not(target_os = "windows"))]
use std::os::unix::fs::PermissionsExt;

use axum::{
    Router,
    extract::{Path, State},
    response::ErrorResponse,
    routing::post,
};
use axum_extra::{
    TypedHeader,
    headers::{Authorization, authorization::Basic},
};
use chrono::{DateTime, Utc};
use clap::Parser;
use constant_time_eq::constant_time_eq;
use eyre::{OptionExt, Result, eyre};
use parking_lot::Mutex;
use positioned_io::RandomAccessFile;
use rand::distr::{Alphanumeric, SampleString};
use rc_zip_tokio::{ReadZip, rc_zip::parse::EntryKind};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    process::Command,
    sync::{RwLock, mpsc, watch},
    time::Instant,
};
use toml_edit::DocumentMut;
use tracing::{error, warn};

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
const PLATFORM: &str = "linux-x64";
#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
const PLATFORM: &str = "linux-arm64";
#[cfg(all(target_os = "macos", target_arch = "x86_64"))]
const PLATFORM: &str = "osx-x64";
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
const PLATFORM: &str = "osx-arm64";
#[cfg(all(target_os = "windows", target_arch = "x86_64"))]
const PLATFORM: &str = "windows-x64";
#[cfg(all(target_os = "windows", target_arch = "aarch64"))]
const PLATFORM: &str = "windows-arm64";
#[cfg(not(all(
    any(target_os = "windows", target_os = "macos", target_os = "linux"),
    any(target_arch = "x86_64", target_arch = "aarch64")
)))]
const PLATFORM: &str = compile_error!("Unsupported platform!");

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Config {
    base_url: String,
    bind_addr: String,
    storage_dir: PathBuf,
    notifications: NotificationConfig,
    forks: HashMap<String, ForkConfig>,
    instances: HashMap<String, InstanceConfig>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct NotificationConfig {
    discord_webhook: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct ForkConfig {
    token: String,
    manifest_url: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct InstanceConfig {
    name: String,
    token: String,
    port: u16,
    fork: String,
    timeout_seconds: f64,
}

#[derive(Parser)]
struct Args {
    config_file: PathBuf,
}

struct ForkManager {
    client: reqwest::Client,
    forks: HashMap<String, Fork>,
}

struct Fork {
    token: String,
    manifest_url: String,
    storage_path: RwLock<PathBuf>,
    current_version: String,
    update_subscriber: watch::Receiver<()>,
    update_notifier: watch::Sender<()>,
}

impl ForkManager {
    async fn new(config: &Config) -> Result<Self> {
        let client = reqwest::Client::new();
        let mut forks = HashMap::new();
        let forks_dir = config.storage_dir.join("forks");
        tokio::fs::create_dir_all(&forks_dir).await?;
        for (id, fork) in &config.forks {
            let file = forks_dir.join(format!("{id}.zip"));
            let manifest: Manifest = client
                .get(&fork.manifest_url)
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            let (latest_id, latest_build) = manifest
                .builds
                .iter()
                .max_by_key(|e| e.1.time)
                .ok_or(eyre!("Fork {id} has no versions?"))?;
            let artifact = latest_build
                .server
                .get(PLATFORM)
                .ok_or(eyre!("Fork {id} has no versions for our platform!"))?;
            let can_skip = if let Ok(mut file) = tokio::fs::File::open(&file).await {
                let mut hasher = Sha256::new();
                let mut buf = [0u8; 8192];
                loop {
                    let read = file.read(&mut buf).await?;
                    if read == 0 {
                        break;
                    }
                    hasher.update(&buf[0..read]);
                }
                let hash = hasher.finalize();
                artifact.sha256 == *hash
            } else {
                false
            };
            if !can_skip {
                let mut file = BufWriter::new(tokio::fs::File::create(&file).await?);
                let mut resp = client.get(&artifact.url).send().await?.error_for_status()?;
                while let Some(chunk) = resp.chunk().await? {
                    file.write_all(&chunk).await?;
                }
            }
            let (send, recv) = watch::channel(());
            forks.insert(
                id.clone(),
                Fork {
                    token: fork.token.clone(),
                    manifest_url: fork.manifest_url.clone(),
                    storage_path: RwLock::new(file),
                    current_version: latest_id.clone(),
                    update_subscriber: recv,
                    update_notifier: send,
                },
            );
        }
        Ok(Self { client, forks })
    }

    async fn check_update(&self, id: &str) -> Result<()> {
        let fork = self.forks.get(id).ok_or(eyre!("Invalid fork!"))?;
        let manifest: Manifest = self
            .client
            .get(&fork.manifest_url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let (latest_id, latest_build) = manifest
            .builds
            .iter()
            .max_by_key(|e| e.1.time)
            .ok_or(eyre!("Fork {id} has no versions?"))?;
        let artifact = latest_build
            .server
            .get(PLATFORM)
            .ok_or(eyre!("Fork {id} has no versions for our platform!"))?;
        if latest_id == &fork.current_version {
            return Ok(());
        }
        let path = fork.storage_path.write().await;
        let mut file = BufWriter::new(tokio::fs::File::create(path.as_path()).await?);
        let mut resp = self
            .client
            .get(&artifact.url)
            .send()
            .await?
            .error_for_status()?;
        while let Some(chunk) = resp.chunk().await? {
            file.write_all(&chunk).await?;
        }
        drop(path);
        _ = fork.update_notifier.send(());
        Ok(())
    }

    async fn extract_to(&self, id: &str, dest: &std::path::Path) -> Result<()> {
        if let Err(e) = tokio::fs::remove_dir_all(&dest).await {
            if e.kind() != ErrorKind::NotFound {
                return Err(e.into());
            }
        }
        tokio::fs::create_dir_all(&dest).await?;
        let fork = self.forks.get(id).ok_or(eyre!("Invalid fork!"))?;
        let path = fork.storage_path.read().await;
        let zip = Arc::new(RandomAccessFile::open(path.as_path())?);
        let zip = zip.read_zip().await?;
        for entry in zip.entries() {
            if entry.kind() != EntryKind::File {
                continue;
            }
            let filename = entry
                .sanitized_name()
                .ok_or_eyre("Invalid filename in server zip!")?;
            let path = dest.join(filename);
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            let mut reader = entry.reader();
            let mut file = tokio::fs::File::create(&path).await?;
            tokio::io::copy(&mut reader, &mut file).await?;
        }
        drop(path);
        Ok(())
    }
}

#[derive(Deserialize, Clone, Debug)]
struct Manifest {
    builds: HashMap<String, Build>,
}

#[derive(Deserialize, Clone, Debug)]
struct Build {
    time: DateTime<Utc>,
    client: Artifact,
    server: HashMap<String, Artifact>,
}

#[derive(Deserialize, Clone, Debug)]
struct Artifact {
    url: String,
    #[serde(with = "hex::serde")]
    sha256: [u8; 32],
}

struct InstanceManager {
    instances: HashMap<String, Arc<Instance>>,
}

struct Instance {
    name: String,
    token: String,
    port: u16,
    fork: parking_lot::Mutex<String>,
    internal_token: parking_lot::Mutex<String>,
    timeout_seconds: f64,
    notifier: mpsc::UnboundedSender<InstanceMessage>,
}

enum InstanceMessage {
    Restart,
    Stop,
    Ping,
    ForkChanged,
}

impl InstanceManager {
    async fn new(fork_manager: &'static ForkManager, config: &Config) -> Result<Self> {
        let client = reqwest::Client::new();
        let mut instances = HashMap::new();
        let instances_dir = config.storage_dir.join("instances");
        for (id, instance_conf) in &config.instances {
            let instance_dir = instances_dir.join(id);
            let (send, recv) = mpsc::unbounded_channel();
            let instance = Arc::new(Instance {
                name: instance_conf.name.clone(),
                token: instance_conf.token.clone(),
                port: instance_conf.port,
                fork: Mutex::new(instance_conf.fork.clone()),
                internal_token: Mutex::new(Alphanumeric.sample_string(&mut rand::rng(), 64)),
                timeout_seconds: instance_conf.timeout_seconds,
                notifier: send,
            });
            tokio::spawn(Self::run_instance(
                config.notifications.discord_webhook.clone(),
                fork_manager,
                config.base_url.clone(),
                id.clone(),
                instance_dir,
                instance.clone(),
                client.clone(),
                recv,
            ));
            instances.insert(id.clone(), instance);
        }
        Ok(Self { instances })
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_instance(
        discord_webhook: Option<String>,
        fork_manager: &'static ForkManager,
        base_url: String,
        instance_id: String,
        instance_dir: PathBuf,
        instance: Arc<Instance>,
        client: reqwest::Client,
        mut recv: mpsc::UnboundedReceiver<InstanceMessage>,
    ) {
        loop {
            match Self::run_instance_inner(
                fork_manager,
                &base_url,
                &instance_id,
                &instance_dir,
                &instance,
                &client,
                &mut recv,
            )
            .await
            {
                Ok(true) => loop {
                    match recv.recv().await {
                        Some(InstanceMessage::Restart) => break,
                        None => return,
                        _ => (),
                    }
                },
                Ok(false) => continue,
                Err(e) => {
                    error!("Restarting instance {instance_id}: {e:?}");
                    if let Some(webhook) = &discord_webhook {
                        if let Err(e) = client
                            .post(webhook)
                            .json(&serde_json::json!({
                                "content": format!("Instance {instance_id} (probably) crashed: {e:#}")
                            }))
                            .send()
                            .await
                            .and_then(|resp| resp.error_for_status()) {
                                warn!(
                                    "Failed to notify crash of instance {}: {e:?}",
                                    instance.name
                                );
                            }
                    }
                }
            }
        }
    }

    async fn run_instance_inner(
        fork_manager: &'static ForkManager,
        base_url: &str,
        instance_id: &str,
        instance_dir: &std::path::Path,
        instance: &Arc<Instance>,
        client: &reqwest::Client,
        recv: &mut mpsc::UnboundedReceiver<InstanceMessage>,
    ) -> Result<bool> {
        let fork_id = {
            let fork_id_lock = instance.fork.lock();
            let fork_id = fork_id_lock.clone();
            drop(fork_id_lock);
            fork_id
        };
        fork_manager
            .extract_to(&fork_id, &instance_dir.join("bin"))
            .await?;
        let internal_token = Alphanumeric.sample_string(&mut rand::rng(), 64);
        *instance.internal_token.lock() = internal_token.clone();
        let fork = fork_manager
            .forks
            .get(&fork_id)
            .ok_or_eyre("Invalid fork for instance!")?;
        let mut fork_subscriber = fork.update_subscriber.clone();
        #[cfg(target_os = "windows")]
        const COMMAND: &str = "Robust.Server.exe";
        #[cfg(not(target_os = "windows"))]
        const COMMAND: &str = "Robust.Server";
        let command_path = instance_dir.join("bin").join(COMMAND).canonicalize()?;
        #[cfg(not(target_os = "windows"))]
        {
            let mut perm = command_path.metadata()?.permissions();
            perm.set_mode(perm.mode() | 0b001001001);
            tokio::fs::set_permissions(&command_path, perm).await?;
        }
        let mut child = Command::new(command_path)
            .current_dir(instance_dir)
            .args([
                OsStr::new("--cvar"),
                OsStr::new(&format!("watchdog.key={instance_id}")),
                OsStr::new("--cvar"),
                OsStr::new(&format!("watchdog.baseUrl={base_url}")),
                OsStr::new("--config-file"),
                instance_dir.join("config.toml").as_os_str(),
                OsStr::new("--data-dir"),
                instance_dir.join("data").as_os_str(),
            ])
            .env("ROBUST_CVAR_watchdog__token", &internal_token)
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?;
        let mut timeout = Instant::now() + Duration::from_secs_f64(instance.timeout_seconds);
        #[derive(Clone, Copy, PartialEq, Eq)]
        enum WaitingForExitType {
            None,
            Restart,
            Kill,
        }
        let mut waiting_for_exit = WaitingForExitType::None;
        loop {
            enum CommandOrExitOrUpdateOrTimeout {
                Command(Option<InstanceMessage>),
                Exit(Result<ExitStatus, std::io::Error>),
                Update,
                Timeout,
            }
            let command_or_exit = tokio::select! {
                res = child.wait() => CommandOrExitOrUpdateOrTimeout::Exit(res),
                res = recv.recv() => CommandOrExitOrUpdateOrTimeout::Command(res),
                _ = fork_subscriber.changed() => CommandOrExitOrUpdateOrTimeout::Update,
                _ = tokio::time::sleep_until(timeout) => CommandOrExitOrUpdateOrTimeout::Timeout,
            };
            match command_or_exit {
                CommandOrExitOrUpdateOrTimeout::Command(None) => return Ok(true),
                CommandOrExitOrUpdateOrTimeout::Command(Some(InstanceMessage::Restart)) => {
                    let resp = client
                        .post(format!("http://localhost:{}/shutdown", instance.port))
                        .header("WatchdogToken", &internal_token)
                        .body(r#"{"Reason":"Watchdog shutting down."}"#)
                        .send()
                        .await
                        .and_then(|resp| resp.error_for_status());
                    if let Err(e) = resp {
                        warn!(
                            "Failed to notify shutdown on instance {}: {e:?}",
                            instance.name
                        );
                    }
                    timeout = Instant::now() + Duration::from_secs(5);
                    waiting_for_exit = WaitingForExitType::Kill;
                }
                CommandOrExitOrUpdateOrTimeout::Command(Some(InstanceMessage::Stop)) => {
                    child.kill().await?;
                    return Ok(true);
                }
                CommandOrExitOrUpdateOrTimeout::Command(Some(InstanceMessage::Ping)) => {
                    if waiting_for_exit != WaitingForExitType::Kill {
                        timeout =
                            Instant::now() + Duration::from_secs_f64(instance.timeout_seconds);
                    }
                }
                CommandOrExitOrUpdateOrTimeout::Command(Some(InstanceMessage::ForkChanged)) => {
                    let resp = client
                        .post(format!("http://localhost:{}/update", instance.port))
                        .header("WatchdogToken", &internal_token)
                        .send()
                        .await
                        .and_then(|resp| resp.error_for_status());
                    if let Err(e) = resp {
                        warn!(
                            "Failed to notify update on instance {}: {e:?}",
                            instance.name
                        );
                    }
                    let fork = fork_manager
                        .forks
                        .get(instance.fork.lock().as_str())
                        .ok_or_eyre("Invalid fork for instance!")?;
                    fork_subscriber = fork.update_subscriber.clone();
                    waiting_for_exit = WaitingForExitType::Restart;
                }
                CommandOrExitOrUpdateOrTimeout::Exit(exit_status) => {
                    let exit_status = exit_status?;
                    if waiting_for_exit != WaitingForExitType::None {
                        return Ok(false);
                    }
                    return Err(eyre!("Process exited with code {:?}", exit_status.code()));
                }
                CommandOrExitOrUpdateOrTimeout::Update => {
                    let resp = client
                        .post(format!("http://localhost:{}/update", instance.port))
                        .header("WatchdogToken", &internal_token)
                        .send()
                        .await
                        .and_then(|resp| resp.error_for_status());
                    if let Err(e) = resp {
                        warn!(
                            "Failed to notify update on instance {}: {e:?}",
                            instance.name
                        );
                    }
                    waiting_for_exit = WaitingForExitType::Restart;
                }
                CommandOrExitOrUpdateOrTimeout::Timeout => {
                    child.kill().await?;
                    return Err(eyre!("Timed out"));
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
struct AppState {
    fork_manager: &'static ForkManager,
    instance_manager: &'static InstanceManager,
    config_file: &'static std::path::Path,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let config_file = tokio::fs::read_to_string(&args.config_file).await?;
    let config: Config = toml_edit::de::from_str(&config_file)?;
    let fork_manager = Box::leak(Box::new(ForkManager::new(&config).await?));
    let app = Router::new()
        .route("/server_api/{key}/ping", post(server_ping))
        .route("/instances/{key}/restart", post(server_restart))
        .route("/instances/{key}/stop", post(server_stop))
        .route("/instances/{key}/setFork/{fork}", post(server_set_fork))
        .route("/instances/{key}/update", post(fork_update))
        .with_state(AppState {
            fork_manager,
            instance_manager: Box::leak(Box::new(
                InstanceManager::new(fork_manager, &config).await?,
            )),
            config_file: Box::leak(Box::new(args.config_file)),
        });

    let listener = tokio::net::TcpListener::bind(&config.bind_addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn server_ping(
    Path(key): Path<String>,
    State(state): State<AppState>,
    TypedHeader(Authorization(creds)): TypedHeader<Authorization<Basic>>,
) -> Result<(), ErrorResponse> {
    if creds.username() != key {
        return Err(StatusCode::FORBIDDEN.into());
    }
    let Some(instance) = state.instance_manager.instances.get(&key) else {
        return Err(StatusCode::NOT_FOUND.into());
    };
    if !constant_time_eq(
        creds.password().as_bytes(),
        instance.internal_token.lock().as_bytes(),
    ) {
        return Err(StatusCode::UNAUTHORIZED.into());
    }
    instance
        .notifier
        .send(InstanceMessage::Ping)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(())
}

async fn server_restart(
    Path(key): Path<String>,
    State(state): State<AppState>,
    TypedHeader(Authorization(creds)): TypedHeader<Authorization<Basic>>,
) -> Result<(), ErrorResponse> {
    if creds.username() != key {
        return Err(StatusCode::FORBIDDEN.into());
    }
    let Some(instance) = state.instance_manager.instances.get(&key) else {
        return Err(StatusCode::NOT_FOUND.into());
    };
    if !constant_time_eq(creds.password().as_bytes(), instance.token.as_bytes()) {
        return Err(StatusCode::UNAUTHORIZED.into());
    }
    instance
        .notifier
        .send(InstanceMessage::Restart)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(())
}

async fn server_stop(
    Path(key): Path<String>,
    State(state): State<AppState>,
    TypedHeader(Authorization(creds)): TypedHeader<Authorization<Basic>>,
) -> Result<(), ErrorResponse> {
    if creds.username() != key {
        return Err(StatusCode::FORBIDDEN.into());
    }
    let Some(instance) = state.instance_manager.instances.get(&key) else {
        return Err(StatusCode::NOT_FOUND.into());
    };
    if !constant_time_eq(creds.password().as_bytes(), instance.token.as_bytes()) {
        return Err(StatusCode::UNAUTHORIZED.into());
    }
    instance
        .notifier
        .send(InstanceMessage::Stop)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(())
}

async fn server_set_fork(
    Path((key, fork)): Path<(String, String)>,
    State(state): State<AppState>,
    TypedHeader(Authorization(creds)): TypedHeader<Authorization<Basic>>,
) -> Result<(), ErrorResponse> {
    if creds.username() != key {
        return Err(StatusCode::FORBIDDEN.into());
    }
    let Some(instance) = state.instance_manager.instances.get(&key) else {
        return Err(StatusCode::NOT_FOUND.into());
    };
    if !constant_time_eq(creds.password().as_bytes(), instance.token.as_bytes()) {
        return Err(StatusCode::UNAUTHORIZED.into());
    }
    let config_file = tokio::fs::read_to_string(&state.config_file)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut config: DocumentMut = config_file
        .parse()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    config["instances"][&key]["fork"] = fork.as_str().into();
    tokio::fs::write(&state.config_file, config.to_string())
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    *instance.fork.lock() = fork;
    instance
        .notifier
        .send(InstanceMessage::ForkChanged)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(())
}

async fn fork_update(
    Path(key): Path<String>,
    State(state): State<AppState>,
    TypedHeader(Authorization(creds)): TypedHeader<Authorization<Basic>>,
) -> Result<(), ErrorResponse> {
    if creds.username() != key {
        return Err(StatusCode::FORBIDDEN.into());
    }
    let Some(fork) = state.fork_manager.forks.get(&key) else {
        return Err(StatusCode::NOT_FOUND.into());
    };
    if !constant_time_eq(creds.password().as_bytes(), fork.token.as_bytes()) {
        return Err(StatusCode::UNAUTHORIZED.into());
    }
    state
        .fork_manager
        .check_update(&key)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(())
}
