use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use serde::Serialize;
use tokio::sync::{broadcast, Mutex, RwLock};

use crate::backup::BackupPlaylist;
use crate::config::{self, AppConfig, APP_NAME};
use crate::song_requests::{QueueView, SongQueue};
use crate::spotify::{load_token, SpotifyAuthSession, SpotifyToken};

#[derive(Clone)]
pub struct AppState {
    pub config: AppConfig,
    pub app_name: &'static str,
    pub version: &'static str,
    pub queue: Arc<RwLock<SongQueue>>,
    pub spotify_auth: Arc<RwLock<Option<SpotifyAuthSession>>>,
    pub spotify_token: Arc<RwLock<Option<SpotifyToken>>>,
    pub twitch_bot_running: Arc<AtomicBool>,
    pub youtube_player_paused_spotify: Arc<AtomicBool>,
    pub youtube_browser_paused: Arc<AtomicBool>,
    pub youtube_browser_volume: Arc<AtomicU8>,
    pub spotify_fallback_started: Arc<AtomicBool>,
    pub spotify_manual_pause_active: Arc<AtomicBool>,
    pub pear_idle_stopped: Arc<AtomicBool>,
    pub youtube_active_pear_video_id: Arc<Mutex<Option<String>>>,
    pub youtube_failed_pear_video_id: Arc<Mutex<Option<String>>>,
    pub pear_waiting_video_id: Arc<Mutex<Option<String>>>,
    pub youtube_waiting_spotify_title: Arc<Mutex<Option<String>>>,
    pub spotify_manual_playback_title: Arc<Mutex<Option<String>>>,
    pub backup: Arc<RwLock<BackupPlaylist>>,
    /// Id reported by the last successful `/api/player/youtube/finish`.
    /// Backup ids are cursor positions, so without this guard a repeated
    /// finish call would skip an extra track.
    pub last_player_finish: Arc<AtomicU64>,
    pub video_meta_cache: Arc<Mutex<HashMap<String, (String, String)>>>,
    pub events: Arc<RwLock<EventLog>>,
    pub shutdown: broadcast::Sender<()>,
}

#[derive(Clone, Debug, Serialize)]
pub struct AppEvent {
    pub id: u64,
    pub timestamp: u64,
    pub kind: &'static str,
    pub message: String,
}

#[derive(Debug)]
pub struct EventLog {
    next_id: u64,
    events: VecDeque<AppEvent>,
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
}

#[derive(Serialize)]
pub struct StatusResponse {
    pub app: &'static str,
    pub version: &'static str,
    pub provider: &'static str,
    pub current_song: Option<SongView>,
    pub queue_length: usize,
}

#[derive(Serialize)]
pub struct SongView {
    pub title: String,
    pub artist: String,
    pub requester: String,
}

impl AppState {
    pub fn new(config: AppConfig) -> Self {
        let queue = if config::queue_persistence_enabled(&config.paths) {
            SongQueue::load_or_new(config.default_provider, &config.paths.queue_file)
        } else {
            SongQueue::new(config.default_provider)
        };
        let (shutdown, _) = broadcast::channel(1);
        let browser_volume = config::configured_volume(
            &config.paths,
            crate::song_requests::MusicProvider::Youtube,
            crate::config::YoutubePlayback::Browser,
        );
        let browser_paused = config::load_player_state(&config.paths).youtube_browser_paused;
        let backup = BackupPlaylist::load(&config.paths);
        let state_queue = if backup.enabled {
            match crate::song_requests::backup_song_request(&backup) {
                Some(request) => queue.with_backup_entry(request),
                None => queue,
            }
        } else {
            queue
        };
        let state = Self {
            spotify_token: Arc::new(RwLock::new(load_token(&config).ok().flatten())),
            config,
            app_name: APP_NAME,
            version: env!("CARGO_PKG_VERSION"),
            queue: Arc::new(RwLock::new(state_queue)),
            spotify_auth: Arc::new(RwLock::new(None)),
            twitch_bot_running: Arc::new(AtomicBool::new(false)),
            youtube_player_paused_spotify: Arc::new(AtomicBool::new(false)),
            youtube_browser_paused: Arc::new(AtomicBool::new(browser_paused)),
            youtube_browser_volume: Arc::new(AtomicU8::new(browser_volume)),
            spotify_fallback_started: Arc::new(AtomicBool::new(false)),
            spotify_manual_pause_active: Arc::new(AtomicBool::new(false)),
            pear_idle_stopped: Arc::new(AtomicBool::new(false)),
            youtube_active_pear_video_id: Arc::new(Mutex::new(None)),
            youtube_failed_pear_video_id: Arc::new(Mutex::new(None)),
            pear_waiting_video_id: Arc::new(Mutex::new(None)),
            youtube_waiting_spotify_title: Arc::new(Mutex::new(None)),
            spotify_manual_playback_title: Arc::new(Mutex::new(None)),
            backup: Arc::new(RwLock::new(backup)),
            last_player_finish: Arc::new(AtomicU64::new(0)),
            video_meta_cache: Arc::new(Mutex::new(HashMap::new())),
            events: Arc::new(RwLock::new(EventLog::new())),
            shutdown,
        };
        state
    }

    /// Move to the next backup track and keep the queue in sync: the backup
    /// entry that just played is dropped and the new one is appended.
    pub async fn advance_and_sync(&self, paths: &crate::config::AppPaths) {
        {
            let mut playlist = self.backup.write().await;
            playlist.advance();
            playlist.save(paths);
        }
        crate::request_flow::sync_backup_queue(self).await;
    }

    pub async fn record_event(&self, kind: &'static str, message: impl Into<String>) {
        self.events.write().await.push(kind, message.into());
    }

    /// Store the OBS Browser player paused state and persist it across restarts.
    pub fn set_browser_paused(&self, paused: bool) {
        self.youtube_browser_paused.store(paused, Ordering::SeqCst);
        if let Err(error) = config::save_player_state(
            &self.config.paths,
            &config::PlayerState {
                youtube_browser_paused: paused,
            },
        ) {
            tracing::warn!(%error, "failed to persist browser paused state");
        }
    }
}

impl EventLog {
    fn new() -> Self {
        Self {
            next_id: 1,
            events: VecDeque::new(),
        }
    }

    fn push(&mut self, kind: &'static str, message: String) {
        let event = AppEvent {
            id: self.next_id,
            timestamp: unix_now(),
            kind,
            message,
        };
        self.next_id += 1;
        self.events.push_front(event);

        while self.events.len() > 120 {
            self.events.pop_back();
        }
    }

    pub fn recent(&self, limit: usize) -> Vec<AppEvent> {
        self.events.iter().take(limit).cloned().collect()
    }

    pub fn clear(&mut self) {
        self.events.clear();
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

impl StatusResponse {
    pub fn from_queue(
        state: &AppState,
        queue: QueueView,
        provider: crate::song_requests::MusicProvider,
    ) -> Self {
        Self {
            app: state.app_name,
            version: state.version,
            provider: match provider {
                crate::song_requests::MusicProvider::Spotify => "spotify",
                crate::song_requests::MusicProvider::Youtube => "youtube",
            },
            current_song: queue.current_song.map(SongView::from),
            queue_length: queue.queue_length,
        }
    }
}

impl From<crate::song_requests::SongRequest> for SongView {
    fn from(song: crate::song_requests::SongRequest) -> Self {
        Self {
            title: song.title,
            artist: song.artist,
            requester: song.requester,
        }
    }
}
