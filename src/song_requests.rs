use std::{collections::VecDeque, fs, path::Path};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::backup;
use crate::youtube::YoutubeVideoRef;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SongRequestInput {
    pub requester: String,
    pub query: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SongRequest {
    pub id: u64,
    pub requester: String,
    pub query: String,
    pub source: RequestSource,
    pub title: String,
    pub artist: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RequestSource {
    Search {
        provider: MusicProvider,
    },
    Spotify {
        uri: String,
    },
    Youtube {
        video_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        playback: Option<YoutubeRequestPlayback>,
    },
    /// Background track injected from the backup playlist. It is a regular
    /// queue entry with a sentinel id, so it behaves like any other request
    /// (skip, /api/skip, player finish) while still being recognizable.
    Backup,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum YoutubeRequestPlayback {
    Pear,
    Browser,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MusicProvider {
    Spotify,
    Youtube,
}

#[derive(Clone, Debug, Serialize)]
pub struct QueueView {
    pub current_song: Option<SongRequest>,
    pub queue: Vec<SongRequest>,
    pub queue_length: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persistence: Option<QueuePersistence>,
}

#[derive(Clone, Debug, Serialize)]
pub struct QueuePersistence {
    pub enabled: bool,
    pub exists: bool,
    pub saved_items: usize,
}

#[derive(Clone, Debug)]
pub struct SongQueue {
    next_id: u64,
    current_song: Option<SongRequest>,
    queue: VecDeque<SongRequest>,
    default_provider: MusicProvider,
}

impl Default for SongQueue {
    fn default() -> Self {
        Self {
            next_id: 1,
            current_song: None,
            queue: VecDeque::new(),
            default_provider: MusicProvider::Youtube,
        }
    }
}

impl SongQueue {
    pub fn new(default_provider: MusicProvider) -> Self {
        Self {
            default_provider,
            ..Self::default()
        }
    }

    pub fn load_or_new(default_provider: MusicProvider, path: &Path) -> Self {
        match Self::load(default_provider, path) {
            Ok(queue) => queue,
            Err(error) => {
                tracing::warn!(%error, path = %path.display(), "failed to load saved queue");
                Self::new(default_provider)
            }
        }
    }

    pub fn load(default_provider: MusicProvider, path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::new(default_provider));
        }

        let data = fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let saved = serde_json::from_str::<SavedQueue>(&data)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        let max_id = saved
            .current_song
            .iter()
            .chain(saved.queue.iter())
            .map(|song| song.id)
            .max()
            .unwrap_or(0);

        Ok(Self {
            next_id: max_id.saturating_add(1).max(1),
            current_song: saved.current_song,
            queue: saved.queue.into(),
            default_provider,
        })
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let saved = SavedQueue {
            current_song: self.current_song.clone(),
            queue: self.queue.iter().cloned().collect(),
        };
        fs::write(path, serde_json::to_vec_pretty(&saved)?)
            .with_context(|| format!("failed to write {}", path.display()))
    }

    pub fn add(&mut self, input: SongRequestInput) -> Result<SongRequest> {
        validate_input(&input)?;

        let source = RequestSource::from_query(&input.query, self.default_provider);
        let next_id = self.allocate_id();
        let request = SongRequest {
            id: next_id,
            requester: input.requester.trim().to_string(),
            title: title_from_source(&source, &input.query),
            artist: artist_from_source(&source),
            query: input.query.trim().to_string(),
            source,
        };

        Ok(self.enqueue(request))
    }

    pub fn add_resolved(&mut self, mut request: SongRequest) -> SongRequest {
        request.id = self.allocate_id();
        self.enqueue(request)
    }

    /// Insert a viewer request. A backup entry that happened to become the
    /// current song (it was alone in the queue) is never played in front of
    /// the request: it moves to the back of the queue instead.
    fn enqueue(&mut self, request: SongRequest) -> SongRequest {
        let current_is_backup = self
            .current_song
            .as_ref()
            .is_some_and(|song| matches!(song.source, RequestSource::Backup));

        if current_is_backup {
            let backup = self.current_song.take().expect("checked above");
            self.queue.push_back(backup);
        }

        if self.current_song.is_none() {
            self.current_song = Some(request.clone());
        } else {
            self.queue.push_back(request.clone());
        }

        request
    }

    /// Append a song without making it current and without touching
    /// `next_id`. Backups own their ids (sentinels), so they must never
    /// shift the counter real requests use.
    pub fn append_raw(&mut self, request: SongRequest) -> SongRequest {
        if self.current_song.is_none() {
            self.current_song = Some(request.clone());
        } else {
            self.queue.push_back(request.clone());
        }

        request
    }

    /// Queue used at startup: appends the backup track (if any) and moves
    /// `next_id` past every real id that is already present.
    pub fn with_backup_entry(mut self, request: SongRequest) -> Self {
        self.next_id = self
            .current_song
            .iter()
            .chain(self.queue.iter())
            .map(|song| song.id)
            .filter(|id| !crate::backup::is_backup_sentinel(*id))
            .max()
            .map(|max| max.saturating_add(1))
            .map_or(self.next_id, |next| next.max(self.next_id));
        self.append_raw(request);
        self
    }

    /// Drop every entry that came from the backup playlist. Used when the
    /// backup playlist is disabled so stale background tracks, whose ids are
    /// no longer meaningful, cannot keep playing.
    pub fn clear_backup_entries(&mut self) {
        self.queue
            .retain(|song| !matches!(song.source, RequestSource::Backup));

        if self
            .current_song
            .as_ref()
            .is_some_and(|song| matches!(song.source, RequestSource::Backup))
        {
            self.current_song = self.queue.pop_front();
        }
    }

    pub fn skip(&mut self) -> Option<SongRequest> {
        self.current_song = self.queue.pop_front();
        self.current_song.clone()
    }

    /// True when the currently playing song is the entry of the backup
    /// playlist that is current in the *player* (i.e. the request queue is
    /// exhausted and only background music is left).
    pub fn is_current_backup(&self) -> bool {
        self.current_song.is_some()
            && self.queue.is_empty()
            && self.current_backup_entry_id().is_some()
    }

    /// Ids of every backup entry except the given one.
    pub fn backup_ids_except(&self, keep: u64) -> Vec<u64> {
        self.current_song
            .iter()
            .chain(self.queue.iter())
            .filter(|song| matches!(song.source, RequestSource::Backup) && song.id != keep)
            .map(|song| song.id)
            .collect()
    }

    /// Sentinel id of the backup entry currently in the current slot, if any.
    pub fn current_backup_entry_id(&self) -> Option<u64> {
        self.current_song
            .as_ref()
            .filter(|song| matches!(song.source, RequestSource::Backup))
            .map(|song| song.id)
    }

    /// True when `id` is the backup entry occupying the current slot.
    pub fn is_current_backup_with_id(&self, id: u64) -> bool {
        self.current_backup_entry_id() == Some(id)
    }

    /// True when the entry with this sentinel id is somewhere in the queue.
    pub fn has_entry(&self, id: u64) -> bool {
        self.current_song
            .iter()
            .chain(self.queue.iter())
            .any(|song| song.id == id)
    }

    /// Move an existing entry to the very end of the waiting queue.
    pub fn move_to_back(&mut self, id: u64) {
        let entry = if self.current_song.as_ref().is_some_and(|song| song.id == id) {
            self.current_song.clone()
        } else {
            self.queue.iter().find(|song| song.id == id).cloned()
        };

        let Some(entry) = entry else {
            return;
        };

        if self.current_song.as_ref().is_some_and(|song| song.id == id) {
            self.current_song = self.queue.pop_front();
        } else if let Some(index) = self.queue.iter().position(|song| song.id == id) {
            self.queue.remove(index);
        }

        self.queue.push_back(entry);
    }

    #[cfg(test)]
    pub fn first_youtube(&self) -> Option<SongRequest> {
        self.current_song
            .iter()
            .chain(self.queue.iter())
            .find(|song| matches!(song.source, RequestSource::Youtube { .. }))
            .cloned()
    }

    pub fn remove_by_id(&mut self, id: u64) -> Option<SongRequest> {
        if self.current_song.as_ref().is_some_and(|song| song.id == id) {
            let removed = self.current_song.take();
            self.current_song = self.queue.pop_front();
            return removed;
        }

        let index = self.queue.iter().position(|song| song.id == id)?;
        self.queue.remove(index)
    }

    pub fn remove_last_by_requester(&mut self, requester: &str) -> Option<SongRequest> {
        let requester = requester.trim();
        let index = self
            .queue
            .iter()
            .rposition(|song| song.requester.eq_ignore_ascii_case(requester));
        if let Some(index) = index {
            return self.queue.remove(index);
        }

        if self
            .current_song
            .as_ref()
            .is_some_and(|song| song.requester.eq_ignore_ascii_case(requester))
        {
            let removed = self.current_song.take();
            self.current_song = self.queue.pop_front();
            return removed;
        }

        None
    }

    pub fn pending_count_by_requester(&self, requester: &str) -> usize {
        let requester = requester.trim();
        self.current_song
            .iter()
            .chain(self.queue.iter())
            .filter(|song| song.requester.eq_ignore_ascii_case(requester))
            .count()
    }

    #[cfg(test)]
    pub fn clear(&mut self) {
        self.current_song = None;
        self.queue.clear();
    }

    pub fn view(&self) -> QueueView {
        QueueView {
            current_song: self.current_song.clone(),
            queue: self.queue.iter().cloned().collect(),
            queue_length: self.queue.len(),
            persistence: None,
        }
    }

    pub fn position_of(&self, id: u64) -> Option<usize> {
        if self.current_song.as_ref().is_some_and(|s| s.id == id) {
            return Some(1);
        }
        self.queue.iter().position(|s| s.id == id).map(|i| i + 2)
    }

    fn allocate_id(&mut self) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        id
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct SavedQueue {
    current_song: Option<SongRequest>,
    queue: Vec<SongRequest>,
}

impl MusicProvider {
    pub fn from_env() -> Option<Self> {
        match std::env::var("SONG_REQUEST_PROVIDER") {
            Ok(value) if value.eq_ignore_ascii_case("spotify") => Some(Self::Spotify),
            Ok(value) if value.eq_ignore_ascii_case("youtube") => Some(Self::Youtube),
            _ => None,
        }
    }
}

impl RequestSource {
    pub fn from_query_public(query: &str, default_provider: MusicProvider) -> Self {
        Self::from_query(query, default_provider)
    }

    fn from_query(query: &str, default_provider: MusicProvider) -> Self {
        if let Some(video) = YoutubeVideoRef::parse(query) {
            return Self::Youtube {
                video_id: video.video_id,
                playback: None,
            };
        }

        if let Some(uri) = SpotifyTrackRef::parse(query).map(|track| track.uri) {
            return Self::Spotify { uri };
        }

        Self::Search {
            provider: default_provider,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpotifyTrackRef {
    pub id: String,
    pub uri: String,
}

impl SpotifyTrackRef {
    pub fn parse(value: &str) -> Option<Self> {
        let trimmed = value.trim();
        let id = parse_spotify_uri(trimmed).or_else(|| parse_spotify_url(trimmed))?;
        Some(Self {
            uri: format!("spotify:track:{id}"),
            id,
        })
    }
}

fn parse_spotify_uri(value: &str) -> Option<String> {
    let id = value.strip_prefix("spotify:track:")?;
    valid_spotify_track_id(id).then(|| id.to_string())
}

fn parse_spotify_url(value: &str) -> Option<String> {
    let without_fragment = value.split_once('#').map_or(value, |(left, _)| left);
    let without_query = without_fragment
        .split_once('?')
        .map_or(without_fragment, |(left, _)| left);
    let path = without_query
        .strip_prefix("https://open.spotify.com/")
        .or_else(|| without_query.strip_prefix("http://open.spotify.com/"))?;
    let parts = path
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    let track_index = parts.iter().position(|part| *part == "track")?;
    let id = parts.get(track_index + 1)?;
    valid_spotify_track_id(id).then(|| (*id).to_string())
}

fn valid_spotify_track_id(value: &str) -> bool {
    value.len() == 22 && value.chars().all(|char| char.is_ascii_alphanumeric())
}

fn validate_input(input: &SongRequestInput) -> Result<()> {
    if input.requester.trim().is_empty() {
        bail!("потрібен замовник");
    }

    let query = input.query.trim();
    if query.is_empty() {
        bail!("потрібен запит");
    }

    if query.chars().count() > 300 {
        bail!("запит занадто довгий");
    }

    Ok(())
}

fn title_from_source(source: &RequestSource, query: &str) -> String {
    match source {
        RequestSource::Youtube { video_id, .. } => format!("YouTube video {video_id}"),
        RequestSource::Spotify { .. } => query.trim().to_string(),
        RequestSource::Search { .. } => query.trim().to_string(),
        RequestSource::Backup => query.trim().to_string(),
    }
}

fn artist_from_source(source: &RequestSource) -> String {
    match source {
        RequestSource::Youtube { .. } => "YouTube".to_string(),
        RequestSource::Spotify { .. } => "Spotify".to_string(),
        RequestSource::Search { provider } => match provider {
            MusicProvider::Spotify => "Spotify search".to_string(),
            MusicProvider::Youtube => "YouTube search".to_string(),
        },
        RequestSource::Backup => "Фонова музика".to_string(),
    }
}

/// Build the queue entry that represents the backup track currently at the
/// playlist cursor. The id is the backup sentinel for that cursor position,
/// so the entry round-trips through the browser player unchanged.
pub fn backup_song_request(playlist: &backup::BackupPlaylist) -> Option<SongRequest> {
    let url = playlist.current_url()?.to_string();
    let video = YoutubeVideoRef::parse(&url)?;
    let position = playlist.current_position();

    Some(SongRequest {
        id: backup::sentinel_id(position),
        requester: "Backup".to_string(),
        query: format!("https://www.youtube.com/watch?v={}", video.video_id),
        source: RequestSource::Backup,
        title: format!("Бекап {}", position + 1),
        artist: "Фонова музика".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn parses_spotify_track_url() {
        let source = RequestSource::from_query_public(
            "https://open.spotify.com/track/3YxaaLqXvyWhQJwVFlvVVa?si=test",
            MusicProvider::Youtube,
        );

        assert_eq!(
            source,
            RequestSource::Spotify {
                uri: "spotify:track:3YxaaLqXvyWhQJwVFlvVVa".to_string()
            }
        );
    }

    #[test]
    fn parses_spotify_international_track_url() {
        let track =
            SpotifyTrackRef::parse("https://open.spotify.com/intl-pt/track/3YxaaLqXvyWhQJwVFlvVVa")
                .expect("spotify track");

        assert_eq!(track.id, "3YxaaLqXvyWhQJwVFlvVVa");
        assert_eq!(track.uri, "spotify:track:3YxaaLqXvyWhQJwVFlvVVa");
    }

    #[test]
    fn parses_spotify_track_uri() {
        let track =
            SpotifyTrackRef::parse("spotify:track:3YxaaLqXvyWhQJwVFlvVVa").expect("spotify track");

        assert_eq!(track.id, "3YxaaLqXvyWhQJwVFlvVVa");
        assert_eq!(track.uri, "spotify:track:3YxaaLqXvyWhQJwVFlvVVa");
    }

    #[test]
    fn first_request_becomes_current_song() {
        let mut queue = SongQueue::default();
        let request = queue
            .add(SongRequestInput {
                requester: "bruno".to_string(),
                query: "https://youtu.be/dQw4w9WgXcQ".to_string(),
            })
            .expect("request");

        let view = queue.view();

        assert_eq!(view.current_song, Some(request));
        assert_eq!(view.queue_length, 0);
    }

    #[test]
    fn second_request_waits_in_queue() {
        let mut queue = SongQueue::default();
        queue
            .add(SongRequestInput {
                requester: "one".to_string(),
                query: "first song".to_string(),
            })
            .expect("first");
        queue
            .add(SongRequestInput {
                requester: "two".to_string(),
                query: "second song".to_string(),
            })
            .expect("second");

        let view = queue.view();

        assert_eq!(view.current_song.expect("current").query, "first song");
        assert_eq!(view.queue_length, 1);
    }

    #[test]
    fn skip_advances_queue() {
        let mut queue = SongQueue::default();
        queue
            .add(SongRequestInput {
                requester: "one".to_string(),
                query: "first song".to_string(),
            })
            .expect("first");
        queue
            .add(SongRequestInput {
                requester: "two".to_string(),
                query: "second song".to_string(),
            })
            .expect("second");

        let current = queue.skip().expect("next");

        assert_eq!(current.query, "second song");
        assert_eq!(queue.view().queue_length, 0);
    }

    #[test]
    fn first_youtube_finds_request_behind_spotify() {
        let mut queue = SongQueue::new(MusicProvider::Spotify);
        queue.add_resolved(SongRequest {
            id: 0,
            requester: "viewer".to_string(),
            query: "daft punk one more time".to_string(),
            source: RequestSource::Spotify {
                uri: "spotify:track:test".to_string(),
            },
            title: "One More Time".to_string(),
            artist: "Daft Punk".to_string(),
        });
        let youtube = queue.add_resolved(SongRequest {
            id: 0,
            requester: "viewer".to_string(),
            query: "https://youtu.be/dQw4w9WgXcQ".to_string(),
            source: RequestSource::Youtube {
                video_id: "dQw4w9WgXcQ".to_string(),
                playback: None,
            },
            title: "Never Gonna Give You Up".to_string(),
            artist: "Rick Astley".to_string(),
        });

        assert_eq!(queue.first_youtube(), Some(youtube));
    }

    #[test]
    fn remove_by_id_removes_queued_youtube_without_advancing_spotify() {
        let mut queue = SongQueue::new(MusicProvider::Spotify);
        let spotify = queue.add_resolved(SongRequest {
            id: 0,
            requester: "viewer".to_string(),
            query: "daft punk one more time".to_string(),
            source: RequestSource::Spotify {
                uri: "spotify:track:test".to_string(),
            },
            title: "One More Time".to_string(),
            artist: "Daft Punk".to_string(),
        });
        let youtube = queue.add_resolved(SongRequest {
            id: 0,
            requester: "viewer".to_string(),
            query: "https://youtu.be/dQw4w9WgXcQ".to_string(),
            source: RequestSource::Youtube {
                video_id: "dQw4w9WgXcQ".to_string(),
                playback: None,
            },
            title: "Never Gonna Give You Up".to_string(),
            artist: "Rick Astley".to_string(),
        });

        assert_eq!(queue.remove_by_id(youtube.id), Some(youtube));
        assert_eq!(queue.view().current_song, Some(spotify));
        assert!(queue.first_youtube().is_none());
    }

    #[test]
    fn queue_persists_current_and_waiting_requests() {
        let path = temp_queue_path();
        let mut queue = SongQueue::new(MusicProvider::Youtube);
        let first = queue
            .add(SongRequestInput {
                requester: "viewer".to_string(),
                query: "first song".to_string(),
            })
            .expect("first");
        let second = queue
            .add(SongRequestInput {
                requester: "viewer".to_string(),
                query: "second song".to_string(),
            })
            .expect("second");

        queue.save(&path).expect("save queue");
        let mut loaded = SongQueue::load(MusicProvider::Spotify, &path).expect("load queue");
        let view = loaded.view();

        assert_eq!(view.current_song, Some(first));
        assert_eq!(view.queue, vec![second]);
        assert_eq!(view.queue_length, 1);
        let third = loaded
            .add(SongRequestInput {
                requester: "viewer".to_string(),
                query: "third song".to_string(),
            })
            .expect("third");
        assert_eq!(third.id, 3);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn append_raw_keeps_next_id_for_real_requests() {
        let mut queue = SongQueue::default();
        let mut backup = backup_song_request(&backup::BackupPlaylist {
            enabled: true,
            urls: vec!["https://youtu.be/dQw4w9WgXcQ".to_string()],
            order: vec![0],
            cursor: 0,
        })
        .expect("backup request");

        backup.id = backup::sentinel_id(0);
        queue.append_raw(backup);

        let request = queue
            .add(SongRequestInput {
                requester: "viewer".to_string(),
                query: "first song".to_string(),
            })
            .expect("request");

        assert_eq!(request.id, 1, "backup entries must not consume request ids");

        // The request takes over and the backup entry is parked behind it.
        let view = queue.view();
        assert_eq!(view.current_song.map(|song| song.id), Some(request.id));
        assert_eq!(view.queue.len(), 1);
        assert!(matches!(
            view.queue.first().map(|song| &song.source),
            Some(RequestSource::Backup)
        ));
    }

    #[test]
    fn clear_backup_entries_promotes_the_next_song() {
        let mut queue = SongQueue::default();
        let backup_request = SongRequest {
            id: backup::sentinel_id(0),
            requester: "Backup".to_string(),
            query: "https://youtu.be/dQw4w9WgXcQ".to_string(),
            source: RequestSource::Backup,
            title: "Бекап 1".to_string(),
            artist: "Фонова музика".to_string(),
        };
        queue.append_raw(backup_request);
        let request = queue
            .add(SongRequestInput {
                requester: "viewer".to_string(),
                query: "first song".to_string(),
            })
            .expect("request");

        queue.clear_backup_entries();

        let view = queue.view();
        assert!(
            matches!(
                view.current_song.as_ref().expect("current").source,
                RequestSource::Search { .. }
            ),
            "the promoted song must be the real request, not another backup"
        );
        assert_eq!(view.current_song.expect("current").id, request.id);
        assert!(view.queue.is_empty());
    }

    #[test]
    fn backup_song_request_carries_sentinel_id_and_video_url() {
        let playlist = backup::BackupPlaylist {
            enabled: true,
            urls: vec!["https://youtu.be/dQw4w9WgXcQ".to_string()],
            order: vec![0],
            cursor: 0,
        };

        let request = backup_song_request(&playlist).expect("backup request");

        assert!(backup::is_backup_sentinel(request.id));
        assert_eq!(request.id, backup::sentinel_id(0));
        assert!(matches!(request.source, RequestSource::Backup));
        assert!(request.query.contains("dQw4w9WgXcQ"));
    }

    #[test]
    fn move_to_back_keeps_the_backup_entry_at_the_end() {
        let mut queue = SongQueue::default();
        let backup_request = SongRequest {
            id: backup::sentinel_id(3),
            requester: "Backup".to_string(),
            query: "https://youtu.be/dQw4w9WgXcQ".to_string(),
            source: RequestSource::Backup,
            title: "Бекап 4".to_string(),
            artist: "Фонова музика".to_string(),
        };
        queue
            .add(SongRequestInput {
                requester: "viewer".to_string(),
                query: "first song".to_string(),
            })
            .expect("request");
        queue.append_raw(backup_request);
        assert!(queue.has_entry(backup::sentinel_id(3)));

        queue.move_to_back(backup::sentinel_id(3));
        assert_eq!(
            queue.view().queue.last().map(|song| song.id),
            Some(backup::sentinel_id(3))
        );
    }

    fn temp_queue_path() -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "song-request-linux-queue-test-{}-{nanos}.json",
            std::process::id()
        ))
    }
}
