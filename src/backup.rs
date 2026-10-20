//! Backup playlist: plays curated YouTube tracks when the local request
//! queue is empty so the stream never goes silent. A real request always
//! takes priority and the backup resumes where it left off.

use std::collections::HashSet;
use std::fs;

use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::config::AppPaths;
use crate::youtube::YoutubeVideoRef;

/// Real queue ids start at 1 and grow by one, so ids far above that range
/// can never collide with them. Backup tracks use the top of the JS-safe
/// integer range: 2^53 - 1 = Number.MAX_SAFE_INTEGER.
///
/// The browser player represents ids as JS `Number` (IEEE-754 double). Any
/// id above 2^53 silently rounds, so a sentinel like u64::MAX - N round-trips
/// as 2^64 = 18446744073709552000, which fails to deserialize back into u64
/// on POST /audio and makes the server answer 422. Keeping sentinels below
/// 2^53 keeps ids exact through the whole player round-trip.
const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991; // 2^53 - 1

/// ids above this threshold belong to the backup namespace. Chosen far above
/// any possible queue id yet safely below MAX_SAFE_INTEGER.
const SENTINEL_THRESHOLD: u64 = 9_000_000_000_000_000;

pub fn is_backup_sentinel(id: u64) -> bool {
    id > SENTINEL_THRESHOLD
}

/// Track id for the backup song at `position` (index inside `order`).
pub fn sentinel_id(position: usize) -> u64 {
    MAX_SAFE_INTEGER - position as u64
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct BackupPlaylist {
    pub enabled: bool,
    pub urls: Vec<String>,
    /// Shuffled permutation of `urls` indices that defines the play order.
    pub order: Vec<usize>,
    /// Index inside `order` of the next track to play.
    pub cursor: usize,
}

impl Default for BackupPlaylist {
    fn default() -> Self {
        Self {
            enabled: false,
            urls: Vec::new(),
            order: Vec::new(),
            cursor: 0,
        }
    }
}

impl BackupPlaylist {
    pub fn load(paths: &AppPaths) -> Self {
        let path = &paths.backup_playlist_file;
        let mut playlist: Self = match fs::read_to_string(path) {
            Ok(raw) => match serde_json::from_str(&raw) {
                Ok(playlist) => playlist,
                Err(error) => {
                    warn!(%error, path = %path.display(), "corrupt backup playlist file; using defaults");
                    Self::default()
                }
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Self::default(),
            Err(error) => {
                warn!(%error, path = %path.display(), "cannot read backup playlist file; using defaults");
                Self::default()
            }
        };

        playlist.clean();
        playlist
    }

    /// Drop invalid or duplicate URLs and rebuild the play order when it no
    /// longer matches the URL list.
    fn clean(&mut self) {
        if !self.enabled {
            return;
        }

        let mut seen = HashSet::new();
        self.urls.retain(|url| {
            // Dedup by video id so the same video never plays twice in a cycle.
            YoutubeVideoRef::parse(url)
                .and_then(|video| seen.insert(video.video_id).then_some(true))
                .unwrap_or(false)
        });

        if self.order.len() != self.urls.len() {
            self.reshuffle();
        } else {
            self.cursor = self.cursor.min(self.urls.len().saturating_sub(1));
        }

        if self.urls.is_empty() {
            self.enabled = false;
        }
    }

    pub fn current_url(&self) -> Option<&str> {
        if !self.enabled {
            return None;
        }
        let index = *self.order.get(self.cursor)?;
        self.urls.get(index).map(String::as_str)
    }

    pub fn current_position(&self) -> usize {
        self.cursor
    }

    /// Move to the next track; reshuffle and restart the cycle when done.
    pub fn advance(&mut self) {
        if !self.enabled || self.urls.is_empty() {
            return;
        }
        self.cursor += 1;
        if self.cursor >= self.order.len() {
            self.reshuffle();
        }
    }

    fn reshuffle(&mut self) {
        let mut order: Vec<usize> = (0..self.urls.len()).collect();
        order.shuffle(&mut rand::rng());
        self.order = order;
        self.cursor = 0;
    }

    pub fn save(&self, paths: &AppPaths) {
        let path = &paths.backup_playlist_file;
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        match serde_json::to_string_pretty(self) {
            Ok(raw) => {
                if let Err(error) = fs::write(path, raw) {
                    warn!(%error, path = %path.display(), "cannot save backup playlist");
                }
            }
            Err(error) => warn!(%error, "cannot serialize backup playlist"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::path::Path;

    use crate::config::AppPaths;

    fn test_paths(root: &Path) -> AppPaths {
        AppPaths {
            config_dir: root.join("config"),
            cache_dir: root.join("cache"),
            state_dir: root.join("state"),
            log_dir: root.join("state/logs"),
            tls_dir: root.join("state/tls"),
            queue_file: root.join("state/queue.json"),
            backup_playlist_file: root.join("state/backup-playlist.json"),
        }
    }

    fn urls() -> Vec<String> {
        vec![
            "https://youtu.be/dQw4w9WgXcQ".to_string(),
            "https://music.youtube.com/watch?v=9bZkp7q19f0".to_string(),
            "https://www.youtube.com/watch?v=fJ9rUzIMcZQ".to_string(),
        ]
    }

    #[test]
    fn missing_file_loads_disabled() {
        let root = env::temp_dir().join(format!("srl-backup-missing-{}", std::process::id()));
        let paths = test_paths(&root);
        let playlist = BackupPlaylist::load(&paths);

        assert!(!playlist.enabled);
        assert!(playlist.urls.is_empty());

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn corrupt_file_loads_defaults() {
        let root = env::temp_dir().join(format!("srl-backup-corrupt-{}", std::process::id()));
        let paths = test_paths(&root);
        fs::create_dir_all(paths.state_dir.clone()).unwrap();
        fs::write(&paths.backup_playlist_file, "{{{not json").unwrap();

        let playlist = BackupPlaylist::load(&paths);
        assert!(!playlist.enabled);
        assert!(playlist.urls.is_empty());

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn invalid_and_duplicate_urls_are_filtered() {
        let root = env::temp_dir().join(format!("srl-backup-filter-{}", std::process::id()));
        let paths = test_paths(&root);
        fs::create_dir_all(paths.state_dir.clone()).unwrap();
        fs::write(
            &paths.backup_playlist_file,
            serde_json::to_string(&BackupPlaylist {
                enabled: true,
                urls: vec![
                    "https://youtu.be/dQw4w9WgXcQ".to_string(),
                    "not a url".to_string(),
                    "https://youtu.be/dQw4w9WgXcQ".to_string(),
                    "https://www.youtube.com/watch?v=9bZkp7q19f0".to_string(),
                ],
                order: vec![],
                cursor: 0,
            })
            .unwrap(),
        )
        .unwrap();

        let playlist = BackupPlaylist::load(&paths);
        assert_eq!(playlist.urls.len(), 2);
        assert!(playlist.urls.iter().any(|url| url.contains("dQw4w9WgXcQ")));
        assert!(playlist.urls.iter().any(|url| url.contains("9bZkp7q19f0")));

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn order_is_rebuilt_when_urls_change() {
        let root = env::temp_dir().join(format!("srl-backup-reorder-{}", std::process::id()));
        let paths = test_paths(&root);
        fs::create_dir_all(paths.state_dir.clone()).unwrap();
        fs::write(
            &paths.backup_playlist_file,
            serde_json::to_string(&BackupPlaylist {
                enabled: true,
                urls: urls(),
                order: vec![0, 1, 2], // stale size matches, no rebuild needed
                cursor: 1,
            })
            .unwrap(),
        )
        .unwrap();
        let playlist = BackupPlaylist::load(&paths);
        assert_eq!(playlist.order.len(), 3);

        // Change the url count -> order has to be rebuilt from scratch.
        fs::write(
            &paths.backup_playlist_file,
            serde_json::to_string(&BackupPlaylist {
                enabled: true,
                urls: urls(),
                order: vec![0, 1], // too short
                cursor: 0,
            })
            .unwrap(),
        )
        .unwrap();
        let playlist = BackupPlaylist::load(&paths);
        assert_eq!(playlist.order.len(), 3);
        let mut sorted = playlist.order.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, vec![0, 1, 2]);

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn advance_wraps_and_reshuffles() {
        let mut playlist = BackupPlaylist {
            enabled: true,
            urls: urls(),
            order: vec![2, 0, 1],
            cursor: 0,
        };

        assert_eq!(playlist.current_url(), Some(urls()[2].as_str()));
        playlist.advance();
        assert_eq!(playlist.current_url(), Some(urls()[0].as_str()));
        playlist.advance();
        assert_eq!(playlist.current_url(), Some(urls()[1].as_str()));

        // Reaching the end triggers a reshuffle and restarts the cycle.
        playlist.advance();
        assert_eq!(playlist.cursor, 0);
        assert_eq!(playlist.order.len(), 3);
        let mut sorted = playlist.order.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, vec![0, 1, 2]);
    }

    #[test]
    fn save_and_load_roundtrip() {
        let root = env::temp_dir().join(format!("srl-backup-roundtrip-{}", std::process::id()));
        let paths = test_paths(&root);
        let playlist = BackupPlaylist {
            enabled: true,
            urls: urls(),
            order: vec![1, 0, 2],
            cursor: 2,
        };
        playlist.save(&paths);

        let loaded = BackupPlaylist::load(&paths);
        assert!(loaded.enabled);
        assert_eq!(loaded.urls, urls());
        assert_eq!(loaded.order, vec![1, 0, 2]);
        assert_eq!(loaded.cursor, 2);

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn disabled_playlist_has_no_current_track() {
        let mut playlist = BackupPlaylist {
            enabled: false,
            urls: urls(),
            order: vec![0, 1, 2],
            cursor: 0,
        };
        assert!(playlist.current_url().is_none());

        playlist.advance();
        assert_eq!(playlist.cursor, 0, "disabled playlist must not advance");
    }

    #[test]
    fn sentinel_ids_do_not_collide_with_queue_ids() {
        assert!(is_backup_sentinel(sentinel_id(0)));
        assert!(!is_backup_sentinel(1_000_000));

        let mut ids: std::collections::HashSet<u64> = HashSet::new();
        for position in 0..1000 {
            assert!(
                ids.insert(sentinel_id(position)),
                "sentinel ids must be unique"
            );
        }
    }

    #[test]
    fn sentinel_ids_fit_in_js_safe_integer_range() {
        // JS Number (double) rounds any integer above 2^53, which breaks the
        // id round-trip through the browser player (POST /audio -> 422).
        assert_eq!(
            sentinel_id(0),
            9_007_199_254_740_991,
            "must start at 2^53 - 1"
        );
        for position in 0..1000 {
            assert!(
                sentinel_id(position) <= 9_007_199_254_740_991,
                "sentinel {position} exceeds Number.MAX_SAFE_INTEGER"
            );
        }
        assert!(
            !is_backup_sentinel(sentinel_id(0) / 2),
            "threshold must sit below the sentinel range"
        );
    }
}
