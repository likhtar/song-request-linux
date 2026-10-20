use anyhow::{anyhow, bail, Context, Result};

use crate::{
    commands::ChatUserRole,
    config,
    song_requests::{
        MusicProvider, RequestSource, SongRequest, SongRequestInput, YoutubeRequestPlayback,
    },
    spotify,
    state::AppState,
    youtube,
};

pub async fn add_request(state: &AppState, input: SongRequestInput) -> Result<SongRequest> {
    add_request_for_role(state, input, ChatUserRole::Streamer).await
}

pub async fn add_request_for_role(
    state: &AppState,
    input: SongRequestInput,
    role: ChatUserRole,
) -> Result<SongRequest> {
    enforce_queue_limit(state, &input, role).await?;

    // A backup track never plays in front of viewer requests: it is moved out
    // of the current slot (playlist advances, next track is parked in the
    // queue) before the new request takes over.
    park_backup_for_request(state).await;

    let request = add_request_unchecked(state, input).await?;
    Ok(request)
}

async fn add_request_unchecked(state: &AppState, input: SongRequestInput) -> Result<SongRequest> {
    let default_provider = config::UiConfigView::load(&state.config.paths).default_provider;

    if let RequestSource::Youtube { video_id, .. } =
        RequestSource::from_query_public(&input.query, default_provider)
    {
        if matches!(default_provider, MusicProvider::Spotify) {
            bail!(
                "активний режим Spotify: посилання YouTube не потрапляє до черги. Перемкни provider на YouTube/Pear або YouTube/OBS, або напиши текст запиту для пошуку в Spotify."
            );
        }
        let request = add_youtube_url_request(state, input, video_id).await?;
        return Ok(request);
    }

    if should_use_spotify(default_provider, &input) {
        let mut token_guard = state.spotify_token.write().await;
        let token = token_guard
            .as_mut()
            .ok_or_else(|| anyhow!("Spotify is not connected"))?;
        let mut request = spotify::search_and_queue(&state.config, token, &input.query).await?;
        request.requester = input.requester.trim().to_string();
        request.query = input.query.trim().to_string();

        let request = add_resolved_and_persist(state, request).await?;
        return Ok(request);
    }

    if should_use_youtube(default_provider, &input) {
        let request = add_youtube_search_request(state, input).await?;
        return Ok(request);
    }

    let viewer_request = state.queue.write().await.add(input)?;
    persist_queue_if_enabled(state).await?;
    Ok(viewer_request)
}

/// A backup track never plays in front of viewer requests. When one is
/// currently playing and a request arrives, the playlist advances so the next
/// backup track is parked in the queue behind the request.
async fn park_backup_for_request(state: &AppState) {
    let playing_backup = {
        let queue = state.queue.read().await;
        queue.is_current_backup()
    };
    if !playing_backup {
        sync_backup_queue(state).await;
        return;
    }

    {
        let mut playlist = state.backup.write().await;
        playlist.advance();
        playlist.save(&state.config.paths);
    }
    state
        .record_event(
            "player",
            "Новий запит: бекап-трек пропущено, далі черга замовлень",
        )
        .await;

    // The played entry still carries the old cursor id, so the sync drops it
    // and appends the next backup track at the end of the queue.
    sync_backup_queue(state).await;
}

async fn add_youtube_url_request(
    state: &AppState,
    input: SongRequestInput,
    video_id: String,
) -> Result<SongRequest> {
    let metadata = youtube::video_metadata(
        &state.config,
        &youtube::YoutubeVideoRef {
            video_id: video_id.clone(),
        },
    )
    .await;

    let (title, artist) = match metadata {
        Ok(metadata) => {
            // Прямі посилання мають дотримуватися тих самих лімітів, що й
            // текстовий пошук: довге відео або не-музика відхиляється.
            youtube::enforce_video_policy(&state.config.youtube, &metadata)?;
            (metadata.title, metadata.channel_title)
        }
        Err(error) => {
            state
                .record_event(
                    "youtube",
                    format!("Nao consegui ler metadata do link YouTube {video_id}: {error}"),
                )
                .await;
            ("YouTube URL".to_string(), "YouTube".to_string())
        }
    };

    let request = SongRequest {
        id: 0,
        requester: input.requester.trim().to_string(),
        query: input.query.trim().to_string(),
        source: RequestSource::Youtube {
            video_id,
            playback: Some(current_youtube_request_playback(state)),
        },
        title,
        artist,
    };

    add_resolved_and_persist(state, request).await
}

fn current_youtube_request_playback(state: &AppState) -> YoutubeRequestPlayback {
    match config::UiConfigView::load(&state.config.paths).youtube_playback {
        config::YoutubePlayback::Pear => YoutubeRequestPlayback::Pear,
        config::YoutubePlayback::Browser => YoutubeRequestPlayback::Browser,
    }
}

async fn add_youtube_search_request(
    state: &AppState,
    input: SongRequestInput,
) -> Result<SongRequest> {
    let metadata = youtube::search_and_validate(&state.config, &input.query).await?;
    let request = SongRequest {
        id: 0,
        requester: input.requester.trim().to_string(),
        query: input.query.trim().to_string(),
        source: RequestSource::Youtube {
            video_id: metadata.video_id,
            playback: Some(current_youtube_request_playback(state)),
        },
        title: metadata.title,
        artist: metadata.channel_title,
    };

    add_resolved_and_persist(state, request).await
}

async fn add_resolved_and_persist(state: &AppState, request: SongRequest) -> Result<SongRequest> {
    let request = state.queue.write().await.add_resolved(request);
    persist_queue_if_enabled(state).await?;
    Ok(request)
}

/// Keep the backup track in sync with the playlist and make sure it sits at
/// the very end of the queue.
///
/// * backup disabled/empty -> stale backup entries are dropped
/// * track already queued     -> moved to the back, no duplicate is created
/// * track missing            -> appended at the back
pub async fn sync_backup_queue(state: &AppState) {
    let request = {
        let playlist = state.backup.read().await;
        if playlist.enabled {
            crate::song_requests::backup_song_request(&playlist)
        } else {
            None
        }
    };

    let persist = {
        let mut queue = state.queue.write().await;
        match request {
            None => {
                queue.clear_backup_entries();
                true
            }
            Some(request) => {
                // Drop the backup entry that just left the playlist: it is
                // either the current one or still waiting in the queue.
                for stale in queue.backup_ids_except(request.id) {
                    queue.remove_by_id(stale);
                }

                // `remove_by_id` above may have promoted the wanted entry
                // into the current slot; moving it again would push it back
                // to the queue and leave nothing playing.
                if queue.is_current_backup_with_id(request.id) {
                    // already the current song, nothing to do
                } else if queue.has_entry(request.id) {
                    queue.move_to_back(request.id);
                } else {
                    queue.append_raw(request);
                }
                true
            }
        }
    };

    if persist {
        persist_queue_if_enabled(state).await.ok();
    }
}

async fn persist_queue_if_enabled(state: &AppState) -> Result<()> {
    if !config::queue_persistence_enabled(&state.config.paths) {
        return Ok(());
    }

    state
        .queue
        .read()
        .await
        .save(&state.config.paths.queue_file)
        .with_context(|| {
            format!(
                "failed to persist queue at {}",
                state.config.paths.queue_file.display()
            )
        })
}

async fn enforce_queue_limit(
    state: &AppState,
    input: &SongRequestInput,
    role: ChatUserRole,
) -> Result<()> {
    let config = config::UiConfigView::load(&state.config.paths);
    let limit = config.queue_limits.limit_for(role);
    if limit == 0 {
        return Ok(());
    }

    let pending = state
        .queue
        .read()
        .await
        .pending_count_by_requester(&input.requester);
    if pending >= usize::from(limit) {
        bail!(
            "досягнуто ліміту {} пісень у черзі для твоєї ролі. Прибери запит командою !rm або зачекай, поки черга рухатиметься.",
            limit
        );
    }

    Ok(())
}

fn should_use_spotify(default_provider: MusicProvider, input: &SongRequestInput) -> bool {
    let source = RequestSource::from_query_public(&input.query, MusicProvider::Spotify);
    matches!(source, RequestSource::Spotify { .. })
        || (matches!(default_provider, MusicProvider::Spotify)
            && !matches!(source, RequestSource::Youtube { .. }))
}

fn should_use_youtube(default_provider: MusicProvider, input: &SongRequestInput) -> bool {
    let source = RequestSource::from_query_public(&input.query, MusicProvider::Youtube);
    matches!(default_provider, MusicProvider::Youtube)
        && matches!(source, RequestSource::Search { .. })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;

    fn isolated_config(name: &str) -> AppConfig {
        let mut config = AppConfig::from_env().expect("config");
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("song-request-linux-{name}-{unique}"));
        config.paths.config_dir = root.join("config");
        config.paths.state_dir = root.join("state");
        config.paths.queue_file = config.paths.state_dir.join("queue.json");
        config
    }

    #[tokio::test]
    async fn queue_limit_blocks_second_viewer_request() {
        let config = isolated_config("limit");
        crate::config::save_ui_config(
            &config.paths,
            crate::config::UiConfigInput {
                default_provider: Some(MusicProvider::Youtube),
                youtube_playback: Some(crate::config::YoutubePlayback::Browser),
                pear_base_url: None,
                spotify_client_id: None,
                spotify_fallback_enabled: Some(false),
                queue_persistence_enabled: Some(false),
                twitch_client_id: None,
                twitch_bot_username: None,
                twitch_channel: None,
                twitch_bot_oauth_token: None,
                youtube_api_key: None,
                youtube_max_duration_seconds: Some(360),
                youtube_allow_non_music: Some(false),
                command_settings: None,
                queue_limits: Some(crate::config::QueueLimitConfig {
                    follower: 1,
                    subscriber: 2,
                    vip: 3,
                    moderator: 10,
                    streamer: 0,
                }),
                overlay: None,
            },
        )
        .expect("save config");
        let state = AppState::new(config);

        add_request_for_role(
            &state,
            SongRequestInput {
                requester: "viewer".to_string(),
                query: "https://youtu.be/dQw4w9WgXcQ".to_string(),
            },
            ChatUserRole::Viewer,
        )
        .await
        .expect("first request");

        let error = add_request_for_role(
            &state,
            SongRequestInput {
                requester: "viewer".to_string(),
                query: "https://youtu.be/9bZkp7q19f0".to_string(),
            },
            ChatUserRole::Viewer,
        )
        .await
        .expect_err("second request should hit limit");

        assert!(error.to_string().contains("ліміту 1 пісень"));
    }

    #[tokio::test]
    async fn streamer_zero_limit_is_unlimited() {
        let config = isolated_config("streamer-unlimited");
        crate::config::save_ui_config(
            &config.paths,
            crate::config::UiConfigInput {
                default_provider: Some(MusicProvider::Youtube),
                youtube_playback: Some(crate::config::YoutubePlayback::Browser),
                pear_base_url: None,
                spotify_client_id: None,
                spotify_fallback_enabled: Some(false),
                queue_persistence_enabled: Some(false),
                twitch_client_id: None,
                twitch_bot_username: None,
                twitch_channel: None,
                twitch_bot_oauth_token: None,
                youtube_api_key: None,
                youtube_max_duration_seconds: Some(360),
                youtube_allow_non_music: Some(false),
                command_settings: None,
                queue_limits: Some(crate::config::QueueLimitConfig::default()),
                overlay: None,
            },
        )
        .expect("save config");
        let state = AppState::new(config);

        for query in [
            "https://youtu.be/dQw4w9WgXcQ",
            "https://youtu.be/9bZkp7q19f0",
        ] {
            add_request_for_role(
                &state,
                SongRequestInput {
                    requester: "heyleao".to_string(),
                    query: query.to_string(),
                },
                ChatUserRole::Streamer,
            )
            .await
            .expect("streamer request");
        }

        assert_eq!(
            state
                .queue
                .read()
                .await
                .pending_count_by_requester("heyleao"),
            2
        );
    }

    #[tokio::test]
    async fn youtube_link_is_rejected_when_spotify_is_default() {
        let config = isolated_config("youtube-link-spotify-mode");
        crate::config::save_ui_config(
            &config.paths,
            crate::config::UiConfigInput {
                default_provider: Some(MusicProvider::Spotify),
                youtube_playback: Some(crate::config::YoutubePlayback::Browser),
                pear_base_url: None,
                spotify_client_id: None,
                spotify_fallback_enabled: Some(false),
                queue_persistence_enabled: Some(false),
                twitch_client_id: None,
                twitch_bot_username: None,
                twitch_channel: None,
                twitch_bot_oauth_token: None,
                youtube_api_key: None,
                youtube_max_duration_seconds: Some(360),
                youtube_allow_non_music: Some(false),
                command_settings: None,
                queue_limits: Some(crate::config::QueueLimitConfig::default()),
                overlay: None,
            },
        )
        .expect("save config");
        let state = AppState::new(config);

        let error = add_request(
            &state,
            SongRequestInput {
                requester: "viewer".to_string(),
                query: "https://youtu.be/dQw4w9WgXcQ".to_string(),
            },
        )
        .await
        .expect_err("youtube urls should be blocked in spotify mode");

        assert!(error.to_string().contains("активний режим Spotify"));
    }

    #[tokio::test]
    async fn backup_track_is_queued_and_skipped_when_a_request_arrives() {
        let config = isolated_config("backup-sync");
        {
            // Two tracks so after the skip we can prove which one is parked.
            let playlist_file = &config.paths.backup_playlist_file;
            std::fs::create_dir_all(playlist_file.parent().expect("parent")).expect("dirs");
            std::fs::write(
                playlist_file,
                serde_json::json!({
                    "enabled": true,
                    "urls": [
                        "https://youtu.be/dQw4w9WgXcQ",
                        "https://youtu.be/9bZkp7q19f0"
                    ],
                    "order": [0, 1],
                    "cursor": 0
                })
                .to_string(),
            )
            .expect("write playlist");
        }
        let state = AppState::new(config);
        *state.backup.write().await = crate::backup::BackupPlaylist::load(&state.config.paths);
        state.queue.write().await.clear();
        sync_backup_queue(&state).await;

        {
            // A backup track in an empty queue is the current song.
            let queue = state.queue.read().await;
            let current = queue.view().current_song.expect("backup current");
            assert_eq!(
                queue.current_backup_entry_id(),
                Some(current.id),
                "the backup entry must own the current slot"
            );
            assert!(matches!(current.source, RequestSource::Backup));
            assert!(crate::backup::is_backup_sentinel(current.id));
        }

        let request = add_request_for_role(
            &state,
            SongRequestInput {
                requester: "viewer".to_string(),
                query: "https://youtu.be/fJ9rUzIMcZQ".to_string(),
            },
            ChatUserRole::Viewer,
        )
        .await
        .expect("request");

        let queue = state.queue.read().await;
        let view = queue.view();
        let waiting = view.queue;
        assert_eq!(
            waiting.len(),
            1,
            "only the next backup track may wait behind the request, got {waiting:?}"
        );
        let parked = waiting.first().expect("parked backup");
        assert!(matches!(parked.source, RequestSource::Backup));
        let current = view.current_song.clone().expect("current");
        assert!(matches!(current.source, RequestSource::Youtube { .. }));
        assert_eq!(
            current.id, request.id,
            "the requested song must take over from the backup track"
        );
        assert_ne!(
            parked.id, current.id,
            "the parked track must be the next backup, not the one that played"
        );
    }

    #[tokio::test]
    async fn disabling_the_backup_playlist_drops_stale_backup_entries() {
        let config = isolated_config("backup-disabled");
        let state = AppState::new(config.clone());
        let stale = SongRequest {
            id: crate::backup::sentinel_id(0),
            requester: "Backup".to_string(),
            query: "https://youtu.be/dQw4w9WgXcQ".to_string(),
            source: RequestSource::Backup,
            title: "Бекап 1".to_string(),
            artist: "Фонова музика".to_string(),
        };
        state.queue.write().await.append_raw(stale);

        // The playlist exists again but with `enabled: false`.
        std::fs::create_dir_all(config.paths.state_dir.clone()).expect("state dir");
        std::fs::write(
            &config.paths.backup_playlist_file,
            serde_json::json!({ "enabled": false, "urls": [], "order": [], "cursor": 0 })
                .to_string(),
        )
        .expect("write playlist");
        *state.backup.write().await = crate::backup::BackupPlaylist::load(&config.paths);

        sync_backup_queue(&state).await;

        let queue = state.queue.read().await;
        let view = queue.view();
        assert!(
            view.current_song.is_none(),
            "backup entries must be dropped"
        );
        assert!(view.queue.is_empty());
    }

    #[test]
    fn spotify_link_uses_spotify_even_when_youtube_is_default() {
        let input = SongRequestInput {
            requester: "viewer".to_string(),
            query: "https://open.spotify.com/track/3YxaaLqXvyWhQJwVFlvVVa?si=test".to_string(),
        };

        assert!(should_use_spotify(MusicProvider::Youtube, &input));
        assert!(!should_use_youtube(MusicProvider::Youtube, &input));
    }
}
