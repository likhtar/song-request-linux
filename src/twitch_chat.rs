use anyhow::{anyhow, Context, Result};
use futures_util::{Sink, SinkExt, StreamExt};
use std::{
    collections::HashMap,
    sync::atomic::Ordering,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, Semaphore};
use tokio::time::{interval, timeout};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

use crate::{
    commands::{parse_chat_command, ChatCommand, ChatCommandInput, ChatUserRole, PlaybackAction},
    config::{self, TwitchBotSecrets, YoutubePlayback},
    display, request_flow,
    song_requests::{MusicProvider, QueueView, RequestSource, SongRequest, YoutubeRequestPlayback},
    state::AppState,
    youtube,
};

const TWITCH_IRC_WS: &str = "wss://irc-ws.chat.twitch.tv:443";

/// Any socket write that takes longer than this is treated as a dead
/// connection so a stuck send can never pin the shared writer lock.
const SEND_TIMEOUT: Duration = Duration::from_secs(10);
/// A chat command that runs longer than this is cancelled, so a hung
/// external API call cannot wedge the shared rate limiter lock forever.
const HANDLER_TIMEOUT: Duration = Duration::from_secs(60);
/// If nothing at all arrives from Twitch for this long, the socket is
/// considered dead (open but silent) and we force a reconnect. Must be
/// comfortably longer than Twitch's own PING cadence (~5 min) so a quiet
/// channel alone never triggers a needless reconnect.
const READ_IDLE_TIMEOUT: Duration = Duration::from_secs(540);
/// Client-side keepalive PING. Twitch answers PONG, so this keeps traffic
/// flowing both ways and lets the read-idle watchdog detect dead sockets.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(45);
const BASE_RECONNECT_DELAY: Duration = Duration::from_secs(5);
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(30);

pub fn spawn_bot(state: AppState, secrets: TwitchBotSecrets) {
    if state
        .twitch_bot_running
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        info!("twitch bot already running");
        return;
    }

    tokio::spawn(async move {
        if let Err(error) = run_bot(state, secrets).await {
            error!(error = %error, "twitch bot stopped");
        }
    });
}

async fn run_bot(state: AppState, secrets: TwitchBotSecrets) -> Result<()> {
    let mut backoff = BASE_RECONNECT_DELAY;
    loop {
        let started = Instant::now();
        match connect_and_serve(&state, &secrets).await {
            Ok(()) => warn!("twitch websocket closed; reconnecting in {:?}", backoff),
            Err(error) => warn!(
                error = %error,
                "twitch bot connection failed; retrying in {:?}",
                backoff
            ),
        }
        // A session that stayed healthy for a while resets the backoff;
        // only rapid consecutive failures keep ramping up the delay.
        if started.elapsed() > Duration::from_secs(60) {
            backoff = BASE_RECONNECT_DELAY;
        }
        tokio::time::sleep(backoff).await;
        backoff = backoff.saturating_mul(2).min(MAX_RECONNECT_BACKOFF);
    }
}

async fn connect_and_serve(state: &AppState, secrets: &TwitchBotSecrets) -> Result<()> {
    info!(
        channel = %secrets.channel,
        username = %secrets.username,
        "connecting twitch bot"
    );

    let (stream, _) = connect_async(TWITCH_IRC_WS)
        .await
        .context("failed to connect to Twitch IRC websocket")?;
    let (mut writer, mut reader) = stream.split();

    writer
        .send(Message::Text(
            "CAP REQ :twitch.tv/tags twitch.tv/commands".into(),
        ))
        .await?;
    writer
        .send(Message::Text(
            format!("PASS oauth:{}", secrets.oauth_token).into(),
        ))
        .await?;
    writer
        .send(Message::Text(format!("NICK {}", secrets.username).into()))
        .await?;
    writer
        .send(Message::Text(format!("JOIN #{}", secrets.channel).into()))
        .await?;

    info!(channel = %secrets.channel, "twitch bot connected");

    let writer = Arc::new(Mutex::new(writer));
    let rate_limiter = Arc::new(Mutex::new(ChatRateLimiter::default()));
    let handlers = Arc::new(Semaphore::new(8));

    // Watchdog: if no traffic arrives for a while (socket open but dead),
    // force a reconnect. Keepalive PING keeps both directions active and
    // surfaces a broken send path (error -> reconnect).
    let mut idle_watchdog = Box::pin(tokio::time::sleep(READ_IDLE_TIMEOUT));
    let mut keepalive = interval(KEEPALIVE_INTERVAL);

    loop {
        tokio::select! {
            message = reader.next() => {
                let message = match message {
                    Some(Ok(message)) => message,
                    Some(Err(error)) => {
                        return Err(error).context("failed to read Twitch message");
                    }
                    None => return Ok(()),
                };
                idle_watchdog = Box::pin(tokio::time::sleep(READ_IDLE_TIMEOUT));

                let Message::Text(text) = message else {
                    continue;
                };

                for line in text.lines() {
                    if line.starts_with("PING ") {
                        // PONG must never wait behind slow command handlers, or Twitch
                        // will close the socket. Answer it immediately.
                        let response = line.replacen("PING", "PONG", 1);
                        send_with_timeout(&writer, Message::Text(response.into())).await?;
                        continue;
                    }

                    let Some(privmsg) = Privmsg::parse(line) else {
                        continue;
                    };

                    // Handle each command in its own task so a slow handler (YouTube
                    // search, Spotify API, ...) cannot stall the read loop and PONGs.
                    let Ok(permit) = handlers.clone().try_acquire_owned() else {
                        continue;
                    };
                    let state = state.clone();
                    let channel = secrets.channel.clone();
                    let writer = writer.clone();
                    let rate_limiter = rate_limiter.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        let result = timeout(
                            HANDLER_TIMEOUT,
                            handle_privmsg(&state, privmsg, &rate_limiter),
                        )
                        .await;
                        let reply = match result {
                            Ok(Some(reply)) => reply,
                            Ok(None) => return,
                            Err(_) => {
                                state
                                    .record_event(
                                        "error",
                                        "команда перервана за таймаут",
                                    )
                                    .await;
                                return;
                            }
                        };
                        let reply = format!(
                            "PRIVMSG #{} :{}",
                            channel,
                            sanitize_irc_reply(&reply)
                        );
                        let _ =
                            send_with_timeout(&writer, Message::Text(reply.into())).await;
                    });
                }
            }
            _ = &mut idle_watchdog => {
                anyhow::bail!("no Twitch messages within {READ_IDLE_TIMEOUT:?}; forcing reconnect");
            }
            _ = keepalive.tick() => {
                // A failed PING means the send path is gone too -> reconnect.
                send_with_timeout(
                    &writer,
                    Message::Text("PING :tmi.twitch.tv".into()),
                )
                .await?;
            }
        }
    }
}

/// Send a Twitch IRC message with a hard timeout so a blocked socket can
/// never pin the shared writer lock.
async fn send_with_timeout<S>(writer: &Arc<Mutex<S>>, message: Message) -> Result<()>
where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let mut guard = writer.lock().await;
    timeout(SEND_TIMEOUT, guard.send(message))
        .await
        .map_err(|_| anyhow!("timed out sending Twitch message"))??;
    Ok(())
}

async fn handle_privmsg(
    state: &AppState,
    privmsg: Privmsg,
    rate_limiter: &Arc<Mutex<ChatRateLimiter>>,
) -> Option<String> {
    let settings = config::command_settings(&state.config.paths);
    let command = parse_chat_command(
        ChatCommandInput {
            requester: privmsg.sender.clone(),
            message: privmsg.message.clone(),
            is_moderator: privmsg.role >= ChatUserRole::Moderator,
            role: privmsg.role,
        },
        &settings,
    );
    if !matches!(command, ChatCommand::Ignored) {
        state
            .record_event(
                "chat",
                format!(
                    "{} ({:?}): {}",
                    privmsg.sender, privmsg.role, privmsg.message
                ),
            )
            .await;
    }

    if let Some(reply) = {
        // Rate-limiter lock is only held for this synchronous check, never
        // across the heavy command body below, so one slow command cannot
        // freeze the limiter for everyone else.
        let mut limiter = rate_limiter.lock().await;
        limiter.check(&privmsg, &command)
    } {
        state.record_event("chat", reply.clone()).await;
        return Some(reply);
    }

    match command {
        ChatCommand::SongRequest { input, role } => {
            let requester = input.requester.clone();
            match request_flow::add_request_for_role(state, input, role).await {
                Ok(request) => {
                    let display_title = display::chat_song_title(&request);
                    state
                        .record_event(
                            "request",
                            format!("{} pediu {}", request.requester, display_title),
                        )
                        .await;
                    request_flow::sync_backup_queue(state).await;
                    save_queue_if_enabled(state).await;
                    let queue = state.queue.read().await;
                    let position = queue.position_of(request.id);
                    drop(queue);
                    match position {
                        Some(1) => Some(format!(
                            "@{requester} запит додано: {display_title}. Зараз грає."
                        )),
                        Some(pos) => Some(format!(
                            "@{requester} запит додано: {display_title}. Позиція: #{pos}."
                        )),
                        None => Some(format!("@{requester} запит додано: {display_title}.")),
                    }
                }
                Err(error) => {
                    let message = format!("@{requester} не вдалося додати: {error}");
                    state.record_event("error", message.clone()).await;
                    Some(message)
                }
            }
        }
        ChatCommand::CurrentSong => Some(current_song_reply(state).await),
        ChatCommand::Queue => Some(queue_reply(state).await),
        ChatCommand::RemoveLast { requester } => {
            Some(remove_last_request_reply(state, requester).await)
        }
        ChatCommand::Skip { requester } => Some(skip_reply(state, requester).await),
        ChatCommand::Playback { requester, action } => {
            Some(playback_reply(state, requester, action).await)
        }
        ChatCommand::Volume { requester, level } => {
            Some(volume_reply(state, requester, level).await)
        }
        ChatCommand::Help => Some(format!("Команди: {}.", help_commands(&settings).join(", "))),
        ChatCommand::AccessDenied {
            requester,
            command,
            required,
        } => {
            let message = access_denied_reply(requester, &command, required);
            state.record_event("access", message.clone()).await;
            Some(message)
        }
        ChatCommand::Ignored => None,
    }
}

#[derive(Default)]
struct ChatRateLimiter {
    last_command: HashMap<String, Instant>,
    last_song_request: HashMap<String, Instant>,
    last_remove: HashMap<String, Instant>,
}

impl ChatRateLimiter {
    fn check(&mut self, privmsg: &Privmsg, command: &ChatCommand) -> Option<String> {
        if privmsg.role >= ChatUserRole::Moderator
            || matches!(
                command,
                ChatCommand::Ignored | ChatCommand::AccessDenied { .. }
            )
        {
            return None;
        }

        let now = Instant::now();
        let user = privmsg.sender.to_ascii_lowercase();
        if Self::is_limited(&mut self.last_command, &user, now, Duration::from_secs(2)) {
            return Some(format!(
                "@{} зачекай кілька секунд перед наступною командою.",
                privmsg.sender
            ));
        }

        match command {
            ChatCommand::SongRequest { .. } => {
                if Self::is_limited(
                    &mut self.last_song_request,
                    &user,
                    now,
                    Duration::from_secs(10),
                ) {
                    return Some(format!(
                        "@{} зачекай кілька секунд перед наступною піснею.",
                        privmsg.sender
                    ));
                }
            }
            ChatCommand::RemoveLast { .. }
                if Self::is_limited(&mut self.last_remove, &user, now, Duration::from_secs(5)) =>
            {
                return Some(format!(
                    "@{} зачекай кілька секунд перед повторним видаленням.",
                    privmsg.sender
                ));
            }
            _ => {}
        }

        None
    }

    fn is_limited(
        store: &mut HashMap<String, Instant>,
        user: &str,
        now: Instant,
        cooldown: Duration,
    ) -> bool {
        if store
            .get(user)
            .is_some_and(|last| now.duration_since(*last) < cooldown)
        {
            return true;
        }

        store.insert(user.to_string(), now);
        false
    }
}

fn sanitize_irc_reply(reply: &str) -> String {
    reply
        .chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(430)
        .collect()
}

async fn remove_last_request_reply(state: &AppState, requester: String) -> String {
    let removed = {
        let mut queue = state.queue.write().await;
        queue.remove_last_by_requester(&requester)
    };
    save_queue_if_enabled(state).await;

    let message = match removed {
        Some(song) => {
            let suffix = if matches!(
                song.source,
                crate::song_requests::RequestSource::Spotify { .. }
            ) {
                " Якщо вона вже у внутрішній черзі Spotify, пропусти її командою skip, коли дійде."
            } else {
                ""
            };
            let title = display::chat_song_title(&song);
            format!("@{requester} прибрав твій останній запит: {title}.{suffix}")
        }
        None => format!("@{requester} не знайшов тві запит у черзі для видалення."),
    };
    state.record_event("request", message.clone()).await;
    message
}

async fn skip_reply(state: &AppState, requester: String) -> String {
    if is_youtube_browser_mode(state) {
        let message = local_browser_skip_reply(state, &requester).await;
        state.record_event("player", message.clone()).await;
        return message;
    }

    if matches!(current_provider(state), MusicProvider::Youtube)
        && matches!(current_youtube_playback(state), YoutubePlayback::Pear)
    {
        let message = pear_skip_reply(state, &requester).await;
        state.record_event("player", message.clone()).await;
        return message;
    }

    if let Some(message) =
        spotify_playback_reply(state, requester.clone(), PlaybackAction::Next).await
    {
        state.record_event("player", message.clone()).await;
        return message;
    }

    let current_song = state.queue.write().await.skip();
    save_queue_if_enabled(state).await;
    let message = match current_song {
        Some(song) => format!(
            "@{requester} пропустив. Зараз: {}",
            display::chat_song_title(&song)
        ),
        None => format!("@{requester} пропустив. Черга порожня."),
    };
    state.record_event("player", message.clone()).await;
    message
}

async fn pear_skip_reply(state: &AppState, requester: &str) -> String {
    match crate::pear::skip_next(&state.config).await {
        Ok(outcome) if outcome.changed => {
            let suffix = if outcome.fallback_used {
                " через внутрішню чергу"
            } else {
                ""
            };
            match outcome.after {
                Some(after) => format!("@{requester} пропустив у Pear{suffix}. Зараз: {after}."),
                None => format!("@{requester} пропустив у Pear{suffix}."),
            }
        }
        Ok(outcome) => {
            let current = outcome
                .after
                .or(outcome.before)
                .map(|song| format!(" Поточна пісня: {song}."))
                .unwrap_or_default();
            format!("@{requester} відправив skip у Pear, але пісня не змінилася.{current}")
        }
        Err(error) => format!("@{requester} не вдалося пропустити у Pear: {error}"),
    }
}

fn help_commands(settings: &crate::commands::CommandSettings) -> Vec<String> {
    let aliases = &settings.aliases;
    vec![
        format!("{} назва/посилання", aliases.song_request[0]),
        aliases.current_song[0].clone(),
        aliases.queue[0].clone(),
        aliases.remove[0].clone(),
        aliases.volume[0].clone(),
        format!("{} 30 (мод)", aliases.volume[0]),
        format!("{} (мод)", aliases.play[0]),
        format!("{} (мод)", aliases.pause[0]),
        format!("{} (мод)", aliases.skip[0]),
    ]
}

async fn playback_reply(state: &AppState, requester: String, action: PlaybackAction) -> String {
    if is_youtube_browser_mode(state) {
        let message = match action {
            PlaybackAction::Next => local_browser_skip_reply(state, &requester).await,
            PlaybackAction::Play | PlaybackAction::Pause => {
                local_browser_playback_reply(state, &requester, action)
            }
        };
        state.record_event("player", message.clone()).await;
        return message;
    }

    if matches!(current_provider(state), MusicProvider::Youtube)
        && matches!(current_youtube_playback(state), YoutubePlayback::Pear)
    {
        let message = match action {
            PlaybackAction::Play => match crate::pear::play(&state.config).await {
                Ok(()) => format!("@{requester} Pear відновлено."),
                Err(error) => format!("@{requester} не вдалося керувати Pear: {error}"),
            },
            PlaybackAction::Pause => match crate::pear::pause(&state.config).await {
                Ok(()) => format!("@{requester} Pear на паузі."),
                Err(error) => format!("@{requester} не вдалося керувати Pear: {error}"),
            },
            PlaybackAction::Next => pear_skip_reply(state, &requester).await,
        };
        state.record_event("player", message.clone()).await;
        return message;
    }

    let message = spotify_playback_reply(state, requester, action)
        .await
        .unwrap_or_else(|| "Spotify не підключено.".to_string());
    state.record_event("player", message.clone()).await;
    message
}

async fn spotify_playback_reply(
    state: &AppState,
    requester: String,
    action: PlaybackAction,
) -> Option<String> {
    let mut token_guard = state.spotify_token.write().await;
    let token = token_guard.as_mut()?;

    let result = match action {
        PlaybackAction::Play => crate::spotify::resume_playback(&state.config, token).await,
        PlaybackAction::Pause => crate::spotify::pause_playback(&state.config, token).await,
        PlaybackAction::Next => crate::spotify::skip_next(&state.config, token).await,
    };

    Some(match result {
        Ok(()) => match action {
            PlaybackAction::Play => format!("@{requester} відтворення відновлено."),
            PlaybackAction::Pause => format!("@{requester} відтворення на паузі."),
            PlaybackAction::Next => format!("@{requester} пропустив на наступну."),
        },
        Err(error) => format!("@{requester} не вдалося керувати Spotify: {error}"),
    })
}

/// Human label for the backup track currently in the queue: real title/artist
/// from the YouTube Data API (cached in AppState), falling back to "Бекап N".
async fn backup_track_label(state: &AppState) -> Option<String> {
    let (url, title) = {
        let queue = state.queue.read().await;
        let song = queue
            .view()
            .current_song
            .filter(|song| matches!(song.source, RequestSource::Backup))?;
        (song.query.clone(), song.title.clone())
    };
    let video_id = youtube::YoutubeVideoRef::parse(&url)?.video_id;

    {
        let cache = state.video_meta_cache.lock().await;
        if let Some((title, artist)) = cache.get(&video_id) {
            return Some(format!("{title} — {artist}"));
        }
    }

    if let Some((title, artist)) = youtube::video_title_artist(&state.config, &video_id).await {
        state
            .video_meta_cache
            .lock()
            .await
            .insert(video_id.clone(), (title.clone(), artist.clone()));
        return Some(format!("{title} — {artist}"));
    }

    Some(title)
}

async fn current_song_reply(state: &AppState) -> String {
    match current_provider(state) {
        MusicProvider::Spotify => {
            if let Some(snapshot) = spotify_queue_snapshot(state).await {
                if let Some(song) = snapshot.currently_playing {
                    return format!("Зараз грає: {song}");
                }
            }
        }
        MusicProvider::Youtube
            if matches!(current_youtube_playback(state), YoutubePlayback::Pear) =>
        {
            if let Ok(Some(song)) = crate::pear::now_playing_request(&state.config).await {
                return format!(
                    "Зараз грає: {} — запит від {}",
                    display::chat_song_title(&song),
                    song.requester
                );
            }
        }
        MusicProvider::Youtube => {}
    }

    let queue = active_app_queue_view(state).await;
    match queue.current_song {
        Some(song) => format!(
            "Зараз грає: {} — запит від {}",
            display::chat_song_title(&song),
            song.requester
        ),
        None => {
            if let Some(label) = backup_track_label(state).await {
                format!("Зараз грає: {label} — бекап-черга")
            } else {
                "Зараз нічого не грає.".to_string()
            }
        }
    }
}

async fn queue_reply(state: &AppState) -> String {
    if matches!(current_provider(state), MusicProvider::Youtube)
        && matches!(current_youtube_playback(state), YoutubePlayback::Pear)
    {
        return pear_queue_reply(state).await;
    }

    if matches!(current_provider(state), MusicProvider::Spotify) {
        if let Some(snapshot) = spotify_queue_snapshot(state).await {
            if snapshot.upcoming.is_empty() {
                return snapshot
                    .currently_playing
                    .map(|song| format!("Зараз грає: {song}. Черга порожня."))
                    .unwrap_or_else(|| "Черга порожня.".to_string());
            }

            return format!("Далі: {}", snapshot.upcoming.join(" | "));
        }
    }

    app_queue_reply(state).await
}

async fn pear_queue_reply(state: &AppState) -> String {
    match crate::pear::queue_display_items(&state.config).await {
        Ok(items) if !items.is_empty() => {
            let selected = items.iter().position(|item| item.selected);
            let current = selected
                .and_then(|index| items.get(index))
                .map(pear_queue_item_label);
            let start = selected.map_or(0, |index| index.saturating_add(1));
            let upcoming = items
                .iter()
                .skip(start)
                .take(5)
                .map(pear_queue_item_label)
                .collect::<Vec<_>>();

            match (current, upcoming.is_empty()) {
                (Some(current), false) => {
                    format!("Pear грає: {current}. Далі: {}", upcoming.join(" | "))
                }
                (Some(current), true) => format!("Pear грає: {current}. Далі в Pear порожньо."),
                (None, false) => format!("У Pear далі: {}", upcoming.join(" | ")),
                (None, true) => app_queue_reply(state).await,
            }
        }
        Ok(_) => app_queue_reply(state).await,
        Err(error) => format!("Не вдалося прочитати чергу Pear: {error}"),
    }
}

fn pear_queue_item_label(item: &crate::pear::PearQueueDisplayItem) -> String {
    match &item.artist {
        Some(artist) if !artist.trim().is_empty() => format!("{} - {}", artist, item.title),
        _ => item.title.clone(),
    }
}

async fn app_queue_reply(state: &AppState) -> String {
    let queue = active_app_queue_view(state).await;
    let current = queue.current_song.map(|song| {
        format!(
            "Зараз грає: {} — запит від {}",
            display::chat_song_title(&song),
            song.requester
        )
    });
    let upcoming = queue
        .queue
        .into_iter()
        .take(5)
        .map(|song| {
            format!(
                "{} — запит від {}",
                display::chat_song_title(&song),
                song.requester
            )
        })
        .collect::<Vec<_>>();

    match (current, upcoming.is_empty()) {
        (Some(current), false) => format!("{current}. Далі: {}", upcoming.join(" | ")),
        (Some(current), true) => format!("{current}. Черга порожня."),
        (None, false) => format!("Далі: {}", upcoming.join(" | ")),
        (None, true) => "Черга порожня.".to_string(),
    }
}

async fn volume_reply(state: &AppState, requester: String, level: Option<u8>) -> String {
    match level {
        Some(level) => {
            let level = level.clamp(1, 100);
            let mut changed = Vec::new();
            let mut errors = Vec::new();

            match (current_provider(state), current_youtube_playback(state)) {
                (MusicProvider::Youtube, YoutubePlayback::Pear) => {
                    match crate::pear::set_volume(&state.config, level).await {
                        Ok(level) => {
                            persist_volume_setting(
                                state,
                                MusicProvider::Youtube,
                                YoutubePlayback::Pear,
                                level,
                            )
                            .await;
                            changed.push(format!("Pear/YouTube {level}%"));
                        }
                        Err(error) => errors.push(format!("Pear/YouTube: {error}")),
                    }
                }
                (MusicProvider::Youtube, YoutubePlayback::Browser) => {
                    let level = set_browser_volume(state, level).await;
                    changed.push(format!("OBS Browser {level}%"));
                }
                (MusicProvider::Spotify, _) => match set_spotify_volume(state, level).await {
                    Some(Ok(level)) => {
                        persist_volume_setting(
                            state,
                            MusicProvider::Spotify,
                            YoutubePlayback::Browser,
                            level,
                        )
                        .await;
                        changed.push(format!("Spotify {level}%"));
                    }
                    Some(Err(error)) => errors.push(format!("Spotify: {error}")),
                    None => errors.push("Spotify не підключено".to_string()),
                },
            }

            if !changed.is_empty() {
                let message = format!("@{requester} гучність змінено: {}.", changed.join(", "));
                state.record_event("volume", message.clone()).await;
                if !errors.is_empty() {
                    state
                        .record_event(
                            "volume",
                            format!(
                                "Гучність змінено частково: {}. Помилки: {}",
                                changed.join(", "),
                                errors.join(" | ")
                            ),
                        )
                        .await;
                }
                message
            } else {
                let message = format!(
                    "@{requester} не вдалося змінити гучність: {}",
                    errors.join(" | ")
                );
                state.record_event("error", message.clone()).await;
                message
            }
        }
        None => current_volume_reply(state).await,
    }
}

async fn set_spotify_volume(state: &AppState, level: u8) -> Option<Result<u8>> {
    let mut token_guard = state.spotify_token.write().await;
    let token = token_guard.as_mut()?;
    Some(crate::spotify::set_volume(&state.config, token, level).await)
}

async fn set_browser_volume(state: &AppState, level: u8) -> u8 {
    let level = level.clamp(1, 100);
    state.youtube_browser_volume.store(level, Ordering::SeqCst);
    persist_volume_setting(
        state,
        MusicProvider::Youtube,
        YoutubePlayback::Browser,
        level,
    )
    .await;
    level
}

async fn persist_volume_setting(
    state: &AppState,
    provider: MusicProvider,
    playback: YoutubePlayback,
    level: u8,
) {
    if let Err(error) =
        config::update_volume_setting(&state.config.paths, provider, playback, level)
    {
        state
            .record_event("error", format!("Nao consegui salvar volume: {error}"))
            .await;
    }
}

async fn current_volume_reply(state: &AppState) -> String {
    if matches!(
        (current_provider(state), current_youtube_playback(state)),
        (MusicProvider::Youtube, YoutubePlayback::Pear)
    ) {
        return match crate::pear::current_volume(&state.config).await {
            Ok(volume) => {
                let suffix = if volume.is_muted {
                    " (вимкнено)"
                } else {
                    ""
                };
                format!("Поточна гучність Pear/YouTube: {}%{suffix}.", volume.state)
            }
            Err(error) => format!("Не вдалося прочитати гучність Pear/YouTube: {error}"),
        };
    }

    if is_youtube_browser_mode(state) {
        let level = state
            .youtube_browser_volume
            .load(Ordering::SeqCst)
            .clamp(1, 100);
        return format!(
            "Поточна гучність OBS Browser: {level}%. Фейдер OBS залишається окремим у мікшері."
        );
    }

    let mut token_guard = state.spotify_token.write().await;
    let Some(token) = token_guard.as_mut() else {
        return "Spotify не підключено.".to_string();
    };

    match crate::spotify::current_volume(&state.config, token).await {
        Ok(Some(level)) => format!("Поточна гучність Spotify: {level}%."),
        Ok(None) => "Не знайшов активний пристрій Spotify для зчитування гучності.".to_string(),
        Err(error) => format!("Не вдалося прочитати гучність Spotify: {error}"),
    }
}

async fn active_app_queue_view(state: &AppState) -> QueueView {
    match current_provider(state) {
        MusicProvider::Spotify => {
            filtered_app_queue_view(state, MusicProvider::Spotify, None).await
        }
        MusicProvider::Youtube => {
            let playback = match current_youtube_playback(state) {
                YoutubePlayback::Pear => YoutubeRequestPlayback::Pear,
                YoutubePlayback::Browser => YoutubeRequestPlayback::Browser,
            };
            filtered_app_queue_view(state, MusicProvider::Youtube, Some(playback)).await
        }
    }
}

async fn filtered_app_queue_view(
    state: &AppState,
    provider: MusicProvider,
    playback: Option<YoutubeRequestPlayback>,
) -> QueueView {
    let view = state.queue.read().await.view();
    let mut requests = Vec::new();
    if let Some(song) = view.current_song {
        requests.push(song);
    }
    requests.extend(view.queue);
    let mut requests = requests
        .into_iter()
        .filter(|song| request_matches_stream(song, provider, playback))
        .collect::<Vec<_>>();
    let current_song = requests.first().cloned();
    if current_song.is_some() {
        requests.remove(0);
    }
    QueueView {
        current_song,
        queue_length: requests.len(),
        queue: requests,
        persistence: None,
    }
}

fn request_matches_stream(
    song: &SongRequest,
    provider: MusicProvider,
    playback: Option<YoutubeRequestPlayback>,
) -> bool {
    match provider {
        MusicProvider::Spotify => matches!(
            song.source,
            RequestSource::Spotify { .. }
                | RequestSource::Search {
                    provider: MusicProvider::Spotify
                }
        ),
        MusicProvider::Youtube => match (&song.source, playback) {
            (
                RequestSource::Youtube {
                    playback: Some(source_playback),
                    ..
                },
                Some(active_playback),
            ) => *source_playback == active_playback,
            _ => false,
        },
    }
}

fn current_provider(state: &AppState) -> MusicProvider {
    config::UiConfigView::load(&state.config.paths).default_provider
}

fn current_youtube_playback(state: &AppState) -> YoutubePlayback {
    config::UiConfigView::load(&state.config.paths).youtube_playback
}

fn is_youtube_browser_mode(state: &AppState) -> bool {
    matches!(current_provider(state), MusicProvider::Youtube)
        && matches!(current_youtube_playback(state), YoutubePlayback::Browser)
}

async fn save_queue_if_enabled(state: &AppState) {
    if !config::queue_persistence_enabled(&state.config.paths) {
        return;
    }

    if let Err(error) = state
        .queue
        .read()
        .await
        .save(&state.config.paths.queue_file)
    {
        state.record_event("error", error.to_string()).await;
    }
}

async fn local_browser_skip_reply(state: &AppState, requester: &str) -> String {
    state.set_browser_paused(false);

    let current_backup = {
        let queue = state.queue.read().await;
        queue.is_current_backup()
    };

    if current_backup {
        // Backup entries are part of the queue, but their id is a playlist
        // cursor: skipping one means advancing the playlist and resyncing.
        state.advance_and_sync(&state.config.paths).await;
        state
            .record_event(
                "player",
                "Бекап-трек пропущено з чату; наступний з бекап-списку",
            )
            .await;

        if let Some(label) = current_song_reply(state).await.strip_prefix("Зараз грає: ") {
            return format!("@{requester} пропустив. Зараз: {label}");
        }
        return format!("@{requester} пропустив. Черга порожня.");
    }

    let current_id = filtered_app_queue_view(
        state,
        MusicProvider::Youtube,
        Some(YoutubeRequestPlayback::Browser),
    )
    .await
    .current_song
    .map(|song| song.id);

    if let Some(id) = current_id {
        let mut queue = state.queue.write().await;
        queue.remove_by_id(id);
        drop(queue);
        save_queue_if_enabled(state).await;
    }

    let current_song = filtered_app_queue_view(
        state,
        MusicProvider::Youtube,
        Some(YoutubeRequestPlayback::Browser),
    )
    .await
    .current_song;

    match current_song {
        Some(song) => format!(
            "@{requester} пропустив. Зараз: {}",
            display::chat_song_title(&song)
        ),
        None => format!("@{requester} пропустив. Черга порожня."),
    }
}

fn local_browser_playback_reply(
    state: &AppState,
    requester: &str,
    action: PlaybackAction,
) -> String {
    match action {
        PlaybackAction::Play => {
            state.set_browser_paused(false);
            format!("@{requester} OBS-плеєр відновлено.")
        }
        PlaybackAction::Pause => {
            state.set_browser_paused(true);
            format!("@{requester} OBS-плеєр на паузі.")
        }
        PlaybackAction::Next => {
            state.set_browser_paused(false);
            format!("@{requester} пропускаю в OBS-плеєрі.")
        }
    }
}

fn access_denied_reply(
    requester: String,
    command: &str,
    required: crate::commands::CommandAccess,
) -> String {
    match required {
        crate::commands::CommandAccess::Follower => {
            format!("@{requester} {command} потребує підписки на канал.")
        }
        crate::commands::CommandAccess::Subscriber => {
            format!("@{requester} {command} потребує саба, VIP, модератора або стрімера.")
        }
        crate::commands::CommandAccess::Vip => {
            format!("@{requester} {command} потребує VIP, модератора або стрімера.")
        }
        crate::commands::CommandAccess::Moderator => {
            format!("@{requester} {command} потребує модератора або стрімера.")
        }
        crate::commands::CommandAccess::Streamer => {
            format!("@{requester} {command} потребує стрімера.")
        }
    }
}

async fn spotify_queue_snapshot(state: &AppState) -> Option<crate::spotify::SpotifyQueueSnapshot> {
    let mut token_guard = state.spotify_token.write().await;
    let token = token_guard.as_mut()?;

    crate::spotify::queue_snapshot(&state.config, token)
        .await
        .ok()
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Privmsg {
    sender: String,
    message: String,
    role: ChatUserRole,
}

impl Privmsg {
    fn parse(line: &str) -> Option<Self> {
        if !line.contains(" PRIVMSG ") {
            return None;
        }

        let (tags, rest) = if let Some(stripped) = line.strip_prefix('@') {
            let (tags, rest) = stripped.split_once(' ')?;
            (Some(tags), rest)
        } else {
            (None, line)
        };

        let sender = parse_sender(rest)?;
        let message = rest.split_once(" :")?.1.to_string();
        let role = ChatUserRole::from_twitch_tags(tags);

        Some(Self {
            sender,
            message,
            role,
        })
    }
}

fn parse_sender(rest: &str) -> Option<String> {
    let sender_part = rest.strip_prefix(':')?.split_once('!')?.0;

    if sender_part.is_empty() {
        None
    } else {
        Some(sender_part.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn app_queue_reply_shows_current_and_upcoming_requests() {
        let mut config = crate::config::AppConfig::from_env().expect("config");
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("song-request-linux-twitch-{unique}"));
        config.paths.config_dir = root.join("config");
        config.paths.state_dir = root.join("state");
        config.paths.queue_file = config.paths.state_dir.join("queue.json");
        crate::config::save_ui_config(
            &config.paths,
            crate::config::UiConfigInput {
                default_provider: Some(MusicProvider::Youtube),
                youtube_playback: Some(YoutubePlayback::Browser),
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
        state.queue.write().await.clear();
        state
            .queue
            .write()
            .await
            .add_resolved(crate::song_requests::SongRequest {
                id: 0,
                requester: "viewer".to_string(),
                query: "https://youtu.be/current".to_string(),
                source: crate::song_requests::RequestSource::Youtube {
                    video_id: "current".to_string(),
                    playback: Some(crate::song_requests::YoutubeRequestPlayback::Browser),
                },
                title: "Current Song".to_string(),
                artist: "Current Artist".to_string(),
            });
        state
            .queue
            .write()
            .await
            .add_resolved(crate::song_requests::SongRequest {
                id: 0,
                requester: "mod".to_string(),
                query: "https://youtu.be/next".to_string(),
                source: crate::song_requests::RequestSource::Youtube {
                    video_id: "next".to_string(),
                    playback: Some(crate::song_requests::YoutubeRequestPlayback::Browser),
                },
                title: "Next Song".to_string(),
                artist: "Next Artist".to_string(),
            });

        let reply = app_queue_reply(&state).await;

        assert!(reply.contains("Зараз грає: Current Artist - Current Song — запит від viewer"));
        assert!(reply.contains("Далі: Next Artist - Next Song — запит від mod"));
    }

    #[test]
    fn parses_privmsg_with_tags() {
        let line = "@badge-info=;badges=moderator/1;color=#fff;mod=1 :viewer!viewer@viewer.tmi.twitch.tv PRIVMSG #heyleao :!sr one more time";
        let message = Privmsg::parse(line).expect("privmsg");

        assert_eq!(message.sender, "viewer");
        assert_eq!(message.message, "!sr one more time");
        assert_eq!(message.role, ChatUserRole::Moderator);
    }

    #[test]
    fn parses_privmsg_with_subscriber_tag() {
        let line = "@badge-info=subscriber/12;badges=subscriber/12;color=#fff;mod=0;subscriber=1 :viewer!viewer@viewer.tmi.twitch.tv PRIVMSG #heyleao :!sr spiders";
        let message = Privmsg::parse(line).expect("privmsg");

        assert_eq!(message.role, ChatUserRole::Subscriber);
    }

    #[test]
    fn parses_plain_privmsg_as_follower() {
        let line = "@badge-info=;badges=;color=#fff;mod=0 :viewer!viewer@viewer.tmi.twitch.tv PRIVMSG #heyleao :!sr spiders";
        let message = Privmsg::parse(line).expect("privmsg");

        assert_eq!(message.role, ChatUserRole::Follower);
    }

    #[test]
    fn parses_privmsg_with_vip_badge() {
        let line = "@badge-info=;badges=vip/1;color=#fff;mod=0 :viewer!viewer@viewer.tmi.twitch.tv PRIVMSG #heyleao :!sr one more time";
        let message = Privmsg::parse(line).expect("privmsg");

        assert_eq!(message.role, ChatUserRole::Vip);
    }

    #[test]
    fn parses_privmsg_with_broadcaster_badge() {
        let line = "@badge-info=;badges=broadcaster/1;color=#fff;mod=0 :heyleao!heyleao@heyleao.tmi.twitch.tv PRIVMSG #heyleao :!skip";
        let message = Privmsg::parse(line).expect("privmsg");

        assert_eq!(message.role, ChatUserRole::Streamer);
    }

    #[test]
    fn ignores_non_privmsg() {
        assert!(Privmsg::parse(":tmi.twitch.tv 001 bot :Welcome").is_none());
    }

    #[test]
    fn sanitizes_irc_reply_controls_and_length() {
        let reply = sanitize_irc_reply("@viewer ok\r\nPRIVMSG #x :owned\u{0007}");

        assert!(!reply.contains('\r'));
        assert!(!reply.contains('\n'));
        assert!(!reply.contains('\u{0007}'));
        assert!(reply.len() <= 430);
    }
}
