use std::time::Duration;

/// Shared HTTP client with a total request timeout so external calls
/// (YouTube/Spotify/Twitch) fail cleanly instead of hanging forever.
/// A request that never returns would otherwise wedge shared locks in the
/// Twitch bot and silently freeze chat handling.
pub fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("failed to build reqwest client")
}
