//! Server configuration settings, resource bounds, and fluent builders.

use std::time::Duration;

/// Default maximum size permitted for the HTTP request header section (64 KB).
pub const DEFAULT_MAX_HEADER_SIZE: usize = 64 * 1024;

/// Default maximum number of individual request header fields allowed (100).
pub const DEFAULT_MAX_HEADERS_COUNT: usize = 100;

/// Default idle timeout duration for persistent Keep-Alive connections (60 seconds).
pub const DEFAULT_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(60);

/// Server connection and protocol configuration settings.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Maximum allowed size in bytes for request lines and headers.
    pub max_header_size: usize,
    /// Maximum allowed number of header fields in a request.
    pub max_headers_count: usize,
    /// Whether persistent HTTP/1.1 Keep-Alive connections are permitted.
    pub keep_alive: bool,
    /// Maximum duration an idle Keep-Alive connection will wait for subsequent requests.
    pub keep_alive_timeout: Option<Duration>,
    /// Whether the server automatically sends `100 Continue` when `Expect: 100-continue` is received.
    pub expect_continue: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_header_size: DEFAULT_MAX_HEADER_SIZE,
            max_headers_count: DEFAULT_MAX_HEADERS_COUNT,
            keep_alive: true,
            keep_alive_timeout: Some(DEFAULT_KEEP_ALIVE_TIMEOUT),
            expect_continue: true,
        }
    }
}

impl ServerConfig {
    /// Creates a new `ServerConfigBuilder` initialized with default settings.
    #[must_use]
    pub fn builder() -> ServerConfigBuilder {
        ServerConfigBuilder::new()
    }
}

/// Fluent builder for [`ServerConfig`].
#[derive(Debug, Clone, Default)]
pub struct ServerConfigBuilder {
    config: ServerConfig,
}

impl ServerConfigBuilder {
    /// Creates a builder initialized with default configuration values.
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: ServerConfig::default(),
        }
    }

    /// Sets the maximum permitted size in bytes for the HTTP request header section.
    #[must_use]
    pub fn max_header_size(mut self, size: usize) -> Self {
        self.config.max_header_size = size;
        self
    }

    /// Sets the maximum permitted number of request header fields.
    #[must_use]
    pub fn max_headers_count(mut self, count: usize) -> Self {
        self.config.max_headers_count = count;
        self
    }

    /// Configures whether persistent Keep-Alive connections are enabled.
    #[must_use]
    pub fn keep_alive(mut self, enabled: bool) -> Self {
        self.config.keep_alive = enabled;
        self
    }

    /// Sets the idle Keep-Alive timeout for persistent connections.
    #[must_use]
    pub fn keep_alive_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.config.keep_alive_timeout = timeout;
        self
    }

    /// Configures whether `Expect: 100-continue` handshakes are automatically processed.
    #[must_use]
    pub fn expect_continue(mut self, enabled: bool) -> Self {
        self.config.expect_continue = enabled;
        self
    }

    /// Consumes the builder and returns the completed [`ServerConfig`].
    #[must_use]
    pub fn build(self) -> ServerConfig {
        self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_config_builder() {
        let config = ServerConfig::builder()
            .max_header_size(16 * 1024)
            .max_headers_count(50)
            .keep_alive(false)
            .keep_alive_timeout(None)
            .expect_continue(false)
            .build();

        assert_eq!(config.max_header_size, 16 * 1024);
        assert_eq!(config.max_headers_count, 50);
        assert!(!config.keep_alive);
        assert_eq!(config.keep_alive_timeout, None);
        assert!(!config.expect_continue);
    }
}
