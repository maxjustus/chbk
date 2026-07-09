//! Retry utilities with configurable backoff strategies.

use std::future::Future;
use std::io::IsTerminal;
use std::time::Duration;
use tokio::time::sleep;

/// Configuration for retry behavior.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub base_delay_ms: u64,
}

impl RetryConfig {
    /// Calculate delay for a given attempt (1-indexed).
    /// Uses exponential backoff with jitter (0-50% of base delay) to prevent thundering herd.
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        use rand::Rng;
        let base_ms = self
            .base_delay_ms
            .saturating_mul(1u64 << attempt.saturating_sub(1));
        // Add jitter: 0-50% of base delay to spread out concurrent retries
        let jitter_ms = rand::thread_rng().gen_range(0..=base_ms / 2);
        Duration::from_millis(base_ms + jitter_ms)
    }
}

/// Result of a retry operation that distinguishes retryable from fatal errors.
#[derive(Debug)]
pub enum RetryResult<T, E> {
    /// Operation succeeded.
    Ok(T),
    /// Operation failed but can be retried.
    Retry(E),
    /// Operation failed fatally, don't retry.
    Fatal(E),
}

/// Execute an async operation with retry logic.
///
/// The closure receives the attempt number (1-indexed) and should return a RetryResult.
/// On Retry errors, waits according to the backoff strategy before retrying.
/// On Fatal errors or max attempts exceeded, returns the last error.
pub async fn with_retry<T, E, F, Fut>(config: &RetryConfig, context: &str, mut f: F) -> Result<T, E>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = RetryResult<T, E>>,
    E: std::fmt::Display,
{
    let mut last_error: Option<E> = None;

    for attempt in 1..=config.max_attempts {
        match f(attempt).await {
            RetryResult::Ok(value) => return Ok(value),
            RetryResult::Fatal(err) => return Err(err),
            RetryResult::Retry(err) => {
                if attempt >= config.max_attempts {
                    return Err(err);
                }

                let delay = config.delay_for_attempt(attempt);
                if !std::io::stdout().is_terminal() {
                    eprintln!(
                        "{} attempt {} failed: {}. Retrying in {} ms...",
                        context,
                        attempt,
                        err,
                        delay.as_millis()
                    );
                }
                sleep(delay).await;
                last_error = Some(err);
            }
        }
    }

    // Loop invariant: last_error is always set if we exit without returning
    #[allow(clippy::expect_used)]
    Err(last_error.expect("retry loop should have set last_error"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn test_exponential_backoff() {
        let config = RetryConfig {
            max_attempts: 5,
            base_delay_ms: 500,
        };

        // With jitter (0-50%), delay is between base and base*1.5
        let d1 = config.delay_for_attempt(1);
        let d2 = config.delay_for_attempt(2);
        let d3 = config.delay_for_attempt(3);
        let d4 = config.delay_for_attempt(4);

        assert!(d1 >= Duration::from_millis(500) && d1 <= Duration::from_millis(750));
        assert!(d2 >= Duration::from_secs(1) && d2 <= Duration::from_millis(1500));
        assert!(d3 >= Duration::from_secs(2) && d3 <= Duration::from_secs(3));
        assert!(d4 >= Duration::from_secs(4) && d4 <= Duration::from_secs(6));
    }

    #[tokio::test]
    async fn test_with_retry_succeeds_first_try() {
        let config = RetryConfig {
            max_attempts: 3,
            base_delay_ms: 500,
        };
        let attempts = AtomicU32::new(0);

        let result: Result<i32, &str> = with_retry(&config, "test", |_| {
            let _ = attempts.fetch_add(1, Ordering::SeqCst);
            async { RetryResult::Ok(42) }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_with_retry_succeeds_after_failures() {
        let config = RetryConfig {
            max_attempts: 3,
            base_delay_ms: 1, // Short delay for test
        };
        let attempts = AtomicU32::new(0);

        let result: Result<i32, String> = with_retry(&config, "test", |attempt| {
            let _ = attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt < 3 {
                    RetryResult::Retry(format!("attempt {attempt} failed"))
                } else {
                    RetryResult::Ok(42)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_with_retry_fatal_stops_immediately() {
        let config = RetryConfig {
            max_attempts: 3,
            base_delay_ms: 500,
        };
        let attempts = AtomicU32::new(0);

        let result: Result<i32, &str> = with_retry(&config, "test", |_| {
            let _ = attempts.fetch_add(1, Ordering::SeqCst);
            async { RetryResult::Fatal("fatal error") }
        })
        .await;

        assert_eq!(result.unwrap_err(), "fatal error");
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_with_retry_exhausts_attempts() {
        let config = RetryConfig {
            max_attempts: 3,
            base_delay_ms: 1,
        };
        let attempts = AtomicU32::new(0);

        let result: Result<i32, &str> = with_retry(&config, "test", |_| {
            let _ = attempts.fetch_add(1, Ordering::SeqCst);
            async { RetryResult::Retry("keep failing") }
        })
        .await;

        assert_eq!(result.unwrap_err(), "keep failing");
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }
}
