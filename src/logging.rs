use std::fs;

use anyhow::{Result, anyhow};
use chrono::{FixedOffset, Utc};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::fmt::writer::MakeWriterExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::config::LoggingConfig;

pub struct LoggingGuard {
    _file_guard: Option<tracing_appender::non_blocking::WorkerGuard>,
}

#[derive(Debug, Clone, Copy)]
struct ShanghaiTimer;

pub fn init(config: &LoggingConfig) -> Result<LoggingGuard> {
    validate_config(config)?;
    let level_filter = parse_level_filter(&config.level)?;

    let file_guard = if config.to_file {
        fs::create_dir_all(&config.directory)?;
        let appender = tracing_appender::rolling::daily(&config.directory, &config.file_prefix);
        let (non_blocking, guard) = tracing_appender::non_blocking(appender);

        match config.to_stdout {
            true => tracing_subscriber::fmt()
                .with_max_level(level_filter)
                .with_ansi(false)
                .with_target(false)
                .with_timer(ShanghaiTimer)
                .with_writer(std::io::stdout.and(non_blocking))
                .finish()
                .init(),
            false => tracing_subscriber::fmt()
                .with_max_level(level_filter)
                .with_ansi(false)
                .with_target(false)
                .with_timer(ShanghaiTimer)
                .with_writer(non_blocking)
                .finish()
                .init(),
        }

        Some(guard)
    } else {
        tracing_subscriber::fmt()
            .with_max_level(level_filter)
            .with_ansi(false)
            .with_target(false)
            .with_timer(ShanghaiTimer)
            .with_writer(std::io::stdout)
            .finish()
            .init();
        None
    };

    Ok(LoggingGuard {
        _file_guard: file_guard,
    })
}

fn validate_config(config: &LoggingConfig) -> Result<()> {
    if !config.to_stdout && !config.to_file {
        return Err(anyhow!(
            "invalid logging config: at least one of logging.to_stdout or logging.to_file must be true"
        ));
    }

    Ok(())
}

impl FormatTime for ShanghaiTimer {
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        let offset = FixedOffset::east_opt(8 * 60 * 60).expect("valid shanghai offset");
        let now = Utc::now().with_timezone(&offset);
        write!(w, "{}", now.format("%Y-%m-%d %H:%M:%S%.3f"))
    }
}

fn parse_level_filter(raw: &str) -> Result<LevelFilter> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "trace" => Ok(LevelFilter::TRACE),
        "debug" => Ok(LevelFilter::DEBUG),
        "info" => Ok(LevelFilter::INFO),
        "warn" | "warning" => Ok(LevelFilter::WARN),
        "error" => Ok(LevelFilter::ERROR),
        "off" => Ok(LevelFilter::OFF),
        other => Err(anyhow!(
            "invalid logging level: {other}, expected trace/debug/info/warn/error/off"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::validate_config;
    use crate::config::LoggingConfig;

    #[test]
    fn rejects_disabled_outputs() {
        let config = LoggingConfig {
            to_stdout: false,
            to_file: false,
            ..LoggingConfig::default()
        };

        assert!(validate_config(&config).is_err());
    }
}
