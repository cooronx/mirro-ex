use std::fs;

use anyhow::{Result, anyhow};
use chrono::{FixedOffset, Utc};
use tracing_subscriber::Layer;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt::format::Writer;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::layer::SubscriberExt;
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

    let (file_writer, file_guard) = if config.to_file {
        fs::create_dir_all(&config.directory)?;
        let appender = tracing_appender::rolling::daily(&config.directory, &config.file_prefix);
        let (non_blocking, guard) = tracing_appender::non_blocking(appender);
        (Some(non_blocking), Some(guard))
    } else {
        (None, None)
    };

    init_subscriber(config.to_stdout, file_writer, level_filter);

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

fn init_subscriber(
    to_stdout: bool,
    file_writer: Option<tracing_appender::non_blocking::NonBlocking>,
    level_filter: LevelFilter,
) {
    match (to_stdout, file_writer) {
        (true, Some(file_writer)) => tracing_subscriber::registry()
            .with(stdout_layer(level_filter))
            .with(file_layer(file_writer, level_filter))
            .init(),
        (true, None) => tracing_subscriber::registry()
            .with(stdout_layer(level_filter))
            .init(),
        (false, Some(file_writer)) => tracing_subscriber::registry()
            .with(file_layer(file_writer, level_filter))
            .init(),
        (false, None) => unreachable!("logging output must be configured"),
    }
}

fn stdout_layer<S>(level_filter: LevelFilter) -> impl Layer<S>
where
    S: tracing::Subscriber,
    for<'a> S: tracing_subscriber::registry::LookupSpan<'a>,
{
    tracing_subscriber::fmt::layer()
        .pretty()
        .with_file(false)
        .with_line_number(false)
        .with_ansi(false)
        .with_target(false)
        .with_timer(ShanghaiTimer)
        .with_writer(std::io::stdout)
        .with_filter(level_filter)
}

fn file_layer<S>(
    writer: tracing_appender::non_blocking::NonBlocking,
    level_filter: LevelFilter,
) -> impl Layer<S>
where
    S: tracing::Subscriber,
    for<'a> S: tracing_subscriber::registry::LookupSpan<'a>,
{
    tracing_subscriber::fmt::layer()
        .compact()
        .with_ansi(false)
        .with_target(false)
        .with_timer(ShanghaiTimer)
        .with_writer(writer)
        .with_filter(level_filter)
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
