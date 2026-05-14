use clickhouse::Row;
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelMessageRange {
    pub channel: i64,
    pub begin_message_number: i64,
    pub end_message_number: i64,
}

#[derive(Debug, Row, Deserialize)]
pub(crate) struct RawOrderMessageRange {
    pub(crate) min_seq: i64,
    pub(crate) max_seq: i64,
    pub(crate) channel: i64,
}

pub(crate) fn validate_time_range<E>(
    start_time_ms: i64,
    end_time_ms: i64,
    make_err: impl FnOnce(i64, i64) -> E,
) -> std::result::Result<(), E> {
    if end_time_ms <= start_time_ms {
        return Err(make_err(start_time_ms, end_time_ms));
    }

    Ok(())
}

pub(crate) fn validate_message_range<E>(
    begin_message_number: i64,
    end_message_number: i64,
    make_err: impl FnOnce(i64, i64) -> E,
) -> std::result::Result<(), E> {
    if end_message_number <= begin_message_number {
        return Err(make_err(begin_message_number, end_message_number));
    }

    Ok(())
}

pub(crate) fn build_message_range<E>(
    value: RawOrderMessageRange,
    make_overflow_err: impl FnOnce(i64, i64) -> E,
) -> std::result::Result<ChannelMessageRange, E> {
    let end_message_number = value
        .max_seq
        .checked_add(1)
        .ok_or_else(|| make_overflow_err(value.channel, value.max_seq))?;

    Ok(ChannelMessageRange {
        channel: value.channel,
        begin_message_number: value.min_seq,
        end_message_number,
    })
}
