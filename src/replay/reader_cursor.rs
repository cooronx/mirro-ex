use crate::common::Market;

/// 回放数据类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayDataKind {
    /// 逐笔委托
    Order,
    /// 逐笔成交
    Transaction,
}

/// 单个数据源在一次回放窗口内的消息号范围。
///
/// 一个 `ChannelRange` 对应一条独立 source，通常由
/// `day + data_kind + market + channel + table_name` 唯一确定。
///
/// 消息号范围统一采用半开区间 `[begin_message_number, end_message_number)`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelRange {
    /// 交易日，例如 `2026-05-12`。
    pub day: String,
    /// 本次回放窗口的起始时间戳（包含），单位毫秒。
    pub start_time_ms: i64,
    /// 本次回放窗口的结束时间戳（不包含），单位毫秒。
    pub end_time_ms: i64,
    /// 数据类型，区分逐笔委托和逐笔成交。
    pub data_kind: ReplayDataKind,
    /// 市场；对于 transaction，初始阶段可能是 `Unknown`，后续再结合数据解析。
    pub market: Market,
    /// 频道号。
    pub channel: i64,
    /// 该 source 在本次回放窗口内的起始消息号（包含）。
    pub begin_message_number: i64,
    /// 该 source 在本次回放窗口内的结束消息号（不包含）。
    pub end_message_number: i64,
    /// 实际查询使用的 ClickHouse 表名。
    pub table_name: String,
}

impl ChannelRange {
    pub fn new(
        day: impl Into<String>,
        start_time_ms: i64,
        end_time_ms: i64,
        data_kind: ReplayDataKind,
        market: Market,
        channel: i64,
        begin_message_number: i64,
        end_message_number: i64,
        table_name: impl Into<String>,
    ) -> Self {
        Self {
            day: day.into(),
            start_time_ms,
            end_time_ms,
            data_kind,
            market,
            channel,
            begin_message_number,
            end_message_number,
            table_name: table_name.into(),
        }
    }
}

/// source读取游标
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReaderCursor {
    /// 读取范围
    pub range: ChannelRange,
    /// 下一次该从哪里开始读取
    pub next_message_number: i64,
    /// 是否已经结束
    pub finished: bool,
}

impl ReaderCursor {
    pub fn new(range: ChannelRange) -> Self {
        let finished = range.begin_message_number >= range.end_message_number;
        let next_message_number = range.begin_message_number;

        Self {
            range,
            next_message_number,
            finished,
        }
    }

    pub fn remaining(&self) -> i64 {
        if self.finished {
            0
        } else {
            self.range
                .end_message_number
                .saturating_sub(self.next_message_number)
        }
    }

    pub fn current_batch_end(&self, batch_size: i64) -> i64 {
        self.next_message_number
            .saturating_add(batch_size)
            .min(self.range.end_message_number)
    }

    pub fn advance_to(&mut self, next_message_number: i64) {
        self.next_message_number = next_message_number.min(self.range.end_message_number);
        self.finished = self.next_message_number >= self.range.end_message_number;
    }
}

#[cfg(test)]
mod tests {
    use super::{ChannelRange, ReaderCursor, ReplayDataKind};
    use crate::common::Market;

    #[test]
    fn initializes_cursor_at_range_start() {
        let range = ChannelRange::new(
            "2026-05-12",
            1_000,
            2_000,
            ReplayDataKind::Order,
            Market::XSHG,
            3,
            100,
            120,
            "sh_table",
        );
        let cursor = ReaderCursor::new(range);

        assert_eq!(cursor.next_message_number, 100);
        assert!(!cursor.finished);
        assert_eq!(cursor.remaining(), 20);
    }

    #[test]
    fn advances_cursor_and_marks_finish() {
        let range = ChannelRange::new(
            "2026-05-12",
            1_000,
            2_000,
            ReplayDataKind::Order,
            Market::XSHE,
            7,
            10,
            15,
            "sz_table",
        );
        let mut cursor = ReaderCursor::new(range);

        assert_eq!(cursor.current_batch_end(3), 13);
        cursor.advance_to(13);
        assert_eq!(cursor.remaining(), 2);

        cursor.advance_to(15);
        assert!(cursor.finished);
        assert_eq!(cursor.remaining(), 0);
    }
}
