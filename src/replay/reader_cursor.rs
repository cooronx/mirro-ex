use crate::common::Market;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelRange {
    pub day: String,
    pub market: Market,
    pub channel: i64,
    pub begin_message_number: i64,
    pub end_message_number: i64,
}

impl ChannelRange {
    pub fn new(
        day: impl Into<String>,
        market: Market,
        channel: i64,
        begin_message_number: i64,
        end_message_number: i64,
    ) -> Self {
        Self {
            day: day.into(),
            market,
            channel,
            begin_message_number,
            end_message_number,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReaderCursor {
    pub range: ChannelRange,
    pub next_message_number: i64,
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
    use super::{ChannelRange, ReaderCursor};
    use crate::common::Market;

    #[test]
    fn initializes_cursor_at_range_start() {
        let range = ChannelRange::new("2026-05-12", Market::XSHG, 3, 100, 120);
        let cursor = ReaderCursor::new(range);

        assert_eq!(cursor.next_message_number, 100);
        assert!(!cursor.finished);
        assert_eq!(cursor.remaining(), 20);
    }

    #[test]
    fn advances_cursor_and_marks_finish() {
        let range = ChannelRange::new("2026-05-12", Market::XSHE, 7, 10, 15);
        let mut cursor = ReaderCursor::new(range);

        assert_eq!(cursor.current_batch_end(3), 13);
        cursor.advance_to(13);
        assert_eq!(cursor.remaining(), 2);

        cursor.advance_to(15);
        assert!(cursor.finished);
        assert_eq!(cursor.remaining(), 0);
    }
}
