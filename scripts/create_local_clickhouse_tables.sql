CREATE TABLE IF NOT EXISTS default.SZOrder
(
    channel        Int64 COMMENT '频道',
    message_number Int64 COMMENT '序号',
    code           String COMMENT '委托代码，统一格式示例：001896.XSHE',
    price          Decimal(20, 4) COMMENT '委托价格，单位: 元',
    volume         Decimal(20, 4) COMMENT '委托量，单位: 股',
    direction      Int8 COMMENT '买卖标记 1: 未知方向, 2: 买, 3: 卖',
    order_type     String COMMENT '委托类型，深市原始类型值',
    time           DateTime64(3) COMMENT '委托时间',
    EventDate      Date MATERIALIZED toDate(time) COMMENT '事件日期'
)
ENGINE = MergeTree
PARTITION BY EventDate
ORDER BY (code, channel, message_number)
COMMENT '深圳逐笔委托本地统一表';

CREATE TABLE IF NOT EXISTS default.SHOrder
(
    channel        Int64 COMMENT '频道',
    message_number Int64 COMMENT '序号',
    code           String COMMENT '委托代码，统一格式示例：601899.XSHG',
    time           DateTime64(3) COMMENT '委托时间',
    price          Decimal(20, 4) COMMENT '委托价格，单位: 元',
    volume         Decimal(20, 4) COMMENT '委托量，单位: 股',
    direction      Int8 COMMENT '买卖标记 1: 未知方向, 2: 买, 3: 卖',
    order_type     Int8 COMMENT '委托类型，0: 增加委托单，1: 删除委托单',
    order_number   Int64 COMMENT '委托单号',
    EventDate      Date MATERIALIZED toDate(time) COMMENT '事件日期'
)
ENGINE = MergeTree
PARTITION BY EventDate
ORDER BY (code, channel, message_number)
COMMENT '上海逐笔委托本地统一表';

CREATE TABLE IF NOT EXISTS default.Transaction
(
    channel        Int64 COMMENT '频道',
    message_number Int64 COMMENT '序号',
    code           String COMMENT '成交代码，统一格式示例：601899.XSHG',
    time           DateTime64(3) COMMENT '成交时间',
    price          Decimal(20, 4) COMMENT '成交价格，单位: 元',
    volume         Decimal(20, 4) COMMENT '成交量，单位: 股',
    buy_number     Int64 COMMENT '买方委托编号',
    sell_number    Int64 COMMENT '卖方委托编号',
    deal_type      String COMMENT '成交类型',
    EventDate      Date MATERIALIZED toDate(time) COMMENT '事件日期'
)
ENGINE = MergeTree
PARTITION BY EventDate
ORDER BY (code, channel, message_number)
COMMENT '沪深合并逐笔成交本地统一表';
