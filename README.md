# Mirro-Ex

Mirro-Ex 是一个正在开发中的沪深市场行情回放与模拟交易系统。

## 功能规划

- [x] 从 ClickHouse 读取沪深逐笔委托与成交数据
- [x] 按日期、时间、证券代码和速度进行行情回放
- [x] 暂停、恢复、停止回放并查询运行状态
- [x] 使用多 worker 重建不同证券的订单簿
- [x] 将订单簿快照导出为 Parquet 文件
- [x] 创建和查询模拟交易账户
- [x] 实现模拟限价委托、撤单和成交
- [x] 实现资金冻结、解冻与成交结算
- [x] 实现持仓冻结、解冻与成交更新
- [x] 提供订单和持仓查询接口
- [x] 提供 Vue Web UI，用于回放控制、行情展示、账户、下单、撤单、订单和持仓查看
- [ ] 通过 NATS 实时发布订单簿快照（当前仅完成连接与编码骨架，尚未在回放快照路径发布）
- [ ] 提供成交明细查询接口和前端成交列表
- [ ] 完善回放结果校验与性能测试

## 数据流

```text
ClickHouse 逐笔行情
        |
        v
行情读取与事件排序
        |
        v
模拟时钟与回放控制
        |
        v
多 worker 订单簿重建
        |
        +--------> Parquet 盘口快照
        |
        +--------> Web 行情状态 / SSE 通知
        |
        +--------> 模拟限价单队列与成交撮合
        |
        +--------> NATS 实时行情（待发布接入）
```

## 环境要求

- Rust 1.85 或更高版本
- ClickHouse
- NATS Server
- Python 3.9 或更高版本（运行辅助脚本时需要）

## L1 与回放 Snapshot 对比测试

这个测试用于把官方 L1 盘口 Parquet 和本系统回放导出的订单簿 snapshot Parquet 进行逐行对比。

1. 打开本地配置文件 `config/conf.toml`，确认 `[replay]` 中开启了 snapshot parquet 导出：

```toml
[replay]
write_snapshot_parquet = true
snapshot_parquet_dir = "data/order_book_snapshot"
```

2. 启动后端并执行一次回放，让系统生成 snapshot parquet：

```bash
cargo run
```

可以通过 Web UI 或 `scripts/replay_controller.py` 发起回放。回放结束后，默认会在下面的路径生成每个交易日、每个标的一个 snapshot 文件：

```text
data/order_book_snapshot/<交易日>/<证券代码>.parquet
```

例如：

```text
data/order_book_snapshot/2024-01-02/300274.XSHE.parquet
```

3. 准备同一交易日、同一证券代码的 L1 Parquet 文件。如果还没有 L1 文件，可以先导出：

```bash
python helpers/export_l1_parquet.py --date 2024-01-02
```

默认输出路径为：

```text
data/l1/<交易日>/<证券代码>.parquet
```

4. 执行对比脚本。脚本文件名是 `scripts/compare_l1_to_snapshots.py`，参数顺序是：L1 Parquet、回放 snapshot Parquet。

```bash
python scripts/compare_l1_to_snapshots.py \
  data/l1/2024-01-02/300274.XSHE.parquet \
  data/order_book_snapshot/2024-01-02/300274.XSHE.parquet \
  --mismatch-output data/compare/300274.XSHE_mismatch.csv
```

5. 查看终端输出中的关键指标：

```text
exact_match_rows=...
mismatch_rows=...
no_snapshot_in_window=...
match_rate=...
```

如果指定了 `--mismatch-output`，不一致的行会写入 CSV，便于继续排查具体的档位价格和数量差异。

项目采用 MIT License。
