use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::matcher::order_book::{LevelSnapshot, OrderBookSnapshot};

const SNAPSHOT_DEPTH: usize = 10;
const DEFAULT_BATCH_SIZE: usize = 8192;

#[derive(Debug)]
struct SnapshotRow {
    ts: i64,
    code: String,
    asks: [Option<LevelValue>; SNAPSHOT_DEPTH],
    bids: [Option<LevelValue>; SNAPSHOT_DEPTH],
}

#[derive(Debug, Clone, Copy)]
struct LevelValue {
    price: f64,
    size: i64,
}

pub struct SnapshotParquetExporter {
    output_dir: PathBuf,
    schema: Arc<Schema>,
    writer: Option<ArrowWriter<File>>,
    rows: Vec<SnapshotRow>,
    batch_size: usize,
}

impl SnapshotParquetExporter {
    pub fn new(output_dir: impl AsRef<Path>) -> Self {
        Self {
            output_dir: output_dir.as_ref().to_path_buf(),
            schema: Arc::new(build_schema()),
            writer: None,
            rows: Vec::with_capacity(DEFAULT_BATCH_SIZE),
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    pub fn start_code_day(&mut self, day: &str, code: &str) -> Result<()> {
        self.close()?;
        let day_dir = self.output_dir.join(day);
        fs::create_dir_all(&day_dir).with_context(|| {
            format!(
                "failed to create snapshot parquet directory at {}",
                day_dir.display()
            )
        })?;

        let file_name = format!("{}.parquet", code.replace(['/', '\\'], "_"));
        let output_path = day_dir.join(file_name);
        let file = File::create(&output_path).with_context(|| {
            format!(
                "failed to create snapshot parquet file at {}",
                output_path.display()
            )
        })?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .build();
        let writer = ArrowWriter::try_new(file, self.schema.clone(), Some(props.into()))
            .context("failed to create snapshot parquet writer")?;

        self.writer = Some(writer);
        Ok(())
    }

    pub fn write_snapshot(
        &mut self,
        timestamp_ms: i64,
        code: &str,
        snapshot: &OrderBookSnapshot,
    ) -> Result<()> {
        if self.writer.is_none() {
            return Ok(());
        }

        self.rows.push(SnapshotRow {
            ts: timestamp_ms,
            code: code.to_string(),
            asks: levels_to_array(&snapshot.asks),
            bids: levels_to_array(&snapshot.bids),
        });

        if self.rows.len() >= self.batch_size {
            self.flush_batch()?;
        }

        Ok(())
    }

    pub fn close(&mut self) -> Result<()> {
        self.flush_batch()?;
        if let Some(writer) = self.writer.take() {
            writer
                .close()
                .context("failed to close snapshot parquet writer")?;
        }
        Ok(())
    }

    fn flush_batch(&mut self) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let Some(writer) = self.writer.as_mut() else {
            self.rows.clear();
            return Ok(());
        };

        let batch = rows_to_batch(self.schema.clone(), &self.rows)?;
        writer
            .write(&batch)
            .context("failed to write snapshot parquet batch")?;
        self.rows.clear();
        Ok(())
    }
}

fn build_schema() -> Schema {
    let mut fields = vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("code", DataType::Utf8, false),
    ];

    for index in 1..=SNAPSHOT_DEPTH {
        fields.push(Field::new(
            format!("ask{index}_price"),
            DataType::Float64,
            true,
        ));
        fields.push(Field::new(
            format!("ask{index}_size"),
            DataType::Int64,
            true,
        ));
        fields.push(Field::new(
            format!("bid{index}_price"),
            DataType::Float64,
            true,
        ));
        fields.push(Field::new(
            format!("bid{index}_size"),
            DataType::Int64,
            true,
        ));
    }

    Schema::new(fields)
}

fn levels_to_array(levels: &[LevelSnapshot]) -> [Option<LevelValue>; SNAPSHOT_DEPTH] {
    let mut result = [None; SNAPSHOT_DEPTH];
    for (index, level) in levels.iter().take(SNAPSHOT_DEPTH).enumerate() {
        result[index] = Some(LevelValue {
            price: level.price as f64 / 10000.0,
            size: level.total_qty,
        });
    }
    result
}

fn rows_to_batch(schema: Arc<Schema>, rows: &[SnapshotRow]) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.ts))),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.code.as_str()),
        )),
    ];

    for index in 0..SNAPSHOT_DEPTH {
        columns.push(Arc::new(Float64Array::from(
            rows.iter()
                .map(|row| row.asks[index].map(|level| level.price))
                .collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| row.asks[index].map(|level| level.size))
                .collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(Float64Array::from(
            rows.iter()
                .map(|row| row.bids[index].map(|level| level.price))
                .collect::<Vec<_>>(),
        )));
        columns.push(Arc::new(Int64Array::from(
            rows.iter()
                .map(|row| row.bids[index].map(|level| level.size))
                .collect::<Vec<_>>(),
        )));
    }

    RecordBatch::try_new(schema, columns).context("failed to build snapshot parquet record batch")
}
