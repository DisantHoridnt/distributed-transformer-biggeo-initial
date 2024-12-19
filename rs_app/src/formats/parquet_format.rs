use anyhow::Result;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::SessionContext;
use parquet::arrow::ArrowWriter;
use std::sync::Arc;

use super::DataFormat;

#[derive(Debug, Clone)]
pub struct ParquetConfig {
    pub compression: Option<String>,
}

impl Default for ParquetConfig {
    fn default() -> Self {
        Self { compression: None }
    }
}

pub struct ParquetFormat {
    config: ParquetConfig,
}

impl ParquetFormat {
    pub fn new(config: ParquetConfig) -> Self {
        Self { config }
    }
}

impl Default for ParquetFormat {
    fn default() -> Self {
        Self {
            config: ParquetConfig::default(),
        }
    }
}

impl DataFormat for ParquetFormat {
    fn read(&self, data: &Bytes) -> Result<DataFrame> {
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(data.clone(), 1024)?;
        let mut batches = Vec::new();
        for result in reader {
            batches.push(result?);
        }
        
        let ctx = SessionContext::new();
        let df = if !batches.is_empty() {
            let df = ctx.read_batch(batches[0].clone())?;
            for batch in batches.into_iter().skip(1) {
                ctx.read_batch(batch)?;
            }
            df
        } else {
            let schema = Schema::empty();
            ctx.read_batch(RecordBatch::new_empty(Arc::new(schema)))?
        };
        Ok(df)
    }

    fn write(&self, df: &DataFrame) -> Result<Bytes> {
        let mut buf = Vec::new();
        let schema = Arc::new(Schema::try_from(df.schema())?);
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None)?;

        let batches = futures::executor::block_on(df.clone().collect())?;
        for batch in batches {
            writer.write(&batch)?;
        }
        writer.close()?;

        Ok(Bytes::from(buf))
    }

    fn write_batch(&self, batch: &RecordBatch) -> Result<Bytes> {
        let mut buf = Vec::new();
        let schema = batch.schema();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None)?;

        writer.write(batch)?;
        writer.close()?;

        Ok(Bytes::from(buf))
    }
}
