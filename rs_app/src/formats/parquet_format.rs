use anyhow::Result;
use arrow::array::RecordBatchReader;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::io::Cursor;

use super::DataFormat;

#[derive(Debug, Clone)]
pub struct ParquetConfig {
    pub compression: Compression,
    pub batch_size: usize,
}

impl Default for ParquetConfig {
    fn default() -> Self {
        Self {
            compression: Compression::SNAPPY,
            batch_size: 1024,
        }
    }
}

pub struct ParquetFormat {
    config: ParquetConfig,
}

impl ParquetFormat {
    pub fn new(config: &ParquetConfig) -> Self {
        Self {
            config: config.clone(),
        }
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
    fn read_batch(&self, data: &Bytes) -> Result<RecordBatch> {
        let mut reader = ParquetRecordBatchReader::try_new(data.clone(), self.config.batch_size)?;
        let batch = reader
            .next()
            .ok_or_else(|| anyhow::anyhow!("No data in Parquet"))??;
        Ok(batch)
    }

    fn write_batch(&self, batch: &RecordBatch) -> Result<Bytes> {
        let mut cursor = Cursor::new(Vec::new());
        {
            let props = WriterProperties::builder()
                .set_compression(self.config.compression)
                .build();

            let mut writer = ArrowWriter::try_new(
                &mut cursor,
                batch.schema(),
                Some(props),
            )?;
            writer.write(batch)?;
            writer.close()?;
        }
        Ok(Bytes::from(cursor.into_inner()))
    }

    fn infer_schema(&self, data: &Bytes) -> Result<SchemaRef> {
        let reader = ParquetRecordBatchReader::try_new(data.clone(), self.config.batch_size)?;
        Ok(reader.schema().clone())
    }
}
