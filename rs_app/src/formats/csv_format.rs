use anyhow::Result;
use arrow::csv::{ReaderBuilder, WriterBuilder};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use std::io::Cursor;
use std::sync::Arc;

use super::DataFormat;

#[derive(Debug, Clone)]
pub struct CsvConfig {
    pub has_header: bool,
    pub delimiter: u8,
    pub batch_size: usize,
}

impl Default for CsvConfig {
    fn default() -> Self {
        Self {
            has_header: true,
            delimiter: b',',
            batch_size: 1024,
        }
    }
}

pub struct CsvFormat {
    config: CsvConfig,
}

impl CsvFormat {
    pub fn new(config: &CsvConfig) -> Self {
        Self {
            config: config.clone(),
        }
    }
}

impl Default for CsvFormat {
    fn default() -> Self {
        Self {
            config: CsvConfig::default(),
        }
    }
}

impl DataFormat for CsvFormat {
    fn read_batch(&self, data: &Bytes) -> Result<RecordBatch> {
        let cursor = Cursor::new(data);
        let (schema, _) = arrow::csv::reader::infer_reader_schema(
            cursor,
            self.config.delimiter,
            Some(self.config.batch_size),
            self.config.has_header,
        )?;

        let cursor = Cursor::new(data);
        let mut reader = ReaderBuilder::new(Arc::new(schema))
            .has_header(self.config.has_header)
            .with_delimiter(self.config.delimiter)
            .with_batch_size(self.config.batch_size)
            .build(cursor)?;

        let batch = reader
            .next()
            .ok_or_else(|| anyhow::anyhow!("No data in CSV"))??;
        Ok(batch)
    }

    fn write_batch(&self, batch: &RecordBatch) -> Result<Bytes> {
        let mut cursor = Cursor::new(Vec::new());
        {
            let mut writer = WriterBuilder::new()
                .has_headers(self.config.has_header)
                .with_delimiter(self.config.delimiter)
                .build(&mut cursor);
            writer.write(batch)?;
        }
        Ok(Bytes::from(cursor.into_inner()))
    }

    fn infer_schema(&self, data: &Bytes) -> Result<SchemaRef> {
        let cursor = Cursor::new(data);
        let (schema, _) = arrow::csv::reader::infer_reader_schema(
            cursor,
            self.config.delimiter,
            Some(self.config.batch_size),
            self.config.has_header,
        )?;
        Ok(Arc::new(schema))
    }
}
