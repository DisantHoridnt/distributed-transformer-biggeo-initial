use std::io::Cursor;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::csv::{ReaderBuilder, WriterBuilder};
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use futures::TryStreamExt;

use crate::formats::{DataFormat, SchemaInference, BoxStream};
use crate::config::CsvConfig;

#[derive(Default)]
pub struct CsvFormat {
    pub has_header: bool,
    pub delimiter: u8,
    pub batch_size: usize,
}

impl CsvFormat {
    pub fn new(config: &CsvConfig) -> Self {
        Self {
            has_header: config.has_header,
            delimiter: config.delimiter as u8,
            batch_size: config.batch_size,
        }
    }
}

#[async_trait::async_trait]
impl SchemaInference for CsvFormat {
    async fn infer_schema(&self, data: &[u8]) -> Result<SchemaRef> {
        let cursor = Cursor::new(data);
        let (schema, _) = arrow::csv::reader::infer_reader_schema(
            cursor,
            self.delimiter,
            Some(1024),
            self.has_header,
        )?;
        Ok(Arc::new(schema))
    }
}

#[async_trait::async_trait]
impl DataFormat for CsvFormat {
    async fn read_batches_from_stream(
        &self,
        schema: SchemaRef,
        mut stream: BoxStream<'static, Result<Bytes>>,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let mut batches = Vec::new();
        
        while let Some(data) = stream.try_next().await? {
            let cursor = Cursor::new(data);
            let mut reader = ReaderBuilder::new(schema.clone())
                .has_header(self.has_header)
                .with_delimiter(self.delimiter)
                .with_batch_size(self.batch_size)
                .build(cursor)?;

            while let Some(batch) = reader.next() {
                batches.push(batch?);
            }
        }

        Ok(Box::pin(futures::stream::iter(batches.into_iter().map(Ok))))
    }

    async fn write_batches(
        &self,
        mut batches: BoxStream<'static, Result<RecordBatch>>,
    ) -> Result<Bytes> {
        let mut all_batches = Vec::new();
        while let Some(batch) = batches.try_next().await? {
            all_batches.push(batch);
        }

        if all_batches.is_empty() {
            return Ok(Bytes::new());
        }

        let schema = all_batches[0].schema();
        let mut buf = Vec::new();
        let mut writer = WriterBuilder::new()
            .has_headers(self.has_header)
            .with_delimiter(self.delimiter)
            .build(&mut buf);

        for batch in all_batches {
            writer.write(&batch)?;
        }
        drop(writer);

        Ok(Bytes::from(buf))
    }
}
