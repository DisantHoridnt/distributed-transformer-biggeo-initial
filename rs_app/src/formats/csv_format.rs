use std::io::Cursor;
use std::sync::Arc;
use anyhow::Result;
use arrow::csv::{Reader, ReaderBuilder};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream};

use crate::formats::DataFormat;

#[derive(Clone)]
pub struct CsvFormat {
    schema: Option<SchemaRef>,
    has_header: bool,
    batch_size: usize,
}

impl CsvFormat {
    pub fn new(has_header: bool, batch_size: usize) -> Self {
        Self {
            schema: None,
            has_header,
            batch_size,
        }
    }

    pub fn with_schema(schema: SchemaRef, has_header: bool, batch_size: usize) -> Self {
        Self {
            schema: Some(schema),
            has_header,
            batch_size,
        }
    }

    async fn infer_schema(&self, data: &[u8]) -> Result<SchemaRef> {
        let cursor = Cursor::new(data);
        let mut reader = csv::Reader::from_reader(cursor);
        let mut headers = Vec::new();
        let mut sample_record = Vec::new();

        // Read headers if present
        if self.has_header {
            headers = reader
                .headers()?
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>();
        }

        // Read first record for type inference
        if let Some(record) = reader.records().next() {
            sample_record = record?.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        }

        // Infer types from the sample record
        let mut fields = Vec::new();
        for (i, value) in sample_record.iter().enumerate() {
            let name = if self.has_header {
                headers.get(i).map(|s| s.as_str()).unwrap_or(&format!("column_{}", i))
            } else {
                &format!("column_{}", i)
            };

            // Simple type inference
            let data_type = if value.parse::<i64>().is_ok() {
                arrow::datatypes::DataType::Int64
            } else if value.parse::<f64>().is_ok() {
                arrow::datatypes::DataType::Float64
            } else {
                arrow::datatypes::DataType::Utf8
            };

            fields.push(arrow::datatypes::Field::new(name, data_type, true));
        }

        Ok(Arc::new(Schema::new(fields)))
    }
}

#[async_trait]
impl DataFormat for CsvFormat {
    async fn read_batches(&self, data: Bytes) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        // Infer schema if not provided
        let schema = match &self.schema {
            Some(s) => s.clone(),
            None => self.infer_schema(&data).await?,
        };

        let cursor = Cursor::new(data);
        let reader = ReaderBuilder::new(schema)
            .has_header(self.has_header)
            .with_batch_size(self.batch_size)
            .build(cursor)?;

        // Convert reader into a stream
        let stream = stream::iter(reader.into_iter().map(|batch| batch.map_err(|e| e.into())));
        Ok(Box::pin(stream))
    }

    async fn write_batches(&self, batches: BoxStream<'static, Result<RecordBatch>>) -> Result<Bytes> {
        let batches: Vec<RecordBatch> = batches.try_collect().await?;
        
        if batches.is_empty() {
            return Err(anyhow::anyhow!("No record batches to write"));
        }

        let schema = batches[0].schema();
        let mut buf = Vec::new();
        {
            let mut writer = arrow::csv::Writer::new(&mut buf);

            // Write header if needed
            if self.has_header {
                writer.write_schema(&schema)?;
            }

            // Write batches
            for batch in batches {
                writer.write(&batch)?;
            }
        }

        Ok(Bytes::from(buf))
    }

    fn clone_box(&self) -> Box<dyn DataFormat + Send + Sync> {
        Box::new(self.clone())
    }
}
