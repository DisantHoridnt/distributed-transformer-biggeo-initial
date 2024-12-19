use crate::formats::{DataFormat, SchemaInference};
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt};
use std::io::Cursor;
use futures::stream;

#[derive(Clone)]
pub struct ParquetFormat;

#[async_trait]
impl SchemaInference for ParquetFormat {
    async fn infer_schema(&self, data: &[u8]) -> Result<SchemaRef> {
        let cursor = Cursor::new(data);
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(cursor, 1024)?;
        Ok(reader.schema())
    }
}

#[async_trait]
impl DataFormat for ParquetFormat {
    async fn read_batches(&self, data: Bytes) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        // First infer/validate schema
        let schema = self.infer_schema(&data).await?;
        
        // Create a cursor from the input bytes
        let cursor = Cursor::new(data);
        
        // Create a parquet reader
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(cursor, 1024)?;
        
        // Validate schema matches
        if reader.schema() != schema {
            return Err(anyhow::anyhow!("Schema changed during file read"));
        }
        
        // Convert the reader into a stream with schema validation
        let schema_clone = schema.clone();
        let stream = stream::iter(reader.map(move |batch| {
            let batch = batch?;
            // Validate each batch
            Self::validate_schema(&schema_clone, &batch)?;
            Ok(batch)
        }));
        
        Ok(Box::pin(stream))
    }

    async fn write_batches(&self, batches: BoxStream<'static, Result<RecordBatch>>) -> Result<Bytes> {
        // Collect all batches (temporarily, until we implement streaming writes)
        let batches: Vec<RecordBatch> = batches.try_collect().await?;
        
        if batches.is_empty() {
            return Err(anyhow::anyhow!("No record batches to write"));
        }
        
        let schema = batches[0].schema();
        let mut buf = Vec::new();
        
        {
            let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buf, schema, None)?;
            
            for batch in batches {
                // Validate schema consistency
                self.validate_schema(&schema, &batch)?;
                writer.write(&batch)?;
            }
            
            writer.close()?;
        }
        
        Ok(Bytes::from(buf))
    }
    
    fn clone_box(&self) -> Box<dyn DataFormat + Send + Sync> {
        Box::new(self.clone())
    }
}
