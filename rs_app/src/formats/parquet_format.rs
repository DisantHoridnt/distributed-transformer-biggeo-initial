use crate::formats::DataFormat;
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt};
use std::io::Cursor;
use futures::stream;

#[derive(Clone)]
pub struct ParquetFormat;

#[async_trait]
impl DataFormat for ParquetFormat {
    async fn read_batches(&self, data: Bytes) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        // Create a cursor from the input bytes
        let cursor = Cursor::new(data);
        
        // Create a parquet reader
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(cursor, 1024)?;
        
        // Convert the reader into a stream
        let stream = stream::iter(reader.map(|batch| batch.map_err(|e| e.into())));
        
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
