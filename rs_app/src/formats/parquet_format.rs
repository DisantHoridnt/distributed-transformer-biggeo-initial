use crate::formats::{DataFormat, SchemaInference, DataStream};
use anyhow::Result;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt, Stream};
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ArrowReader};
use parquet::file::reader::{SerializedFileReader, FileReader};
use pin_project_lite::pin_project;

pin_project! {
    struct StreamingParquetReader {
        #[pin]
        stream: BoxStream<'static, Result<Bytes>>,
        buffer: Vec<u8>,
        schema: SchemaRef,
        row_groups_read: usize,
        total_row_groups: usize,
        batch_size: usize,
    }
}

impl StreamingParquetReader {
    fn new(stream: BoxStream<'static, Result<Bytes>>, schema: SchemaRef, batch_size: usize) -> Self {
        Self {
            stream,
            buffer: Vec::new(),
            schema,
            row_groups_read: 0,
            total_row_groups: 0,
            batch_size,
        }
    }
}

impl Stream for StreamingParquetReader {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // Read from stream until we have enough data for a row group
        loop {
            if *this.row_groups_read >= *this.total_row_groups && *this.total_row_groups > 0 {
                return Poll::Ready(None);
            }

            match this.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(chunk))) => {
                    this.buffer.extend_from_slice(&chunk);
                    
                    // Try to read the file metadata if we haven't yet
                    if *this.total_row_groups == 0 {
                        if let Ok(reader) = SerializedFileReader::new(Cursor::new(&this.buffer)) {
                            *this.total_row_groups = reader.metadata().num_row_groups();
                        } else {
                            // Not enough data yet for metadata
                            continue;
                        }
                    }

                    // Try to read a row group
                    if let Ok(reader) = SerializedFileReader::new(Cursor::new(&this.buffer)) {
                        if let Ok(arrow_reader) = ParquetRecordBatchReader::try_new(
                            reader,
                            this.schema.clone(),
                            Some(*this.batch_size),
                            Some(&[*this.row_groups_read]),
                            None,
                        ) {
                            *this.row_groups_read += 1;
                            
                            // Return the first batch from this row group
                            if let Some(batch) = arrow_reader.into_iter().next() {
                                return Poll::Ready(Some(batch.map_err(|e| e.into())));
                            }
                        }
                    }
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    if this.buffer.is_empty() {
                        return Poll::Ready(None);
                    }
                    // Try one last time with remaining buffer
                    continue;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[derive(Clone)]
pub struct ParquetFormat {
    batch_size: usize,
}

impl Default for ParquetFormat {
    fn default() -> Self {
        Self {
            batch_size: 1024,
        }
    }
}

impl ParquetFormat {
    pub fn new(batch_size: usize) -> Self {
        Self { batch_size }
    }
}

#[async_trait]
impl SchemaInference for ParquetFormat {
    async fn infer_schema(&self, data: &[u8]) -> Result<SchemaRef> {
        let cursor = Cursor::new(data);
        let reader = SerializedFileReader::new(cursor)?;
        let arrow_reader = ParquetRecordBatchReader::try_new(reader, self.batch_size)?;
        Ok(arrow_reader.schema())
    }
}

#[async_trait]
impl DataFormat for ParquetFormat {
    async fn read_batches_from_stream(
        &self,
        schema: SchemaRef,
        stream: DataStream,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let reader = StreamingParquetReader::new(stream, schema, self.batch_size);
        Ok(Box::pin(reader))
    }

    async fn write_batches(&self, batches: BoxStream<'static, Result<RecordBatch>>) -> Result<Bytes> {
        // For now, we still buffer for writing
        // TODO: Implement streaming writer when Arrow Parquet supports it
        let batches: Vec<RecordBatch> = batches.try_collect().await?;
        
        if batches.is_empty() {
            return Err(anyhow::anyhow!("No record batches to write"));
        }
        
        let schema = batches[0].schema();
        let mut buf = Vec::new();
        
        {
            let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buf, schema.clone(), None)?;
            
            for batch in batches {
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
