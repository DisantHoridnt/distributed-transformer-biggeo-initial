use crate::formats::{DataFormat, SchemaInference, DataStream};
use crate::config::ParquetConfig;
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
use parquet::arrow::arrow_writer::{ArrowWriter, ArrowWriterProperties};
use parquet::file::reader::{SerializedFileReader, FileReader};
use parquet::file::properties::WriterProperties;
use pin_project_lite::pin_project;

pin_project! {
    struct StreamingParquetReader {
        #[pin]
        stream: BoxStream<'static, Result<Bytes>>,
        buffer: Vec<u8>,
        schema: SchemaRef,
        row_groups_read: usize,
        total_row_groups: usize,
        config: Arc<ParquetConfig>,
    }
}

impl StreamingParquetReader {
    fn new(stream: BoxStream<'static, Result<Bytes>>, schema: SchemaRef, config: Arc<ParquetConfig>) -> Self {
        Self {
            stream,
            buffer: Vec::new(),
            schema,
            row_groups_read: 0,
            total_row_groups: 0,
            config,
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
                            continue;
                        }
                    }

                    // Try to read a row group
                    if let Ok(reader) = SerializedFileReader::new(Cursor::new(&this.buffer)) {
                        if let Ok(arrow_reader) = ParquetRecordBatchReader::try_new(
                            reader,
                            this.schema.clone(),
                            Some(this.config.batch_size),
                            Some(&[*this.row_groups_read]),
                            None,
                        ) {
                            *this.row_groups_read += 1;
                            
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
                    continue;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[derive(Clone)]
pub struct ParquetFormat {
    config: Arc<ParquetConfig>,
}

impl ParquetFormat {
    pub fn new(config: &ParquetConfig) -> Self {
        Self {
            config: Arc::new(config.clone()),
        }
    }
}

#[async_trait]
impl SchemaInference for ParquetFormat {
    async fn infer_schema(&self, data: &[u8]) -> Result<SchemaRef> {
        let reader = SerializedFileReader::new(Cursor::new(data))?;
        let arrow_reader = ParquetRecordBatchReader::try_new(reader, self.config.batch_size)?;
        Ok(arrow_reader.schema())
    }
}

#[async_trait]
impl DataFormat for ParquetFormat {
    async fn read_batches(&self, data: Bytes) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let reader = SerializedFileReader::new(Cursor::new(data))?;
        let arrow_reader = ParquetRecordBatchReader::try_new(reader, self.config.batch_size)?;
        let schema = arrow_reader.schema();
        
        let stream = stream::iter(arrow_reader.map(|r| r.map_err(|e| e.into())));
        Ok(Box::pin(stream))
    }

    async fn read_batches_from_stream(
        &self,
        schema: SchemaRef,
        stream: DataStream,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let reader = StreamingParquetReader::new(stream, schema, self.config.clone());
        Ok(Box::pin(reader))
    }

    async fn write_batches(&self, batches: BoxStream<'static, Result<RecordBatch>>) -> Result<Bytes> {
        let batches: Vec<RecordBatch> = batches.try_collect().await?;
        
        if batches.is_empty() {
            return Err(anyhow::anyhow!("No record batches to write"));
        }
        
        let schema = batches[0].schema();
        let mut buf = Vec::new();
        
        let props = WriterProperties::builder()
            .set_compression(match self.config.compression.as_str() {
                "snappy" => parquet::basic::Compression::SNAPPY,
                "gzip" => parquet::basic::Compression::GZIP,
                "brotli" => parquet::basic::Compression::BROTLI,
                "lz4" => parquet::basic::Compression::LZ4,
                "zstd" => parquet::basic::Compression::ZSTD,
                _ => parquet::basic::Compression::UNCOMPRESSED,
            })
            .set_data_page_size_limit(self.config.page_size)
            .set_dictionary_page_size_limit(self.config.dictionary_page_size)
            .set_write_batch_size(self.config.batch_size)
            .build();

        let arrow_props = ArrowWriterProperties::builder()
            .set_max_row_group_size(self.config.row_group_size)
            .build();
        
        let mut writer = ArrowWriter::try_new(
            &mut buf,
            schema.clone(),
            Some(props),
            Some(arrow_props),
        )?;
        
        for batch in batches {
            writer.write(&batch)?;
        }
        
        writer.close()?;
        Ok(Bytes::from(buf))
    }
    
    fn clone_box(&self) -> Box<dyn DataFormat + Send + Sync> {
        Box::new(self.clone())
    }
}
