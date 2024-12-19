use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use futures::TryStreamExt;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, BrotliLevel, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::formats::{DataFormat, SchemaInference, BoxStream};

#[derive(Default)]
pub struct ParquetFormat {
    pub compression: String,
    pub batch_size: usize,
}

#[async_trait::async_trait]
impl SchemaInference for ParquetFormat {
    async fn infer_schema(&self, data: &[u8]) -> Result<SchemaRef> {
        let bytes = Bytes::copy_from_slice(data);
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)?;
        Ok(reader.schema().clone())
    }
}

#[async_trait::async_trait]
impl DataFormat for ParquetFormat {
    async fn read_batches_from_stream(
        &self,
        _schema: SchemaRef,
        mut stream: BoxStream<'static, Result<Bytes>>,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let mut batches = Vec::new();
        
        while let Some(data) = stream.try_next().await? {
            let reader = ParquetRecordBatchReaderBuilder::try_new(data)?
                .with_batch_size(self.batch_size)
                .build()?;

            for batch in reader {
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

        let props = WriterProperties::builder()
            .set_compression(match self.compression.as_str() {
                "brotli" => Compression::BROTLI(BrotliLevel::default()),
                "gzip" => Compression::GZIP(GzipLevel::default()),
                "zstd" => Compression::ZSTD(ZstdLevel::default()),
                _ => Compression::UNCOMPRESSED,
            })
            .build();

        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props))?;

        for batch in all_batches {
            writer.write(&batch)?;
        }

        writer.close()?;
        Ok(Bytes::from(buf))
    }
}
