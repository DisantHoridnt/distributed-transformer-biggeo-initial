use std::io::Cursor;
use std::sync::Arc;
use anyhow::Result;
use arrow::csv::{Reader, ReaderBuilder};
use arrow::datatypes::{Schema, SchemaRef, DataType, Field};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};

use crate::formats::{DataFormat, SchemaInference};
use crate::config::{Config, CsvConfig};

#[derive(Clone)]
pub struct CsvFormat {
    config: Arc<CsvConfig>,
    schema: Option<SchemaRef>,
}

impl CsvFormat {
    pub fn new(config: &CsvConfig) -> Self {
        Self {
            config: Arc::new(config.clone()),
            schema: None,
        }
    }

    pub fn with_schema(schema: SchemaRef, config: &CsvConfig) -> Self {
        Self {
            config: Arc::new(config.clone()),
            schema: Some(schema),
        }
    }

    fn validate_schema(schema: &Schema, batch: &RecordBatch) -> Result<()> {
        if schema != batch.schema().as_ref() {
            return Err(anyhow::anyhow!(
                "Schema mismatch. Expected: {:?}, Got: {:?}",
                schema,
                batch.schema()
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl SchemaInference for CsvFormat {
    async fn infer_schema(&self, data: &[u8]) -> Result<SchemaRef> {
        if let Some(schema) = &self.schema {
            return Ok(schema.clone());
        }

        let mut reader = csv::Reader::from_reader(data);
        let mut sample_rows = Vec::new();
        let mut row_count = 0;

        // Skip header if needed
        if self.config.default_has_header {
            reader.headers()?;
        }

        // Sample rows for type inference
        while let Some(result) = reader.records().next() {
            let record = result?;
            sample_rows.push(record);
            row_count += 1;
            if row_count >= self.config.schema_sample_size {
                break;
            }
        }

        // Infer field types from samples
        let mut fields = Vec::new();
        if !sample_rows.is_empty() {
            let num_cols = sample_rows[0].len();
            for col in 0..num_cols {
                let field_name = if self.config.default_has_header {
                    reader.headers()?.get(col).unwrap_or(&format!("col_{}", col)).to_string()
                } else {
                    format!("col_{}", col)
                };

                // Infer type from samples
                let mut has_non_numeric = false;
                let mut has_decimal = false;
                for row in &sample_rows {
                    if let Some(value) = row.get(col) {
                        if !value.parse::<i64>().is_ok() {
                            if value.parse::<f64>().is_ok() {
                                has_decimal = true;
                            } else {
                                has_non_numeric = true;
                                break;
                            }
                        }
                    }
                }

                let data_type = if has_non_numeric {
                    DataType::Utf8
                } else if has_decimal {
                    DataType::Float64
                } else {
                    DataType::Int64
                };

                fields.push(Field::new(field_name, data_type, true));
            }
        }

        Ok(Arc::new(Schema::new(fields)))
    }
}

#[async_trait]
impl DataFormat for CsvFormat {
    async fn read_batches(&self, data: Bytes) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let schema = self.infer_schema(&data).await?;

        let cursor = Cursor::new(data);
        let reader = ReaderBuilder::new(schema.clone())
            .has_header(self.config.default_has_header)
            .with_delimiter(self.config.delimiter as u8)
            .with_quote(self.config.quote as u8)
            .with_batch_size(self.config.batch_size)
            .build(cursor)?;

        let schema_clone = schema.clone();
        let stream = stream::iter(reader.into_iter().map(move |batch_result| {
            let batch = batch_result?;
            Self::validate_schema(&schema_clone, &batch)?;
            Ok(batch)
        }));

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
            let mut writer = arrow::csv::Writer::new(&mut buf)
                .with_delimiter(self.config.delimiter as u8)
                .with_quote(self.config.quote as u8);

            if self.config.default_has_header {
                writer.write_schema(&schema)?;
            }

            for batch in batches {
                self.validate_schema(&schema, &batch)?;
                writer.write(&batch)?;
            }
        }

        Ok(Bytes::from(buf))
    }

    fn clone_box(&self) -> Box<dyn DataFormat + Send + Sync> {
        Box::new(self.clone())
    }
}
