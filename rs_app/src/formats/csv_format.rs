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

#[derive(Clone)]
pub struct CsvFormat {
    schema: Option<SchemaRef>,
    has_header: bool,
    batch_size: usize,
    sample_size: usize,  // Number of rows to sample for type inference
}

impl CsvFormat {
    pub fn new(has_header: bool, batch_size: usize) -> Self {
        Self {
            schema: None,
            has_header,
            batch_size,
            sample_size: 1000,  // Default to sampling 1000 rows
        }
    }

    pub fn with_schema(schema: SchemaRef, has_header: bool, batch_size: usize) -> Self {
        Self {
            schema: Some(schema),
            has_header,
            batch_size,
            sample_size: 1000,
        }
    }

    fn infer_field_type(&self, values: &[String]) -> DataType {
        let mut has_int = true;
        let mut has_float = true;
        let mut all_empty = true;

        for value in values {
            if value.is_empty() {
                continue;
            }
            all_empty = false;

            // Try parsing as integer
            if has_int && value.parse::<i64>().is_err() {
                has_int = false;
            }

            // Try parsing as float
            if has_float && value.parse::<f64>().is_err() {
                has_float = false;
            }

            // If neither int nor float, must be string
            if !has_int && !has_float {
                break;
            }
        }

        if all_empty {
            DataType::Utf8
        } else if has_int {
            DataType::Int64
        } else if has_float {
            DataType::Float64
        } else {
            DataType::Utf8
        }
    }

    fn validate_schema(schema: &SchemaRef, batch: &RecordBatch) -> Result<()> {
        if schema.fields().len() != batch.num_columns() {
            return Err(anyhow::anyhow!("Schema mismatch: expected {} columns, got {}", schema.fields().len(), batch.num_columns()));
        }

        for (i, field) in schema.fields().iter().enumerate() {
            if field.data_type() != batch.column(i).data_type() {
                return Err(anyhow::anyhow!("Schema mismatch: expected column {} to be of type {:?}, got {:?}", i, field.data_type(), batch.column(i).data_type()));
            }
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

        let cursor = Cursor::new(data);
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(self.has_header)
            .flexible(true)
            .from_reader(cursor);

        let headers = if self.has_header {
            reader.headers()?.iter().map(|s| s.to_string()).collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        // Read sample rows for type inference
        let mut columns: Vec<Vec<String>> = Vec::new();
        let mut row_count = 0;
        let mut column_count = 0;

        for result in reader.records() {
            let record = result?;
            if columns.is_empty() {
                column_count = record.len();
                columns = vec![Vec::new(); column_count];
            }

            for (i, field) in record.iter().enumerate() {
                if i < column_count {
                    columns[i].push(field.to_string());
                }
            }

            row_count += 1;
            if row_count >= self.sample_size {
                break;
            }
        }

        // Infer types for each column
        let fields = columns.iter().enumerate().map(|(i, values)| {
            let name = if self.has_header && i < headers.len() {
                headers[i].clone()
            } else {
                format!("column_{}", i)
            };
            
            let data_type = self.infer_field_type(values);
            Field::new(name, data_type, true)
        }).collect();

        Ok(Arc::new(Schema::new(fields)))
    }
}

#[async_trait]
impl DataFormat for CsvFormat {
    async fn read_batches(&self, data: Bytes) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        // Infer schema if not provided
        let schema = self.infer_schema(&data).await?;

        let cursor = Cursor::new(data);
        let reader = ReaderBuilder::new(schema.clone())
            .has_header(self.has_header)
            .with_batch_size(self.batch_size)
            .build(cursor)?;

        // Create a stream that validates schema for each batch
        let schema_clone = schema.clone();
        let stream = stream::iter(reader.into_iter().map(move |batch_result| {
            let batch = batch_result?;
            // Validate schema
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
            let mut writer = arrow::csv::Writer::new(&mut buf);

            // Write header if needed
            if self.has_header {
                writer.write_schema(&schema)?;
            }

            // Write batches with schema validation
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
