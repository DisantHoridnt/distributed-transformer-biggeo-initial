use anyhow::Result;
use arrow::csv::{ReaderBuilder, WriterBuilder};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use csv;
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::SessionContext;
use std::io::Cursor;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct CsvConfig {
    pub has_header: bool,
    pub delimiter: u8,
}

impl Default for CsvConfig {
    fn default() -> Self {
        Self {
            has_header: true,
            delimiter: b',',
        }
    }
}

pub struct CsvFormat {
    config: CsvConfig,
}

impl Default for CsvFormat {
    fn default() -> Self {
        Self {
            config: CsvConfig::default(),
        }
    }
}

impl CsvFormat {
    pub fn new(config: CsvConfig) -> Self {
        Self { config }
    }

    fn infer_schema(&self, data: &Bytes) -> Result<Arc<Schema>> {
        let cursor = Cursor::new(data);
        let mut reader = csv::Reader::from_reader(cursor);
        let headers: Vec<String> = if self.config.has_header {
            reader.headers()?.iter().map(|s| s.to_string()).collect()
        } else {
            (0..reader.headers()?.len())
                .map(|i| format!("column_{}", i))
                .collect()
        };

        let fields: Vec<Field> = headers
            .into_iter()
            .map(|name| Field::new(name, arrow::datatypes::DataType::Utf8, true))
            .collect();

        Ok(Arc::new(Schema::new(fields)))
    }
}

impl super::DataFormat for CsvFormat {
    fn read(&self, data: &Bytes) -> Result<DataFrame> {
        let schema = self.infer_schema(data)?;
        let cursor = Cursor::new(data);
        let reader = ReaderBuilder::new(schema.clone())
            .has_header(self.config.has_header)
            .with_delimiter(self.config.delimiter)
            .build(cursor)?;

        let mut batches = Vec::new();
        for result in reader {
            batches.push(result?);
        }
        
        let ctx = SessionContext::new();
        let df = if !batches.is_empty() {
            let df = ctx.read_batch(batches[0].clone())?;
            for batch in batches.into_iter().skip(1) {
                ctx.read_batch(batch)?;
            }
            df
        } else {
            ctx.read_batch(RecordBatch::new_empty(schema))?
        };
        Ok(df)
    }

    fn write(&self, df: &DataFrame) -> Result<Bytes> {
        let mut buf = Vec::new();
        let mut writer = WriterBuilder::new()
            .has_headers(self.config.has_header)
            .with_delimiter(self.config.delimiter)
            .build(&mut buf);

        let batches = futures::executor::block_on(df.clone().collect())?;
        for batch in batches {
            writer.write(&batch)?;
        }
        drop(writer);

        Ok(Bytes::from(buf))
    }

    fn write_batch(&self, batch: &RecordBatch) -> Result<Bytes> {
        let mut buf = Vec::new();
        let mut writer = WriterBuilder::new()
            .has_headers(self.config.has_header)
            .with_delimiter(self.config.delimiter)
            .build(&mut buf);

        writer.write(batch)?;
        drop(writer);

        Ok(Bytes::from(buf))
    }
}
