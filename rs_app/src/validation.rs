use crate::config::*;
use anyhow::{Result, anyhow};
use std::path::Path;

/// Validates the entire configuration
pub fn validate_config(config: &Config) -> Result<()> {
    validate_formats(&config.formats)?;
    validate_plugins(&config.plugins)?;
    validate_storage(&config.storage)?;
    validate_streaming(&config.streaming)?;
    validate_processing(&config.processing)?;
    Ok(())
}

/// Validates format-specific configurations
fn validate_formats(config: &FormatConfig) -> Result<()> {
    // CSV validation
    if config.csv.batch_size == 0 {
        return Err(anyhow!("CSV batch size cannot be zero"));
    }
    if config.csv.schema_sample_size == 0 {
        return Err(anyhow!("CSV schema sample size cannot be zero"));
    }
    if config.csv.max_sample_bytes == 0 {
        return Err(anyhow!("CSV max sample bytes cannot be zero"));
    }

    // Parquet validation
    if config.parquet.batch_size == 0 {
        return Err(anyhow!("Parquet batch size cannot be zero"));
    }
    if config.parquet.row_group_size == 0 {
        return Err(anyhow!("Parquet row group size cannot be zero"));
    }
    if config.parquet.page_size == 0 {
        return Err(anyhow!("Parquet page size cannot be zero"));
    }
    if config.parquet.dictionary_page_size == 0 {
        return Err(anyhow!("Parquet dictionary page size cannot be zero"));
    }
    
    match config.parquet.compression.as_str() {
        "snappy" | "gzip" | "brotli" | "lz4" | "zstd" | "none" => Ok(()),
        _ => Err(anyhow!("Invalid Parquet compression codec: {}", config.parquet.compression)),
    }?;

    // Default format validation
    if config.default.batch_size == 0 {
        return Err(anyhow!("Default batch size cannot be zero"));
    }
    if config.default.schema_sample_size == 0 {
        return Err(anyhow!("Default schema sample size cannot be zero"));
    }

    Ok(())
}

/// Validates plugin configuration
fn validate_plugins(config: &PluginConfig) -> Result<()> {
    if config.enable_plugins {
        if let Some(dir) = &config.plugin_dir {
            if !dir.exists() {
                return Err(anyhow!("Plugin directory does not exist: {:?}", dir));
            }
            if !dir.is_dir() {
                return Err(anyhow!("Plugin path is not a directory: {:?}", dir));
            }
        } else {
            return Err(anyhow!("Plugin directory must be specified when plugins are enabled"));
        }
    }

    if config.load_timeout_secs == 0 {
        return Err(anyhow!("Plugin load timeout cannot be zero"));
    }

    if config.enable_hot_reload && config.hot_reload_interval_secs == 0 {
        return Err(anyhow!("Hot reload interval cannot be zero when hot reload is enabled"));
    }

    Ok(())
}

/// Validates storage configuration
fn validate_storage(config: &StorageConfig) -> Result<()> {
    if config.read_buffer_size == 0 {
        return Err(anyhow!("Read buffer size cannot be zero"));
    }
    if config.write_buffer_size == 0 {
        return Err(anyhow!("Write buffer size cannot be zero"));
    }
    if config.max_concurrent_requests == 0 {
        return Err(anyhow!("Max concurrent requests cannot be zero"));
    }

    // Validate retry config
    if config.retry.initial_delay_ms == 0 {
        return Err(anyhow!("Initial retry delay cannot be zero"));
    }
    if config.retry.max_delay_ms < config.retry.initial_delay_ms {
        return Err(anyhow!("Max retry delay cannot be less than initial delay"));
    }
    if config.retry.backoff_multiplier <= 1.0 {
        return Err(anyhow!("Backoff multiplier must be greater than 1.0"));
    }

    Ok(())
}

/// Validates streaming configuration
fn validate_streaming(config: &StreamingConfig) -> Result<()> {
    if config.max_buffer_memory == 0 {
        return Err(anyhow!("Max buffer memory cannot be zero"));
    }
    if config.buffer_pool_size == 0 {
        return Err(anyhow!("Buffer pool size cannot be zero"));
    }
    if config.read_timeout_secs == 0 {
        return Err(anyhow!("Read timeout cannot be zero"));
    }
    if config.write_timeout_secs == 0 {
        return Err(anyhow!("Write timeout cannot be zero"));
    }
    if config.max_in_flight_batches == 0 {
        return Err(anyhow!("Max in-flight batches cannot be zero"));
    }

    Ok(())
}

/// Validates processing configuration
fn validate_processing(config: &ProcessingConfig) -> Result<()> {
    if config.max_memory_bytes == 0 {
        return Err(anyhow!("Max memory bytes cannot be zero"));
    }
    if config.parallel_threads == 0 {
        return Err(anyhow!("Parallel threads cannot be zero"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_config() {
        let config = Config::default();
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_invalid_batch_size() {
        let mut config = Config::default();
        config.formats.csv.batch_size = 0;
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn test_invalid_compression() {
        let mut config = Config::default();
        config.formats.parquet.compression = "invalid".to_string();
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn test_invalid_plugin_config() {
        let mut config = Config::default();
        config.plugins.enable_plugins = true;
        assert!(validate_config(&config).is_err()); // Should fail without plugin dir
    }

    #[test]
    fn test_invalid_retry_config() {
        let mut config = Config::default();
        config.storage.retry.max_delay_ms = 
            config.storage.retry.initial_delay_ms / 2;
        assert!(validate_config(&config).is_err());
    }
}
