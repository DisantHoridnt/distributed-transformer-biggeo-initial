#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::env;
    use tokio;
    use tempfile::TempDir;
    use futures_util::TryStreamExt;

    async fn test_storage_operations(storage: Box<dyn Storage>) -> Result<()> {
        // Test put operation with nested paths
        let test_data = Bytes::from("Hello, World!");
        storage.put("nested/path/test.txt", test_data.clone()).await?;

        // Test get operation
        let retrieved_data = storage.get("nested/path/test.txt").await?;
        assert_eq!(retrieved_data, test_data);

        // Test list operation with no prefix
        let mut list_stream = storage.list(None).await?;
        let mut found = false;
        while let Some(path) = list_stream.try_next().await? {
            if path.contains("test.txt") {
                found = true;
                break;
            }
        }
        assert!(found, "test.txt should be found in listing");

        // Test list operation with prefix
        let mut list_stream = storage.list(Some("nested/")).await?;
        let mut found = false;
        while let Some(path) = list_stream.try_next().await? {
            if path.contains("test.txt") {
                found = true;
                break;
            }
        }
        assert!(found, "test.txt should be found with prefix");

        // Test non-existent file
        let result = storage.get("nonexistent.txt").await;
        assert!(result.is_err(), "Getting non-existent file should fail");

        // Test empty file
        let empty_data = Bytes::from("");
        storage.put("empty.txt", empty_data.clone()).await?;
        let retrieved_empty = storage.get("empty.txt").await?;
        assert_eq!(retrieved_empty, empty_data);

        // Test large file
        let large_data = Bytes::from("a".repeat(1024 * 1024)); // 1MB
        storage.put("large.txt", large_data.clone()).await?;
        let retrieved_large = storage.get("large.txt").await?;
        assert_eq!(retrieved_large, large_data);

        Ok(())
    }

    #[tokio::test]
    async fn test_local_storage() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = LocalStorage::new(temp_dir.path().to_string_lossy().to_string());
        test_storage_operations(Box::new(storage)).await
    }

    #[tokio::test]
    async fn test_local_storage_edge_cases() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = LocalStorage::new(temp_dir.path().to_string_lossy().to_string());
        
        // Test with absolute paths (should be handled relative to base)
        let data = Bytes::from("test");
        storage.put("/absolute/path.txt", data.clone()).await?;
        let retrieved = storage.get("/absolute/path.txt").await?;
        assert_eq!(retrieved, data);

        // Test with .. in path (should be normalized)
        let result = storage.put("../outside.txt", data.clone()).await;
        assert!(result.is_err(), "Should not allow writing outside base directory");

        Ok(())
    }

    #[tokio::test]
    #[ignore] // Run only when AWS credentials are available
    async fn test_s3_storage() -> Result<()> {
        // Ensure AWS credentials are set
        env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID must be set for test");
        env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_SECRET_ACCESS_KEY must be set for test");
        
        let test_bucket = env::var("TEST_S3_BUCKET").expect("TEST_S3_BUCKET must be set for test");
        let url = format!("s3://{}/test-prefix", test_bucket);
        let storage = from_url(&url).await?;
        test_storage_operations(storage).await
    }

    #[tokio::test]
    #[ignore] // Run only when Azure credentials are available
    async fn test_azure_storage() -> Result<()> {
        // Ensure Azure credentials are set
        env::var("AZURE_STORAGE_ACCOUNT").expect("AZURE_STORAGE_ACCOUNT must be set for test");
        env::var("AZURE_STORAGE_ACCESS_KEY").expect("AZURE_STORAGE_ACCESS_KEY must be set for test");
        
        let test_container = env::var("TEST_AZURE_CONTAINER")
            .expect("TEST_AZURE_CONTAINER must be set for test");
        let url = format!("azure://{}/test-prefix", test_container);
        let storage = from_url(&url).await?;
        test_storage_operations(storage).await
    }

    #[tokio::test]
    async fn test_url_parsing() {
        // Test invalid URL
        let result = from_url("invalid://test").await;
        assert!(result.is_err());

        // Test missing bucket/container
        let result = from_url("s3:///path").await;
        assert!(result.is_err());
        let result = from_url("azure:///path").await;
        assert!(result.is_err());

        // Test valid URLs
        let result = from_url("file:///tmp/test").await;
        assert!(result.is_ok());
        
        let result = from_url("s3://bucket/path").await;
        assert!(result.is_err()); // Should fail due to missing credentials

        let result = from_url("azure://container/path").await;
        assert!(result.is_err()); // Should fail due to missing credentials
    }
}
