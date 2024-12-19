use std::sync::Arc;
use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use tokio::sync::Semaphore;

/// A pool of reusable buffers for streaming operations
pub struct BufferPool {
    /// Available buffers
    buffers: Arc<Mutex<Vec<BytesMut>>>,
    /// Semaphore to control buffer allocation
    semaphore: Arc<Semaphore>,
    /// Size of each buffer
    buffer_size: usize,
    /// Maximum memory allowed for all buffers
    max_memory: usize,
}

impl BufferPool {
    /// Create a new buffer pool
    pub fn new(config: &crate::config::StreamingConfig) -> Self {
        let buffer_size = config.max_buffer_memory / config.buffer_pool_size;
        let buffers = (0..config.buffer_pool_size)
            .map(|_| BytesMut::with_capacity(buffer_size))
            .collect();

        Self {
            buffers: Arc::new(Mutex::new(buffers)),
            semaphore: Arc::new(Semaphore::new(config.buffer_pool_size)),
            buffer_size,
            max_memory: config.max_buffer_memory,
        }
    }

    /// Acquire a buffer from the pool
    pub async fn acquire(&self) -> BufferGuard {
        let _permit = self.semaphore.acquire().await.unwrap();
        let buffer = self.buffers.lock().pop().unwrap_or_else(|| {
            BytesMut::with_capacity(self.buffer_size)
        });

        BufferGuard {
            buffer: Some(buffer),
            pool: Arc::new(self.clone()),
        }
    }

    /// Return a buffer to the pool
    fn return_buffer(&self, mut buffer: BytesMut) {
        buffer.clear();
        self.buffers.lock().push(buffer);
    }

    /// Get the buffer size
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Get the maximum memory limit
    pub fn max_memory(&self) -> usize {
        self.max_memory
    }
}

impl Clone for BufferPool {
    fn clone(&self) -> Self {
        Self {
            buffers: Arc::clone(&self.buffers),
            semaphore: Arc::clone(&self.semaphore),
            buffer_size: self.buffer_size,
            max_memory: self.max_memory,
        }
    }
}

/// RAII guard for a buffer from the pool
pub struct BufferGuard {
    buffer: Option<BytesMut>,
    pool: Arc<BufferPool>,
}

impl BufferGuard {
    /// Get a reference to the underlying buffer
    pub fn get_ref(&self) -> &BytesMut {
        self.buffer.as_ref().unwrap()
    }

    /// Get a mutable reference to the underlying buffer
    pub fn get_mut(&mut self) -> &mut BytesMut {
        self.buffer.as_mut().unwrap()
    }

    /// Convert the buffer into Bytes, consuming the guard
    pub fn into_bytes(mut self) -> Bytes {
        self.buffer.take().unwrap().freeze()
    }
}

impl Drop for BufferGuard {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.pool.return_buffer(buffer);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StreamingConfig;

    #[tokio::test]
    async fn test_buffer_pool() {
        let config = StreamingConfig {
            max_buffer_memory: 1024 * 1024,
            buffer_pool_size: 4,
            read_timeout_secs: 30,
            write_timeout_secs: 30,
            enable_backpressure: true,
            max_in_flight_batches: 2,
        };

        let pool = BufferPool::new(&config);
        
        // Test acquiring buffers
        let mut guards = Vec::new();
        for _ in 0..4 {
            guards.push(pool.acquire().await);
        }

        // Verify buffer size
        assert_eq!(pool.buffer_size(), 262144); // 1MB / 4

        // Write to buffers
        for guard in &mut guards {
            guard.get_mut().extend_from_slice(b"test");
        }

        // Release buffers
        drop(guards);

        // Verify we can acquire again
        let guard = pool.acquire().await;
        assert_eq!(guard.get_ref().capacity(), 262144);
    }
}
