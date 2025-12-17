use pyo3::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use tokio_memq_crate::mq::{
    MessageQueue, Publisher, Subscriber, TopicOptions, ConsumptionMode,
    PartitionRouting, TopicMessage,
};
use tokio_memq_crate::mq::traits::MessageSubscriber;
use tokio_memq_crate::mq::serializer::SerializationFormat;
use serde_json::Value;
use pythonize::{pythonize, depythonize};
use pyo3_async_runtimes::tokio::future_into_py;

#[pyclass(name = "TopicOptions")]
#[derive(Clone)]
/// Configuration options for a topic.
struct PyTopicOptions {
    #[pyo3(get, set)]
    /// Maximum number of messages to keep in the topic.
    max_messages: Option<usize>,
    #[pyo3(get, set)]
    /// Message time-to-live in milliseconds.
    message_ttl_ms: Option<u64>,
    #[pyo3(get, set)]
    /// Whether to enable LRU eviction when max_messages is reached.
    lru_enabled: bool,
    #[pyo3(get, set)]
    /// Number of partitions for the topic.
    partitions: Option<usize>,
}

#[pymethods]
impl PyTopicOptions {
    #[new]
    #[pyo3(signature = (max_messages=None, message_ttl_ms=None, lru_enabled=true, partitions=None))]
    /// Create new topic options.
    ///
    /// Args:
    ///     max_messages (int, optional): Max messages to keep.
    ///     message_ttl_ms (int, optional): Message TTL in ms.
    ///     lru_enabled (bool): Enable LRU eviction. Defaults to True.
    ///     partitions (int, optional): Number of partitions.
    fn new(max_messages: Option<usize>, message_ttl_ms: Option<u64>, lru_enabled: bool, partitions: Option<usize>) -> Self {
        PyTopicOptions {
            max_messages,
            message_ttl_ms,
            lru_enabled,
            partitions,
        }
    }

    fn __repr__(&self) -> String {
        format!("<TopicOptions max_messages={:?} ttl={:?} lru={} partitions={:?}>", 
            self.max_messages, self.message_ttl_ms, self.lru_enabled, self.partitions)
    }
}

impl From<PyTopicOptions> for TopicOptions {
    fn from(opts: PyTopicOptions) -> Self {
        TopicOptions {
            max_messages: opts.max_messages,
            message_ttl: opts.message_ttl_ms.map(Duration::from_millis),
            lru_enabled: opts.lru_enabled,
            idle_timeout: None,
            consume_idle_timeout: None,
            partitions: opts.partitions,
        }
    }
}

#[pyclass(name = "MessageQueue")]
/// Main entry point for the message queue system.
struct PyMessageQueue {
    inner: Arc<MessageQueue>,
}

#[pymethods]
impl PyMessageQueue {
    #[new]
    /// Create a new MessageQueue instance.
    fn new() -> Self {
        let rt = pyo3_async_runtimes::tokio::get_runtime();
        let _guard = rt.enter();
        PyMessageQueue {
            inner: Arc::new(MessageQueue::new()),
        }
    }

    /// Get a publisher for the specified topic.
    ///
    /// Args:
    ///     topic (str): The topic name.
    ///
    /// Returns:
    ///     Publisher: A publisher instance.
    fn publisher(&self, topic: String) -> PyPublisher {
        PyPublisher {
            inner: self.inner.publisher(topic),
            mq: self.inner.clone(),
        }
    }

    /// Get a publisher for the specified topic with a fixed key.
    ///
    /// Args:
    ///     topic (str): The topic name.
    ///     key (str): The fixed key to use for messages.
    ///
    /// Returns:
    ///     Publisher: A publisher instance.
    fn publisher_with_key(&self, topic: String, key: String) -> PyPublisher {
        PyPublisher {
            inner: self.inner.publisher_with_key(topic, key),
            mq: self.inner.clone(),
        }
    }

    /// Subscribe to a topic.
    ///
    /// Args:
    ///     topic (str): The topic name.
    ///
    /// Returns:
    ///     Subscriber: A subscriber instance (awaitable).
    fn subscriber<'py>(&self, py: Python<'py>, topic: String) -> PyResult<Bound<'py, PyAny>> {
        let mq = self.inner.clone();
        future_into_py(py, async move {
            let sub = mq.subscriber(topic).await.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Python::with_gil(|py| {
                let bound = Bound::new(py, PySubscriber { inner: Arc::new(sub) })?;
                Ok(bound.unbind())
            })
        })
    }

    /// Subscribe to a topic with custom options.
    ///
    /// Args:
    ///     topic (str): The topic name.
    ///     options (TopicOptions): Configuration options.
    ///
    /// Returns:
    ///     Subscriber: A subscriber instance (awaitable).
    fn subscriber_with_options<'py>(&self, py: Python<'py>, topic: String, options: &PyTopicOptions) -> PyResult<Bound<'py, PyAny>> {
        let mq = self.inner.clone();
        let opts: TopicOptions = options.clone().into();
        future_into_py(py, async move {
            let sub = mq.subscriber_with_options(topic, opts).await.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Python::with_gil(|py| {
                let bound = Bound::new(py, PySubscriber { inner: Arc::new(sub) })?;
                Ok(bound.unbind())
            })
        })
    }

    #[pyo3(signature = (topic, consumer_id, mode="LastOffset".to_string(), offset=None))]
    /// Join a consumer group.
    ///
    /// Args:
    ///     topic (str): Topic name.
    ///     consumer_id (str): Unique consumer ID.
    ///     mode (str): Consumption mode ("Earliest", "Latest", "LastOffset", "Offset").
    ///     offset (int, optional): Offset if mode is "Offset".
    ///
    /// Returns:
    ///     Subscriber: A subscriber instance (awaitable).
    fn subscriber_group<'py>(&self, py: Python<'py>, topic: String, consumer_id: String, mode: String, offset: Option<usize>) -> PyResult<Bound<'py, PyAny>> {
        let mq = self.inner.clone();
        let consumption_mode = match mode.as_str() {
            "Earliest" => ConsumptionMode::Earliest,
            "Latest" => ConsumptionMode::Latest,
            "LastOffset" => ConsumptionMode::LastOffset,
            "Offset" => ConsumptionMode::Offset(offset.unwrap_or(0)),
            _ => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid mode")),
        };
        
        future_into_py(py, async move {
            let sub = mq.subscriber_group(topic, consumer_id, consumption_mode).await.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Python::with_gil(|py| {
                let bound = Bound::new(py, PySubscriber { inner: Arc::new(sub) })?;
                Ok(bound.unbind())
            })
        })
    }

    #[pyo3(signature = (topic, partition_count, options=None))]
    /// Create a partitioned topic.
    ///
    /// Args:
    ///     topic (str): Topic name.
    ///     partition_count (int): Number of partitions.
    ///     options (TopicOptions, optional): Topic options.
    fn create_partitioned_topic<'py>(&self, py: Python<'py>, topic: String, partition_count: usize, options: Option<PyTopicOptions>) -> PyResult<Bound<'py, PyAny>> {
        let mq = self.inner.clone();
        let opts = match options {
            Some(o) => o.into(),
            None => TopicOptions::default(),
        };
        future_into_py(py, async move {
            mq.create_partitioned_topic(topic, opts, partition_count).await.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(())
        })
    }

    #[pyo3(signature = (topic, partition, consumer_id=None, mode="LastOffset".to_string(), offset=None))]
    /// Subscribe to a specific partition.
    ///
    /// Args:
    ///     topic (str): Topic name.
    ///     partition (int): Partition ID.
    ///     consumer_id (str, optional): Consumer ID.
    ///     mode (str): Consumption mode.
    ///     offset (int, optional): Offset.
    ///
    /// Returns:
    ///     Subscriber: A subscriber instance (awaitable).
    fn subscribe_partition<'py>(&self, py: Python<'py>, topic: String, partition: usize, consumer_id: Option<String>, mode: String, offset: Option<usize>) -> PyResult<Bound<'py, PyAny>> {
        let mq = self.inner.clone();
        let consumption_mode = match mode.as_str() {
            "Earliest" => ConsumptionMode::Earliest,
            "Latest" => ConsumptionMode::Latest,
            "LastOffset" => ConsumptionMode::LastOffset,
            "Offset" => ConsumptionMode::Offset(offset.unwrap_or(0)),
            _ => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid mode")),
        };
        
        future_into_py(py, async move {
            let sub = mq.subscribe_partition(topic, partition, consumer_id, consumption_mode).await.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Python::with_gil(|py| {
                let bound = Bound::new(py, PySubscriber { inner: Arc::new(sub) })?;
                Ok(bound.unbind())
            })
        })
    }

    /// Get statistics for a specific partition.
    ///
    /// Args:
    ///     topic (str): Topic name.
    ///     partition (int): Partition ID.
    ///
    /// Returns:
    ///     dict: Partition statistics (message_count, subscriber_count, etc.).
    fn get_partition_stats<'py>(&self, py: Python<'py>, topic: String, partition: usize) -> PyResult<Bound<'py, PyAny>> {
        let mq = self.inner.clone();
        future_into_py(py, async move {
            let stats = mq.get_partition_stats(topic, partition).await;
            Python::with_gil(|py| {
                match stats {
                    Some(s) => {
                        let dict = pyo3::types::PyDict::new(py);
                        dict.set_item("partition_id", s.partition_id)?;
                        dict.set_item("message_count", s.message_count)?;
                        dict.set_item("subscriber_count", s.subscriber_count)?;
                        dict.set_item("dropped_messages", s.dropped_messages)?;
                        let lags = pyo3::types::PyDict::new(py);
                        for (k, v) in s.consumer_lags {
                            lags.set_item(k, v)?;
                        }
                        dict.set_item("consumer_lags", lags)?;
                        Ok(dict.into_any().unbind())
                    },
                    None => Ok(py.None())
                }
            })
        })
    }

    /// Get statistics for all partitions of a topic.
    ///
    /// Args:
    ///     topic (str): Topic name.
    ///
    /// Returns:
    ///     list[dict]: List of partition statistics.
    fn get_all_partition_stats<'py>(&self, py: Python<'py>, topic: String) -> PyResult<Bound<'py, PyAny>> {
        let mq = self.inner.clone();
        future_into_py(py, async move {
            let all_stats = mq.get_all_partition_stats(topic).await;
            Python::with_gil(|py| {
                let list = pyo3::types::PyList::empty(py);
                for s in all_stats {
                     let dict = pyo3::types::PyDict::new(py);
                     dict.set_item("partition_id", s.partition_id)?;
                     dict.set_item("message_count", s.message_count)?;
                     dict.set_item("subscriber_count", s.subscriber_count)?;
                     dict.set_item("dropped_messages", s.dropped_messages)?;
                     let lags = pyo3::types::PyDict::new(py);
                     for (k, v) in s.consumer_lags {
                         lags.set_item(k, v)?;
                     }
                     dict.set_item("consumer_lags", lags)?;
                     list.append(dict)?;
                }
                Ok(list.into_any().unbind())
            })
        })
    }
    
    #[pyo3(signature = (topic, strategy, key=None, fixed_id=None))]
    /// Set the routing strategy for a partitioned topic.
    ///
    /// Args:
    ///     topic (str): Topic name.
    ///     strategy (str): Strategy name ("RoundRobin", "Random", "Hash", "Fixed").
    ///     key (str, optional): Key for Hash strategy.
    ///     fixed_id (int, optional): Partition ID for Fixed strategy.
    fn set_partition_routing<'py>(&self, py: Python<'py>, topic: String, strategy: String, key: Option<String>, fixed_id: Option<usize>) -> PyResult<Bound<'py, PyAny>> {
        let mq = self.inner.clone();
        let routing = match strategy.as_str() {
            "RoundRobin" => PartitionRouting::RoundRobin,
            "Random" => PartitionRouting::Random,
            "Hash" => {
                if let Some(k) = key {
                    PartitionRouting::Hash(k)
                } else {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Hash strategy requires a key"));
                }
            },
            "Fixed" => {
                if let Some(id) = fixed_id {
                    PartitionRouting::Fixed(id)
                } else {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Fixed strategy requires a fixed_id"));
                }
            },
            _ => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid strategy")),
        };

        future_into_py(py, async move {
            mq.set_partition_routing(topic, routing).await.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            Ok(())
        })
    }
    
    /// List all partitioned topics.
    ///
    /// Returns:
    ///     list[str]: List of topic names.
    fn list_partitioned_topics<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mq = self.inner.clone();
        future_into_py(py, async move {
            let topics = mq.list_partitioned_topics().await;
            Python::with_gil(|py| {
                Ok(topics.into_pyobject(py)?.unbind())
            })
        })
    }

    /// Delete a partitioned topic.
    ///
    /// Args:
    ///     topic (str): Topic name.
    ///
    /// Returns:
    ///     bool: True if deleted, False if not found.
    fn delete_partitioned_topic<'py>(&self, py: Python<'py>, topic: String) -> PyResult<Bound<'py, PyAny>> {
        let mq = self.inner.clone();
        future_into_py(py, async move {
            let result = mq.delete_partitioned_topic(&topic).await;
            Ok(result)
        })
    }
    
    /// Get statistics for a topic.
    ///
    /// Args:
    ///     topic (str): Topic name.
    ///
    /// Returns:
    ///     dict: Topic statistics or None if not found.
    fn get_topic_stats<'py>(&self, py: Python<'py>, topic: String) -> PyResult<Bound<'py, PyAny>> {
        let mq = self.inner.clone();
        future_into_py(py, async move {
            let stats = mq.get_topic_stats(topic).await;
            Python::with_gil(|py| {
                match stats {
                    Some(s) => {
                        let dict = pyo3::types::PyDict::new(py);
                        dict.set_item("message_count", s.message_count)?;
                        dict.set_item("subscriber_count", s.subscriber_count)?;
                        dict.set_item("dropped_messages", s.dropped_messages)?;
                        // consumer_lags
                        let lags = pyo3::types::PyDict::new(py);
                        for (k, v) in s.consumer_lags {
                            lags.set_item(k, v)?;
                        }
                        dict.set_item("consumer_lags", lags)?;
                        Ok(dict.into_any().unbind())
                    },
                    None => Ok(py.None())
                }
            })
        })
    }
}

#[pyclass(name = "Publisher")]
/// A publisher for a specific topic.
struct PyPublisher {
    inner: Publisher,
    mq: Arc<MessageQueue>,
}

#[pymethods]
impl PyPublisher {
    #[pyo3(signature = (data, key=None))]
    /// Publish a message to the topic.
    ///
    /// Args:
    ///     data (Any): The message data (will be serialized to JSON).
    ///     key (str, optional): Key for Hash routing.
    fn publish<'py>(&self, py: Python<'py>, data: Bound<'py, PyAny>, key: Option<String>) -> PyResult<Bound<'py, PyAny>> {
        let publisher = self.inner.clone();
        let mq = self.mq.clone();
        // Access topic name via MessagePublisher trait or direct method if available
        // Publisher implements MessagePublisher trait which has topic() method
        let topic = publisher.topic().to_string(); 
        
        // Convert PyAny to serde_json::Value
        let value: Value = depythonize(&data)?;
        
        future_into_py(py, async move {
            // Check if we should use partitioned publish
            // We construct a TopicMessage manually to pass to publish_to_partitioned
            let format = SerializationFormat::Json;
            // new_with_format is available on TopicMessage
            let mut message = TopicMessage::new_with_format(topic.clone(), &value, format.clone())
                 .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
            
            if let Some(k) = key {
                message.key = Some(k);
            }
            
            // Try publishing to partitioned topic
            let result = mq.publish_to_partitioned(message.clone()).await;
            
            match result {
                Ok(_) => Ok(()),
                Err(e) => {
                    let err_msg = e.to_string();
                    // Check if error indicates topic not found (fallback to normal topic)
                    if err_msg.contains("Partitioned topic does not exist") || err_msg.contains("分区主题不存在") {
                        // Fallback to normal publisher
                         publisher.publish_with_format(&value, SerializationFormat::Json).await
                            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
                    } else {
                        // Real error
                        Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err_msg))
                    }
                }
            }
        })
    }

    #[pyo3(signature = (data, key=None))]
    /// Alias for publish.
    fn send<'py>(&self, py: Python<'py>, data: Bound<'py, PyAny>, key: Option<String>) -> PyResult<Bound<'py, PyAny>> {
        self.publish(py, data, key)
    }

    fn __repr__(&self) -> String {
        format!("<Publisher topic='{}'>", self.inner.topic())
    }
}

#[pyclass(name = "Subscriber")]
/// A subscriber to a topic.
struct PySubscriber {
    inner: Arc<Subscriber>,
}

#[pymethods]
impl PySubscriber {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let sub = self.inner.clone();
        future_into_py(py, async move {
            match sub.recv().await {
                Ok(msg) => {
                    let value: Value = msg.deserialize().map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
                    Python::with_gil(|py| {
                        let bound = pythonize(py, &value)?;
                        Ok(bound.unbind())
                    })
                },
                Err(e) => Err(PyErr::new::<pyo3::exceptions::PyStopAsyncIteration, _>(e.to_string()))
            }
        })
    }

    fn __repr__(&self) -> String {
        let cid = self.inner.consumer_id.as_deref().unwrap_or("None");
        format!("<Subscriber topic='{}' consumer_id='{}'>", self.inner.topic_name, cid)
    }

    /// Receive the next message.
    ///
    /// Returns:
    ///     Any: The message data.
    fn recv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let sub = self.inner.clone();
        future_into_py(py, async move {
            let msg = sub.recv().await.map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
            // msg is TopicMessage.
            // deserialize it.
            // We expect JSON value.
            let value: Value = msg.deserialize().map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
            
            Python::with_gil(|py| {
                 let bound = pythonize(py, &value)?;
                 Ok(bound.unbind())
            })
        })
    }
    
    /// Try to receive a message without blocking.
    ///
    /// Returns:
    ///     Any or None: The message data if available, else None.
    fn try_recv<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let sub = self.inner.clone();
        future_into_py(py, async move {
            // Note: Subscriber::try_recv IS async in async-mq!
            let result = sub.try_recv().await;
            Python::with_gil(|py| {
                match result {
                    Ok(msg) => {
                        let value: Value = msg.deserialize().map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
                        let bound = pythonize(py, &value)?;
                        Ok(bound.unbind())
                    },
                    Err(_) => Ok(py.None())
                }
            })
        })
    }
    
    /// Get the current offset of the subscriber.
    ///
    /// Returns:
    ///     int: The current offset.
    fn current_offset<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let sub = self.inner.clone();
        future_into_py(py, async move {
            let offset = sub.current_offset().await;
            Ok(offset)
        })
    }

    /// Reset the subscriber's offset to the beginning.
    fn reset_offset<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let sub = self.inner.clone();
        future_into_py(py, async move {
            sub.reset_offset().await;
            Ok(())
        })
    }

    #[getter]
    /// Get the consumer ID (if part of a consumer group).
    fn consumer_id(&self) -> Option<String> {
        self.inner.consumer_id.clone()
    }

    #[getter]
    /// Get the topic name.
    fn topic(&self) -> String {
        self.inner.topic_name.clone()
    }
}

#[pymodule]
#[pyo3(name = "tokio_memq")]
fn init_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyMessageQueue>()?;
    m.add_class::<PyPublisher>()?;
    m.add_class::<PySubscriber>()?;
    m.add_class::<PyTopicOptions>()?;
    Ok(())
}
