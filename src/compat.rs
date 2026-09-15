//! Tiberius data-query compatibility over the bridge's existing connection.
//!
//! Use [`Client::query_compat`](crate::Client::query_compat) or
//! [`Client::simple_query_compat`](crate::Client::simple_query_compat) when
//! migrating code that consumes Tiberius `QueryStream` items. The bridge's
//! native buffered and row-only streaming APIs remain unchanged.

use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_core::Stream;

use crate::row::RowSchema;
use crate::{Column, Result, Row};

/// Metadata emitted before the rows of each result set.
#[derive(Debug, Clone)]
pub struct ResultMetadata {
    schema: Arc<RowSchema>,
    result_index: usize,
}

impl ResultMetadata {
    pub(crate) fn new(schema: Arc<RowSchema>, result_index: usize) -> Self {
        Self {
            schema,
            result_index,
        }
    }

    /// Columns in wire order for this result set.
    pub fn columns(&self) -> &[Column] {
        &self.schema.columns
    }

    /// Zero-based result-set index.
    pub fn result_index(&self) -> usize {
        self.result_index
    }
}

/// An item from a compatibility query stream.
#[derive(Debug)]
pub enum QueryItem {
    /// A row in the current result set.
    Row(Row),
    /// Metadata marking the start of a result set.
    Metadata(ResultMetadata),
}

impl QueryItem {
    /// Borrow this item as metadata, if it is metadata.
    pub fn as_metadata(&self) -> Option<&ResultMetadata> {
        match self {
            Self::Metadata(metadata) => Some(metadata),
            Self::Row(_) => None,
        }
    }

    /// Borrow this item as a row, if it is a row.
    pub fn as_row(&self) -> Option<&Row> {
        match self {
            Self::Row(row) => Some(row),
            Self::Metadata(_) => None,
        }
    }

    /// Consume this item as metadata, if it is metadata.
    pub fn into_metadata(self) -> Option<ResultMetadata> {
        match self {
            Self::Metadata(metadata) => Some(metadata),
            Self::Row(_) => None,
        }
    }

    /// Consume this item as a row, if it is a row.
    pub fn into_row(self) -> Option<Row> {
        match self {
            Self::Row(row) => Some(row),
            Self::Metadata(_) => None,
        }
    }
}

/// Incremental Tiberius-compatible metadata and row stream.
pub struct QueryStream<'a> {
    inner: Pin<Box<dyn Stream<Item = Result<QueryItem>> + Send + 'a>>,
    peeked: Option<Result<QueryItem>>,
    columns: Option<Arc<RowSchema>>,
}

impl<'a> QueryStream<'a> {
    pub(crate) fn new(inner: Pin<Box<dyn Stream<Item = Result<QueryItem>> + Send + 'a>>) -> Self {
        Self {
            inner,
            peeked: None,
            columns: None,
        }
    }

    /// Return columns for the current or next result set.
    pub async fn columns(&mut self) -> Result<Option<&[Column]>> {
        if self.columns.is_none() && self.peeked.is_none() {
            match poll_fn(|cx| self.inner.as_mut().poll_next(cx)).await {
                Some(Ok(item)) => {
                    if let QueryItem::Metadata(metadata) = &item {
                        self.columns = Some(Arc::clone(&metadata.schema));
                    }
                    self.peeked = Some(Ok(item));
                }
                Some(Err(error)) => return Err(error),
                None => {}
            }
        }

        Ok(self
            .columns
            .as_deref()
            .map(|schema| schema.columns.as_slice()))
    }

    /// Flatten all result sets into a row-only stream.
    pub fn into_row_stream(mut self) -> Pin<Box<dyn Stream<Item = Result<Row>> + Send + 'a>> {
        Box::pin(async_stream::try_stream! {
            while let Some(item) = poll_fn(|cx| Pin::new(&mut self).poll_next(cx)).await {
                if let QueryItem::Row(row) = item? {
                    yield row;
                }
            }
        })
    }

    /// Collect all result sets.
    ///
    /// Unlike Tiberius at the pinned compatibility commit, the bridge keeps
    /// empty middle and trailing result sets instead of dropping them.
    pub async fn into_results(mut self) -> Result<Vec<Vec<Row>>> {
        let mut results = Vec::new();
        while let Some(item) = poll_fn(|cx| Pin::new(&mut self).poll_next(cx)).await {
            match item? {
                QueryItem::Metadata(_) => results.push(Vec::new()),
                QueryItem::Row(row) => {
                    if results.is_empty() {
                        results.push(Vec::new());
                    }
                    if let Some(result) = results.last_mut() {
                        result.push(row);
                    }
                }
            }
        }
        Ok(results)
    }

    /// Collect and return the first result set, draining later results.
    pub async fn into_first_result(self) -> Result<Vec<Row>> {
        Ok(self
            .into_results()
            .await?
            .into_iter()
            .next()
            .unwrap_or_default())
    }

    /// Collect and return the first row of the first result set.
    pub async fn into_row(self) -> Result<Option<Row>> {
        Ok(self.into_first_result().await?.into_iter().next())
    }
}

impl Stream for QueryStream<'_> {
    type Item = Result<QueryItem>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = match self.peeked.take() {
            Some(item) => Poll::Ready(Some(item)),
            None => self.inner.as_mut().poll_next(cx),
        };
        if let Poll::Ready(Some(Ok(QueryItem::Metadata(metadata)))) = &item {
            self.columns = Some(Arc::clone(&metadata.schema));
        }
        item
    }
}
