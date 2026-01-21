use crate::TokioContext;
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_buffer::Buffer;
use arrow_ipc::reader::StreamDecoder;
use arrow_schema::{ArrowError, SchemaRef};
use clickhouse::query::BytesCursor;
use std::ops::ControlFlow;

pub(crate) struct ArrowStreamReader {
    tokio: TokioContext,
    state: ReaderState,
    schema: SchemaRef,
    first_batch: Option<RecordBatch>,
}

struct ReaderState {
    cursor: BytesCursor,
    decoder: StreamDecoder,
    buffer: Option<Buffer>,
}

impl ArrowStreamReader {
    pub(crate) async fn begin(
        tokio: TokioContext,
        cursor: BytesCursor,
    ) -> Result<Self, ArrowError> {
        // We need to read the schema message so that `RecordBatchReader::schema()` is infallible.
        let mut state = ReaderState {
            cursor,
            decoder: StreamDecoder::new(),
            buffer: None,
        };

        let mut first_batch = None;

        let schema = loop {
            if let Some(schema) = state.decoder.schema() {
                break schema;
            }

            if first_batch.is_some() {
                return Err(ArrowError::SchemaError(
                    "received first RecordBatch before receiving Schema".into(),
                ));
            }

            match state.try_read_batch().await? {
                ControlFlow::Break(Some(batch)) => {
                    first_batch = Some(batch);
                }
                ControlFlow::Break(None) => {
                    return Err(ArrowError::SchemaError(
                        "response stream ended before receiving Schema".into(),
                    ));
                }
                ControlFlow::Continue(()) => (),
            }
        };

        Ok(Self {
            tokio: tokio.clone(),
            state,
            schema,
            first_batch,
        })
    }
}

impl RecordBatchReader for ArrowStreamReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Iterator for ArrowStreamReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(first_batch) = self.first_batch.take() {
            return Some(Ok(first_batch));
        }

        self.tokio.block_on(self.state.next_batch()).transpose()
    }
}

impl ReaderState {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        loop {
            if let ControlFlow::Break(opt) = self.try_read_batch().await? {
                return Ok(opt);
            }
        }
    }

    async fn try_read_batch(&mut self) -> Result<ControlFlow<Option<RecordBatch>>, ArrowError> {
        while let Some(buffer) = self.buffer.as_mut().filter(|buf| !buf.is_empty()) {
            if let Some(batch) = self.decoder.decode(buffer)? {
                return Ok(ControlFlow::Break(Some(batch)));
            }
        }

        if let Some(buffer) = read_buffer(&mut self.cursor).await? {
            self.buffer = Some(buffer);
            return Ok(ControlFlow::Continue(()));
        }

        self.decoder.finish()?;
        Ok(ControlFlow::Break(None))
    }
}

async fn read_buffer(cursor: &mut BytesCursor) -> Result<Option<Buffer>, ArrowError> {
    cursor
        .next()
        .await
        .map(|buf| buf.map(Into::into))
        .map_err(|e| ArrowError::ExternalError(e.into()))
}
