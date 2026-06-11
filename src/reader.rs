use crate::TokioContext;
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use clickhouse_ext_arrow::ArrowCursor;

use clickhouse::error::Error as ChError;

pub(crate) struct ArrowStreamReader {
    tokio: TokioContext,
    cursor: ArrowCursor,
    schema: SchemaRef,
    first_batch: Option<RecordBatch>,
}

impl ArrowStreamReader {
    pub(crate) async fn begin(
        tokio: TokioContext,
        mut cursor: ArrowCursor,
    ) -> Result<Self, ArrowError> {
        // We need to read the schema message so that `RecordBatchReader::schema()` is infallible.
        // This entails reading up to, and storing, the first record batch if applicable.
        let first_batch = cursor.next().await.map_err(ch_error_to_arrow_error)?;

        let schema = cursor
            .schema()
            // Empty result, possibly because the query was DDL; don't error.
            // https://github.com/ClickHouse/adbc_clickhouse/issues/49
            .unwrap_or_else(|| Schema::empty().into());

        Ok(Self {
            tokio: tokio.clone(),
            cursor,
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

        self.tokio
            .block_on(self.cursor.next())
            .map_err(ch_error_to_arrow_error)
            .transpose()
    }
}

fn ch_error_to_arrow_error(e: ChError) -> ArrowError {
    if let ChError::Other(e) = e {
        return e
            .downcast::<ArrowError>()
            .map_or_else(ArrowError::ExternalError, |e| *e);
    }

    ArrowError::ExternalError(e.into())
}
