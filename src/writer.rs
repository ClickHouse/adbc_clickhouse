use crate::TokioContext;
use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{ArrowError, Schema};
use clickhouse::insert_formatted::BufInsertFormatted;
use std::io::Write;
use std::num::Saturating;

// `clickhouse-ext-arrow` has an `ArrowInsert` type that accepts `RecordBatch`es,
// but by maintaining our own blocking writer, we can actually block when the buffer is full
// in order to synchronously flush it.
//
// This saves us from having to overcommit like the async `ArrowInsert` type does,
// since it has to write the full `RecordBatch` to the buffer synchronously.
// While it does try to opportunistically flush the buffer,
// this is not guaranteed since it cannot block.
pub struct ArrowStreamWriter<'a> {
    writer: StreamWriter<BlockingWriter<'a>>,
    rows_written: Saturating<u64>,
}

// We can't use `tokio_util::io::SyncIoBridge` because it won't drive a current-thread runtime
// https://github.com/ClickHouse/adbc_clickhouse/issues/25
struct BlockingWriter<'a> {
    tokio: &'a TokioContext,
    insert: BufInsertFormatted,
}

impl<'a> ArrowStreamWriter<'a> {
    pub fn begin(
        tokio: &'a TokioContext,
        schema: &Schema,
        insert: BufInsertFormatted,
    ) -> Result<Self, ArrowError> {
        // Writes the beginning of the stream
        let writer = StreamWriter::try_new(BlockingWriter { tokio, insert }, schema)?;

        Ok(Self {
            writer,
            rows_written: Saturating(0),
        })
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        self.writer.write(batch)?;
        self.rows_written += batch.num_rows() as u64;
        Ok(())
    }

    pub fn finish(self) -> Result<BufInsertFormatted, ArrowError> {
        let BlockingWriter { insert, .. } = self.writer.into_inner()?;

        tracing::record_all!(
            insert._priv_span(),
            clickhouse.request.sent_rows = self.rows_written.0
        );

        Ok(insert)
    }
}

// Not currently needed
// impl RecordBatchWriter for ArrowStreamWriter<'_> { ... }

impl Write for BlockingWriter<'_> {
    #[inline(always)]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.tokio
            .block_on(self.insert.write(buf))
            .map_err(Into::into)
    }

    #[inline(always)]
    fn flush(&mut self) -> std::io::Result<()> {
        self.tokio.block_on(self.insert.flush()).map_err(Into::into)
    }
}
