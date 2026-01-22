use crate::TokioContext;
use arrow_array::RecordBatch;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{ArrowError, Schema};
use clickhouse::insert_formatted::{BufInsertFormatted, InsertFormatted};
use std::io::Write;

pub struct ArrowStreamWriter<'a> {
    writer: StreamWriter<BlockingWriter<'a>>,
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
        insert: InsertFormatted,
    ) -> Result<Self, ArrowError> {
        // Writes the beginning of the stream
        let writer = StreamWriter::try_new(
            BlockingWriter {
                tokio,
                insert: insert.buffered(),
            },
            schema,
        )?;

        Ok(Self { writer })
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        self.writer.write(batch)
    }

    pub fn finish(self) -> Result<BufInsertFormatted, ArrowError> {
        let BlockingWriter { insert, .. } = self.writer.into_inner()?;
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
