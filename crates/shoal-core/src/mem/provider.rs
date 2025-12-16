use crate::mem::table::ShoalTable;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{MemTable, TableProvider, TableType};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use std::any::Any;
use std::sync::Arc;

/// A DataFusion TableProvider that wraps a ShoalTable.
///
/// This provider creates a new ephemeral `MemTable` for every scan, populated
/// by a snapshot of the underlying `ShoalTable`. This ensures queries see a
/// consistent view of the data (sealed tail + head snapshot) without blocking
/// writers for the duration of the query.
#[derive(Debug)]
pub struct ShoalTableProvider {
    table: ShoalTable,
}

impl ShoalTableProvider {
    pub fn new(table: ShoalTable) -> Self {
        Self { table }
    }
}

#[async_trait]
impl TableProvider for ShoalTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // 1. Snapshot the table state.
        // This takes a brief read lock (or write lock, depending on internal implementation)
        // to clone the sealed batch Arcs and finish_clone the head builders.
        // The lock is released immediately after this line.
        let (sealed, head) = self
            .table
            .snapshot()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        // 2. Construct partitions for the MemTable.
        // We treat the entire sealed tail + optional head as a single partition for now.
        // Optimization: We could split this into multiple partitions if parallelism is needed.
        let mut batch_vec = sealed;
        if let Some(h) = head {
            batch_vec.push(h);
        }
        let partitions = vec![batch_vec];

        // 3. Delegate to DataFusion's MemTable.
        // MemTable handles the actual physical plan generation (projection, filtering, etc.).
        let mem_table = MemTable::try_new(self.schema(), partitions)?;

        mem_table.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        // MemTable supports basic filtering, so we can signal 'Inexact' (DataFusion will apply filter)
        // or just let the default behavior handle it (which is often empty -> unsupported).
        // For simplicity and correctness with MemTable delegation, we return Unsupported
        // and let the MemTable's scan logic (which creates a MemoryExec) handle filtering if capable,
        // or let DataFusion add a FilterExec on top.
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }
}
