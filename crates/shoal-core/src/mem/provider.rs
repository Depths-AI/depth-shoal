use crate::mem::table::TableHandle;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{MemTable, TableProvider, TableType};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct ShoalTableProvider {
    handle: TableHandle,
}

impl ShoalTableProvider {
    pub fn new(handle: TableHandle) -> Self {
        Self { handle }
    }
}

#[async_trait]
impl TableProvider for ShoalTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.handle.schema()
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
        // 1. Snapshot via ArcSwap (Wait-free)
        let batches = self.handle.snapshot();

        // 2. Construct partitions
        // Note: batches is Vec<RecordBatch>, partitions is Vec<Vec<RecordBatch>>
        let partitions = vec![batches];

        // 3. Delegate to DataFusion's MemTable
        let mem_table = MemTable::try_new(self.schema(), partitions)?;

        mem_table.scan(state, projection, filters, limit).await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }
}
