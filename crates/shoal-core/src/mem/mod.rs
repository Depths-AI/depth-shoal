pub mod provider;
pub mod table;

use crate::error::Result;
use crate::mem::provider::ShoalTableProvider;
use crate::mem::table::{IngestionWorker, TableHandle, TableSnapshot};
use crate::spec::{ShoalRuntimeConfig, ShoalSchema, ShoalTableConfig, ShoalTableRef};
use arc_swap::ArcSwap;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::prelude::{SessionConfig, SessionContext};
use std::sync::Arc;
use tokio::sync::mpsc;

/// The entry point for the Shoal in-memory runtime.
pub struct ShoalRuntime {
    ctx: SessionContext,
    config: ShoalRuntimeConfig,
}

impl ShoalRuntime {
    pub fn new(config: ShoalRuntimeConfig) -> Result<Self> {
        let session_config = SessionConfig::new()
            .with_default_catalog_and_schema(
                config.default_catalog.clone(),
                config.default_schema.clone(),
            )
            .with_information_schema(true);

        let ctx = SessionContext::new_with_config(session_config);

        Ok(Self { ctx, config })
    }

    pub fn create_table(
        &self,
        table_ref: ShoalTableRef,
        schema: ShoalSchema,
        table_config: ShoalTableConfig,
    ) -> Result<TableHandle> {
        let arrow_schema: SchemaRef = Arc::new((&schema).try_into()?);

        let initial_snapshot = TableSnapshot {
            schema: arrow_schema.clone(),
            batches: Vec::new(),
            bytes_estimate: 0,
        };
        let snapshot = Arc::new(ArcSwap::from_pointee(initial_snapshot));

        let (tx, rx) = mpsc::channel(1024);

        // Fix: Use '?' to handle the Result returned by IngestionWorker::new
        let worker = IngestionWorker::new(rx, arrow_schema, table_config, snapshot.clone())?;
        tokio::spawn(worker.run());

        let handle = TableHandle::new(tx, snapshot.clone());
        let provider = ShoalTableProvider::new(handle.clone());

        let catalog = if table_ref.catalog.as_str().is_empty() {
            &self.config.default_catalog
        } else {
            table_ref.catalog.as_str()
        };

        let schema_name = if table_ref.schema.as_str().is_empty() {
            &self.config.default_schema
        } else {
            table_ref.schema.as_str()
        };

        let table_reference = datafusion::sql::TableReference::Full {
            catalog: catalog.into(),
            schema: schema_name.into(),
            table: table_ref.table.as_str().into(),
        };

        self.ctx
            .register_table(table_reference, Arc::new(provider))?;

        Ok(handle)
    }

    pub async fn sql(&self, query: &str) -> Result<Vec<RecordBatch>> {
        let df = self.ctx.sql(query).await?;
        let batches = df.collect().await?;
        Ok(batches)
    }
}
