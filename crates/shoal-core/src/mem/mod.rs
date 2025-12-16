pub mod provider;
pub mod table;

use crate::error::Result;
use crate::mem::provider::ShoalTableProvider;
use crate::mem::table::{IngestionWorker, TableHandle, TableState};
use crate::spec::{ShoalRuntimeConfig, ShoalSchema, ShoalTableConfig, ShoalTableRef};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::{SessionConfig, SessionContext};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;

/// The entry point for the Shoal in-memory runtime.
///
/// Manages the DataFusion SessionContext and table registration.
pub struct ShoalRuntime {
    ctx: SessionContext,
    config: ShoalRuntimeConfig,
}

impl ShoalRuntime {
    /// Create a new runtime with the given configuration.
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

    /// Create and register a new table in the runtime.
    ///
    /// Spawns a background `IngestionWorker` task for the table.
    /// Returns a `TableHandle` for async ingestion.
    pub fn create_table(
        &self,
        table_ref: ShoalTableRef,
        schema: ShoalSchema,
        table_config: ShoalTableConfig,
    ) -> Result<TableHandle> {
        // 1. Create the storage state
        let table_state = TableState::new(schema, table_config)?;
        let inner = Arc::new(RwLock::new(table_state));

        // 2. Create the MPSC channel and worker
        // Buffer size 1024 is arbitrary but reasonable for ingestion pressure
        let (tx, rx) = mpsc::channel(1024);
        let worker = IngestionWorker::new(rx, inner.clone());

        // 3. Spawn the worker task
        tokio::spawn(worker.run());

        // 4. Create the public handle (using crate-internal constructor)
        let handle = TableHandle::new(tx, inner.clone());

        // 5. Create the provider adapter (uses the same shared inner state)
        // We construct a temporary TableHandle for the provider, or strictly
        // speaking the provider just needs access to `snapshot`.
        // We'll wrap the handle in the provider for simplicity.
        let provider = ShoalTableProvider::new(handle.clone());

        // 6. Register with DataFusion
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

    /// Execute a SQL query against the registered tables.
    pub async fn sql(&self, query: &str) -> Result<Vec<RecordBatch>> {
        let df = self.ctx.sql(query).await?;
        let batches = df.collect().await?;
        Ok(batches)
    }
}
