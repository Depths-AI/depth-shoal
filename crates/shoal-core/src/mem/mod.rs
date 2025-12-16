pub mod provider;
pub mod table;

use crate::error::Result;
use crate::mem::provider::ShoalTableProvider;
use crate::mem::table::{IngestionWorker, SharedTableState, TableHandle};
use crate::spec::{ShoalRuntimeConfig, ShoalSchema, ShoalTableConfig, ShoalTableRef};
use arrow::datatypes::SchemaRef;
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
        // 1. Create the Arrow schema
        let arrow_schema: SchemaRef = Arc::new((&schema).try_into()?);

        // 2. Create the Shared Read State
        let shared_state = SharedTableState::new(arrow_schema.clone(), table_config.clone());
        let shared = Arc::new(RwLock::new(shared_state));

        // 3. Create the MPSC channel
        let (tx, rx) = mpsc::channel(1024);

        // 4. Create and Spawn Worker (owns Write State)
        let worker = IngestionWorker::new(rx, arrow_schema, table_config, shared.clone());
        tokio::spawn(worker.run());

        // 5. Create Handle
        let handle = TableHandle::new(tx, shared.clone());

        // 6. Create Provider (uses Handle)
        let provider = ShoalTableProvider::new(handle.clone());

        // 7. Register with DataFusion
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
