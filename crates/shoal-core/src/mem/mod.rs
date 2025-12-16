pub mod provider;
pub mod table;

use crate::error::Result;
use crate::mem::provider::ShoalTableProvider;
use crate::mem::table::ShoalTable;
use crate::spec::{ShoalRuntimeConfig, ShoalSchema, ShoalTableConfig, ShoalTableRef};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::{SessionConfig, SessionContext};
use std::sync::Arc;

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
    /// Returns a `ShoalTable` handle which can be used for direct ingestion.
    pub fn create_table(
        &self,
        table_ref: ShoalTableRef,
        schema: ShoalSchema,
        table_config: ShoalTableConfig,
    ) -> Result<ShoalTable> {
        // 1. Create the storage engine
        let table = ShoalTable::new(schema, table_config)?;

        // 2. Create the provider adapter
        let provider = ShoalTableProvider::new(table.clone());

        // 3. Register with DataFusion
        // Resolve catalog/schema (use defaults if empty)
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

        // Ensure schema exists (if not, create it in memory)
        // DataFusion SessionContext auto-creates the default catalog("datafusion"),
        // but we need to check nested structures if we use custom ones.
        // For simplicity, we register directly using the fully qualified reference helpers implies
        // we might need to construct the TableReference.

        let table_reference = datafusion::sql::TableReference::Full {
            catalog: catalog.into(),
            schema: schema_name.into(),
            table: table_ref.table.as_str().into(),
        };

        self.ctx
            .register_table(table_reference, Arc::new(provider))?;

        Ok(table)
    }

    /// Execute a SQL query against the registered tables.
    pub async fn sql(&self, query: &str) -> Result<Vec<RecordBatch>> {
        let df = self.ctx.sql(query).await?;
        let batches = df.collect().await?;
        Ok(batches)
    }
}
