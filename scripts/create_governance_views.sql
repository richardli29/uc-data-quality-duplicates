-- =============================================================================
-- Governance Materialized Views
-- =============================================================================
-- Creates materialized views that mirror system.information_schema privilege
-- tables into a `governance` schema within your catalog.
--
-- WHY: The app's service principal needs to read who has access to each table.
--      system.information_schema requires metastore admin to grant access.
--      Materialized views use DEFINER rights — they run with the creator's
--      permissions, so the SP only needs SELECT on the governance schema.
--
-- REQUIREMENTS:
--   - Run this as a user who has SELECT on system.information_schema
--     (typically any workspace user or metastore admin)
--   - A running SQL warehouse
--
-- USAGE:
--   1. Open in a Databricks SQL editor
--   2. Set the widget to your catalog name
--   3. Run all statements
--
-- After creation, grant the app SP access:
--   GRANT USE SCHEMA ON SCHEMA <catalog>.governance TO `<SP_ID>`;
--   GRANT SELECT ON SCHEMA <catalog>.governance TO `<SP_ID>`;
-- =============================================================================

CREATE WIDGET TEXT catalog DEFAULT 'main';

CREATE SCHEMA IF NOT EXISTS ${catalog}.governance;

COMMENT ON SCHEMA ${catalog}.governance IS
  'Materialized views mirroring system.information_schema privilege tables. Used by the UC Data Duplicates app to read permissions without MANAGE privilege.';

-- Catalog-level grants (who can USE CATALOG, etc.)
CREATE OR REPLACE MATERIALIZED VIEW ${catalog}.governance.catalog_privileges
AS SELECT * FROM system.information_schema.catalog_privileges;

-- Schema-level grants (USE SCHEMA, SELECT, ALL PRIVILEGES, etc.)
CREATE OR REPLACE MATERIALIZED VIEW ${catalog}.governance.schema_privileges
AS SELECT * FROM system.information_schema.schema_privileges;

-- Table-level grants (SELECT, MODIFY, etc.)
CREATE OR REPLACE MATERIALIZED VIEW ${catalog}.governance.table_privileges
AS SELECT * FROM system.information_schema.table_privileges;
