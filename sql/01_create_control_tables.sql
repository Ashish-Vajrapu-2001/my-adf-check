CREATE SCHEMA control;
GO

CREATE TABLE control.source_systems (
    source_system_id VARCHAR(20) PRIMARY KEY,
    source_system_name VARCHAR(100) NOT NULL,
    platform VARCHAR(50) NOT NULL,
    server_name VARCHAR(255),
    database_name VARCHAR(128),
    port INT DEFAULT 1433,
    auth_method VARCHAR(50) DEFAULT 'ManagedIdentity',
    keyvault_secret_name VARCHAR(200),
    is_active BIT DEFAULT 1,
    created_date DATETIME2 DEFAULT GETDATE()
);

CREATE TABLE control.table_metadata (
    table_id VARCHAR(20) PRIMARY KEY,
    source_system_id VARCHAR(20) NOT NULL,
    schema_name VARCHAR(50) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    entity_name VARCHAR(255),
    primary_key_columns VARCHAR(500) NOT NULL,        -- CRITICAL: "CUSTOMER_ID" or "CITY,STATE"
    is_composite_key BIT DEFAULT 0,                   -- CRITICAL: 0=single, 1=composite
    load_type VARCHAR(20) DEFAULT 'incremental',      -- 'initial', 'incremental'
    load_frequency VARCHAR(50),
    load_priority INT DEFAULT 5,
    depends_on_table_ids VARCHAR(500),                -- Comma-separated parent table_ids
    is_active BIT DEFAULT 1,
    initial_load_completed BIT DEFAULT 0,             -- CRITICAL: Tracks if initial done
    last_sync_version BIGINT,                         -- CRITICAL: For Change Tracking
    last_sync_timestamp DATETIME2,
    last_load_status VARCHAR(20),                     -- 'success', 'failed', 'running'
    last_load_start_time DATETIME2,
    last_load_end_time DATETIME2,
    last_load_rows_processed BIGINT,
    last_error_message NVARCHAR(MAX),
    retry_count INT DEFAULT 0,
    max_retries INT DEFAULT 3,
    created_date DATETIME2 DEFAULT GETDATE(),
    last_modified_date DATETIME2 DEFAULT GETDATE(),
    CONSTRAINT fk_table_source FOREIGN KEY (source_system_id)
        REFERENCES control.source_systems(source_system_id),
    CONSTRAINT uq_table UNIQUE (source_system_id, schema_name, table_name)
);

CREATE INDEX idx_tm_system ON control.table_metadata(source_system_id);
CREATE INDEX idx_tm_status ON control.table_metadata(last_load_status);
CREATE INDEX idx_tm_initial ON control.table_metadata(initial_load_completed);

CREATE TABLE control.load_dependencies (
    dependency_id INT IDENTITY(1,1) PRIMARY KEY,
    parent_table_id VARCHAR(20) NOT NULL,
    child_table_id VARCHAR(20) NOT NULL,
    dependency_type VARCHAR(20) DEFAULT 'REQUIRED',
    CONSTRAINT fk_dep_parent FOREIGN KEY (parent_table_id)
        REFERENCES control.table_metadata(table_id),
    CONSTRAINT fk_dep_child FOREIGN KEY (child_table_id)
        REFERENCES control.table_metadata(table_id),
    CONSTRAINT chk_no_self_dep CHECK (parent_table_id <> child_table_id)
);
