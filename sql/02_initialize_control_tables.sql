-- 1. Populate Source Systems
INSERT INTO control.source_systems (source_system_id, source_system_name, platform, database_name) VALUES
('SRC-001', 'Azure SQL ERP', 'Azure SQL Database', 'ERP_DB'),
('SRC-002', 'Azure SQL CRM', 'Azure SQL Database', 'CRM_DB'),
('SRC-003', 'Azure SQL Marketing', 'Azure SQL Database', 'MKT_DB');

-- 2. Populate Table Metadata
INSERT INTO control.table_metadata
(table_id, source_system_id, schema_name, table_name, entity_name, primary_key_columns, is_composite_key, load_type, load_frequency, load_priority, depends_on_table_ids)
VALUES
-- CRM System
('ENT-001', 'SRC-002', 'CRM', 'Customers', 'Customer Master', 'CUSTOMER_ID', 0, 'incremental', 'Real-time (CDC)', 10, NULL),
('ENT-002', 'SRC-002', 'CRM', 'CustomerRegistrationSource', 'Acquisition Attribution', 'REGISTRATION_SOURCE_ID', 0, 'incremental', 'One-time', 20, 'ENT-001,ENT-010'),
('ENT-011', 'SRC-002', 'CRM', 'INCIDENTS', 'Support Incident', 'INCIDENT_ID', 0, 'incremental', 'Real-time (CDC)', 30, 'ENT-001,ENT-003'),
('ENT-012', 'SRC-002', 'CRM', 'INTERACTIONS', 'Customer Interaction', 'INTERACTION_ID', 0, 'incremental', 'Real-time (CDC)', 40, 'ENT-001,ENT-011'),
('ENT-013', 'SRC-002', 'CRM', 'SURVEYS', 'Customer Survey', 'SURVEY_ID', 0, 'incremental', 'Real-time', 40, 'ENT-001,ENT-003,ENT-011'),

-- ERP System
('ENT-006', 'SRC-001', 'ERP', 'CITY_TIER_MASTER', 'City Tier Classification', 'CITY,STATE', 1, 'incremental', 'Quarterly', 5, NULL),
('ENT-008', 'SRC-001', 'ERP', 'CATEGORIES', 'Product Category', 'CATEGORY_ID', 0, 'incremental', 'Batch', 5, NULL),
('ENT-009', 'SRC-001', 'ERP', 'BRANDS', 'Brand Master', 'BRAND_ID', 0, 'incremental', 'Batch', 5, NULL),
('ENT-005', 'SRC-001', 'ERP', 'ADDRESSES', 'Customer Address', 'ADDRESS_ID', 0, 'incremental', 'Event-driven', 20, 'ENT-001,ENT-006'),
('ENT-007', 'SRC-001', 'ERP', 'MTL_SYSTEM_ITEMS_B', 'Product Master', 'INVENTORY_ITEM_ID', 0, 'incremental', 'Daily batch', 20, 'ENT-008,ENT-009'),
('ENT-003', 'SRC-001', 'ERP', 'OE_ORDER_HEADERS_ALL', 'Order Header', 'ORDER_ID', 0, 'incremental', 'Real-time (CDC)', 30, 'ENT-001,ENT-005'),
('ENT-004', 'SRC-001', 'ERP', 'OE_ORDER_LINES_ALL', 'Order Line Item', 'LINE_ID', 0, 'incremental', 'Real-time (CDC)', 40, 'ENT-003,ENT-007'),

-- Marketing System
('ENT-010', 'SRC-003', 'MARKETING', 'MARKETING_CAMPAIGNS', 'Marketing Campaign', 'CAMPAIGN_ID', 0, 'incremental', 'Daily batch', 5, NULL);

-- 3. Populate Load Dependencies
INSERT INTO control.load_dependencies (parent_table_id, child_table_id) VALUES
-- Customer Dependencies
('ENT-001', 'ENT-002'), -- Customer -> Registration
('ENT-001', 'ENT-005'), -- Customer -> Address
('ENT-001', 'ENT-003'), -- Customer -> Orders
('ENT-001', 'ENT-011'), -- Customer -> Incidents
('ENT-001', 'ENT-013'), -- Customer -> Surveys

-- Address Dependencies
('ENT-006', 'ENT-005'), -- City Tier -> Address

-- Product Dependencies
('ENT-008', 'ENT-007'), -- Category -> Product
('ENT-009', 'ENT-007'), -- Brand -> Product

-- Order Dependencies
('ENT-005', 'ENT-003'), -- Address -> Order Header
('ENT-003', 'ENT-004'), -- Order Header -> Order Lines
('ENT-007', 'ENT-004'), -- Product -> Order Lines
('ENT-003', 'ENT-011'), -- Order -> Incidents (Optional link)
('ENT-003', 'ENT-013'), -- Order -> Surveys (Optional link)

-- Incident Dependencies
('ENT-011', 'ENT-012'), -- Incident -> Interactions
('ENT-011', 'ENT-013'), -- Incident -> Surveys (Optional link)

-- Marketing Dependencies
('ENT-010', 'ENT-002'); -- Campaign -> Registration Source
