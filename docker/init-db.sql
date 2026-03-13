-- =============================================================================
-- Pre-Delinquency Intervention Engine — Database Schema
-- =============================================================================
-- This schema models a real bank's core data: customers, accounts, transactions,
-- merchant risk scoring, feature storage, risk scores, and intervention tracking.
-- =============================================================================

-- ─── Core Banking Tables ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    age INT,
    gender VARCHAR(10),
    region VARCHAR(100),
    tenure_months INT,
    product_holdings JSONB DEFAULT '{}',
    credit_bureau_score INT,
    expected_salary_day INT,
    preferred_channel VARCHAR(20) DEFAULT 'app',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
ALTER TABLE customers REPLICA IDENTITY FULL;

CREATE TABLE IF NOT EXISTS accounts (
    account_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) REFERENCES customers(customer_id),
    account_type VARCHAR(20) NOT NULL,
    balance DECIMAL(15,2) DEFAULT 0.00,
    currency VARCHAR(3) DEFAULT 'INR',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transactions (
    txn_id VARCHAR(36) PRIMARY KEY,
    customer_id VARCHAR(20) REFERENCES customers(customer_id),
    account_id VARCHAR(20) REFERENCES accounts(account_id),
    txn_type VARCHAR(20) NOT NULL,
    amount DECIMAL(15,2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'INR',
    merchant_id VARCHAR(50),
    merchant_category_code VARCHAR(10),
    merchant_name VARCHAR(255),
    direction VARCHAR(10) NOT NULL,
    status VARCHAR(20) DEFAULT 'success',
    channel VARCHAR(20),
    description TEXT,
    txn_timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ─── Merchant Risk Lookup (Static Reference Data) ───────────────────────────

CREATE TABLE IF NOT EXISTS merchant_risk_scores (
    mcc VARCHAR(10) PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL,
    risk_score DECIMAL(3,2) NOT NULL,
    risk_category VARCHAR(20) NOT NULL
);

-- ─── Feature Store Offline Table ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS customer_features_offline (
    customer_id VARCHAR(20),
    feature_timestamp TIMESTAMPTZ NOT NULL,
    -- Streaming features (snapshotted from Flink)
    discretionary_spend_7d DECIMAL(15,2),
    atm_withdrawals_count_7d INT,
    lending_app_txn_count_7d INT,
    weighted_lending_risk_7d DECIMAL(5,2),
    savings_balance_pct_change_7d DECIMAL(5,2),
    failed_autodebits_count_7d INT,
    total_txn_count_7d INT,
    avg_txn_amount_7d DECIMAL(15,2),
    -- Batch features (computed by Spark)
    salary_delay_days INT,
    utility_payment_avg_delay_days DECIMAL(5,1),
    discretionary_spend_trend DECIMAL(5,2),
    PRIMARY KEY (customer_id, feature_timestamp)
);

-- ─── Risk Score History ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS risk_scores (
    customer_id VARCHAR(20) REFERENCES customers(customer_id),
    score_timestamp TIMESTAMPTZ DEFAULT NOW(),
    risk_probability DECIMAL(5,4),
    credit_score INT,
    risk_tier VARCHAR(20),
    top_shap_factors JSONB,
    model_version VARCHAR(50),
    PRIMARY KEY (customer_id, score_timestamp)
);

-- ─── Intervention Tracking ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS interventions (
    intervention_id VARCHAR(36) PRIMARY KEY,
    customer_id VARCHAR(20) REFERENCES customers(customer_id),
    risk_score_at_trigger DECIMAL(5,4),
    intervention_type VARCHAR(50),
    channel VARCHAR(20),
    status VARCHAR(20) DEFAULT 'sent',
    outcome VARCHAR(20),
    triggered_at TIMESTAMPTZ DEFAULT NOW(),
    resolved_at TIMESTAMPTZ
);

-- ─── Indexes for Query Performance ──────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_transactions_customer ON transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(txn_timestamp);
CREATE INDEX IF NOT EXISTS idx_transactions_mcc ON transactions(merchant_category_code);
CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status);
CREATE INDEX IF NOT EXISTS idx_accounts_customer ON accounts(customer_id);
CREATE INDEX IF NOT EXISTS idx_features_customer ON customer_features_offline(customer_id);
CREATE INDEX IF NOT EXISTS idx_risk_scores_customer ON risk_scores(customer_id);
CREATE INDEX IF NOT EXISTS idx_risk_scores_tier ON risk_scores(risk_tier);
CREATE INDEX IF NOT EXISTS idx_interventions_customer ON interventions(customer_id);

-- ─── Seed Data: Merchant Risk Scores ────────────────────────────────────────
-- Based on real MCC codes. Risk score: 0.0 (safe) to 1.0 (high risk).

INSERT INTO merchant_risk_scores (mcc, category_name, risk_score, risk_category) VALUES
    -- LOW RISK (0.00 - 0.25): Essential spending
    ('5411', 'Grocery Stores / Supermarkets',           0.05, 'low'),
    ('5912', 'Drug Stores / Pharmacies',                0.05, 'low'),
    ('5541', 'Service Stations / Gas',                  0.08, 'low'),
    ('5812', 'Restaurants',                             0.10, 'low'),
    ('5814', 'Fast Food Restaurants',                   0.10, 'low'),
    ('5311', 'Department Stores',                       0.12, 'low'),
    ('5211', 'Building Materials / Hardware',            0.10, 'low'),
    ('5200', 'Home Supply Warehouse',                   0.10, 'low'),
    ('5331', 'Variety Stores',                          0.12, 'low'),
    ('4111', 'Local / Suburban Transit',                0.05, 'low'),
    ('4121', 'Taxi / Ride Share',                       0.08, 'low'),
    ('4131', 'Bus Lines',                               0.05, 'low'),
    ('4900', 'Utilities: Electric / Gas / Water',       0.05, 'low'),
    ('4814', 'Telecom Services',                        0.08, 'low'),
    ('4816', 'Internet / Network Services',             0.08, 'low'),
    ('5399', 'General Merchandise',                     0.10, 'low'),
    ('5451', 'Dairy Products Stores',                   0.05, 'low'),
    ('5462', 'Bakeries',                                0.05, 'low'),
    ('5499', 'Misc Food Stores',                        0.08, 'low'),
    ('8011', 'Doctors / Physicians',                    0.05, 'low'),
    ('8021', 'Dentists / Orthodontists',                0.05, 'low'),
    ('8099', 'Medical Services',                        0.08, 'low'),
    ('8211', 'Schools / Educational Services',          0.05, 'low'),
    ('8220', 'Colleges / Universities',                 0.05, 'low'),

    -- MEDIUM RISK (0.26 - 0.50): Discretionary spending
    ('5691', 'Clothing Stores',                         0.28, 'medium'),
    ('5699', 'Misc Apparel / Accessories',              0.28, 'medium'),
    ('5944', 'Jewelry / Watch / Clock Stores',          0.35, 'medium'),
    ('5732', 'Electronics Stores',                      0.30, 'medium'),
    ('5734', 'Computer Software Stores',                0.28, 'medium'),
    ('5735', 'Music / Record Stores',                   0.25, 'medium'),
    ('5941', 'Sporting Goods Stores',                   0.28, 'medium'),
    ('5945', 'Hobby / Toy / Game Stores',               0.30, 'medium'),
    ('5947', 'Gift / Card / Novelty Shops',             0.28, 'medium'),
    ('5661', 'Shoe Stores',                             0.25, 'medium'),
    ('7011', 'Hotels / Lodging',                        0.30, 'medium'),
    ('7032', 'Recreational Camps',                      0.30, 'medium'),
    ('7230', 'Beauty / Barber Shops',                   0.20, 'medium'),
    ('7298', 'Health / Spa',                            0.32, 'medium'),
    ('7512', 'Car Rental',                              0.28, 'medium'),
    ('7832', 'Movie Theaters',                          0.25, 'medium'),
    ('7841', 'Video / DVD Rental',                      0.25, 'medium'),
    ('7911', 'Dance Halls / Studios',                   0.30, 'medium'),
    ('7922', 'Theater Tickets',                         0.28, 'medium'),
    ('7929', 'Bands / Orchestras / Entertainment',      0.30, 'medium'),
    ('7933', 'Bowling Alleys',                          0.28, 'medium'),
    ('7941', 'Sports Clubs / Fields',                   0.30, 'medium'),
    ('7991', 'Tourist Attractions',                     0.28, 'medium'),
    ('7999', 'Recreation Services',                     0.32, 'medium'),
    ('5815', 'Digital Goods: Media / Books',            0.25, 'medium'),
    ('5816', 'Digital Goods: Games',                    0.35, 'medium'),
    ('5817', 'Digital Goods: Software',                 0.28, 'medium'),
    ('5818', 'Digital Goods: Large / Apps',             0.30, 'medium'),

    -- HIGH RISK (0.51 - 0.75): Financial stress indicators
    ('5933', 'Pawn Shops',                              0.70, 'high'),
    ('5960', 'Direct Marketing / Insurance',            0.55, 'high'),
    ('5962', 'Direct Marketing / Travel',               0.55, 'high'),
    ('5964', 'Direct Marketing / Catalog',              0.50, 'high'),
    ('5969', 'Direct Marketing / Other',                0.55, 'high'),
    ('6010', 'Financial Institutions / Manual Cash',    0.60, 'high'),
    ('6011', 'ATM / Cash Disbursements',                0.55, 'high'),
    ('6012', 'Financial Institutions / Merchandise',    0.65, 'high'),
    ('6050', 'Quasi Cash / Prepaid',                    0.60, 'high'),
    ('6211', 'Security Brokers / Dealers',              0.55, 'high'),
    ('6300', 'Insurance Sales / Underwriting',          0.50, 'high'),
    ('6513', 'Real Estate Agents / Rentals',            0.50, 'high'),
    ('5094', 'Precious Stones / Metals / Watches',      0.60, 'high'),
    ('7273', 'Dating / Escort Services',                0.70, 'high'),
    ('7297', 'Massage Parlors',                         0.65, 'high'),
    ('7994', 'Video / Arcade Game Rooms',               0.55, 'high'),
    ('7995', 'Betting / Casino Chips',                  0.75, 'high'),

    -- CRITICAL RISK (0.76 - 1.00): Strong distress signals
    ('6051', 'Quasi Cash / Lending Apps / Payday',      0.90, 'critical'),
    ('6052', 'Crypto / Digital Currency',               0.85, 'critical'),
    ('6540', 'Stored Value / Prepaid Wallet Loads',     0.80, 'critical'),
    ('7800', 'Government Lottery',                      0.80, 'critical'),
    ('7801', 'Online Gambling',                         0.92, 'critical'),
    ('7802', 'Horse / Dog Racing',                      0.88, 'critical'),
    ('9223', 'Bail / Bond Payments',                    0.95, 'critical'),
    ('9311', 'Tax Payments',                            0.78, 'critical'),
    ('9405', 'Intra-Government Purchases',              0.76, 'critical')
ON CONFLICT (mcc) DO NOTHING;

-- ─── Keep existing tables from old schema (backward compat) ─────────────────

CREATE TABLE IF NOT EXISTS outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregatetype VARCHAR(255) NOT NULL,
    aggregateid VARCHAR(255) NOT NULL,
    type VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
