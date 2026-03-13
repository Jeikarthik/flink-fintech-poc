from datetime import timedelta
from feast import Entity, FeatureService, FeatureView, Field
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import PostgreSQLSource
from feast.types import Float32, Float64, Int32, Int64, String

# Define an entity for the customer
customer = Entity(
    name="customer",
    join_keys=["customer_id"],
    description="Customer entity for which we predict delinquency risk",
)

# Define the PostgreSQL source for offline features (batch + streaming historicals)
customer_features_source = PostgreSQLSource(
    name="customer_features_offline",
    query="SELECT * FROM public.customer_features_offline",
    timestamp_field="feature_timestamp",
)

# Real-time features (computed by Flink, synced to Postgres for historicals)
transaction_features_view = FeatureView(
    name="transaction_features",
    entities=[customer],
    ttl=timedelta(days=7),
    schema=[
        Field(name="discretionary_spend_7d", dtype=Float64),
        Field(name="atm_withdrawals_count_7d", dtype=Int32),
        Field(name="lending_app_txn_count_7d", dtype=Int32),
        Field(name="weighted_lending_risk_7d", dtype=Float64),
        Field(name="savings_balance_pct_change_7d", dtype=Float64),
        Field(name="failed_autodebits_count_7d", dtype=Int32),
        Field(name="total_txn_count_7d", dtype=Int32),
        Field(name="avg_txn_amount_7d", dtype=Float64),
    ],
    online=True,
    source=customer_features_source,
    tags={"team": "streaming_analytics"},
)

# Batch features (computed by Spark)
batch_features_view = FeatureView(
    name="batch_features",
    entities=[customer],
    ttl=timedelta(days=30),
    schema=[
        Field(name="salary_delay_days", dtype=Int32),
        Field(name="utility_payment_avg_delay_days", dtype=Float64),
        Field(name="discretionary_spend_trend", dtype=Float64),
    ],
    online=True,
    source=customer_features_source,
    tags={"team": "batch_analytics"},
)

# Define a Feature Service that bundles features for the risk model
risk_scoring_features = FeatureService(
    name="risk_scoring_features_v1",
    features=[
        transaction_features_view,
        batch_features_view
    ],
)
