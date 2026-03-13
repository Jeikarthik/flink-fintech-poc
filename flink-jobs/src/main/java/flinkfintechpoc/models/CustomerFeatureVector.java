package flinkfintechpoc.models;

import java.math.BigDecimal;

/**
 * Represents the aggregated streaming features for a customer over a rolling window.
 */
public class CustomerFeatureVector {
    public String customer_id;
    public long timestamp_ms;
    
    // 7-day rolling window features
    public BigDecimal discretionary_spend_7d = BigDecimal.ZERO;
    public int atm_withdrawals_count_7d = 0;
    public int lending_app_txn_count_7d = 0;
    public BigDecimal weighted_lending_risk_7d = BigDecimal.ZERO;
    public BigDecimal savings_balance_pct_change_7d = BigDecimal.ZERO; // Optional, might need account events
    public int failed_autodebits_count_7d = 0;
    public int total_txn_count_7d = 0;
    public BigDecimal avg_txn_amount_7d = BigDecimal.ZERO;

    public CustomerFeatureVector() {}

    public CustomerFeatureVector(String customer_id, long timestamp_ms) {
        this.customer_id = customer_id;
        this.timestamp_ms = timestamp_ms;
    }

    @Override
    public String toString() {
        return "CustomerFeatureVector{" +
                "customer_id='" + customer_id + '\'' +
                ", total_txns_7d=" + total_txn_count_7d +
                ", lending_app_txns_7d=" + lending_app_txn_count_7d +
                ", discretionary_spend_7d=" + discretionary_spend_7d +
                '}';
    }
}
