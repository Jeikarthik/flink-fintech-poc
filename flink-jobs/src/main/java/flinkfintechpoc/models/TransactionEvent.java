package flinkfintechpoc.models;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Represents a raw financial transaction from the banking system.
 */
public class TransactionEvent {
    public String txn_id;
    public String customer_id;
    public String account_id;
    public String txn_type;
    public BigDecimal amount;
    public String currency;
    public String merchant_id;
    public String merchant_category_code;
    public String merchant_name;
    public String direction;
    public String status;
    public String channel;
    public String description;
    public Instant txn_timestamp;

    public TransactionEvent() {}

    @Override
    public String toString() {
        return "TransactionEvent{" +
                "txn_id='" + txn_id + '\'' +
                ", customer_id='" + customer_id + '\'' +
                ", amount=" + amount +
                ", merchant_category_code='" + merchant_category_code + '\'' +
                '}';
    }
}
