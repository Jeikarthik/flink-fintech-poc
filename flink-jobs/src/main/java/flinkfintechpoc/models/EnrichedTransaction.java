package flinkfintechpoc.models;

import java.math.BigDecimal;

/**
 * Represents a transaction enriched with merchant risk context.
 */
public class EnrichedTransaction {
    public TransactionEvent transaction;
    public BigDecimal merchant_risk_score;
    public String merchant_risk_category;

    public EnrichedTransaction() {}

    public EnrichedTransaction(TransactionEvent transaction, BigDecimal merchant_risk_score, String merchant_risk_category) {
        this.transaction = transaction;
        this.merchant_risk_score = merchant_risk_score;
        this.merchant_risk_category = merchant_risk_category;
    }

    @Override
    public String toString() {
        return "EnrichedTransaction{" +
                "txn_id='" + transaction.txn_id + '\'' +
                ", mcc='" + transaction.merchant_category_code + '\'' +
                ", risk_score=" + merchant_risk_score +
                ", risk_category='" + merchant_risk_category + '\'' +
                '}';
    }
}
