package flinkfintechpoc.models;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Represents an update to a customer's account balance.
 */
public class AccountUpdateEvent {
    public String account_id;
    public String customer_id;
    public String account_type;
    public BigDecimal balance;
    public String currency;
    public Instant updated_at;

    public AccountUpdateEvent() {}

    @Override
    public String toString() {
        return "AccountUpdateEvent{" +
                "account_id='" + account_id + '\'' +
                ", customer_id='" + customer_id + '\'' +
                ", balance=" + balance +
                '}';
    }
}
