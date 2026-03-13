package flinkfintechpoc.functions;

import flinkfintechpoc.models.CustomerFeatureVector;
import flinkfintechpoc.models.EnrichedTransaction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Iterator;
import java.util.Map;

/**
 * Computes 7-day rolling features per customer using manual MapState and timers.
 * Uses Flink 2.0's open(OpenContext) method signature.
 */
public class FeatureAggregator extends KeyedProcessFunction<String, EnrichedTransaction, CustomerFeatureVector> {

    private transient MapState<Long, EnrichedTransaction> txnState;
    private static final long WINDOW_DURATION_MS = 7 * 24 * 60 * 60 * 1000L; // 7 days

    @Override
    public void open(OpenContext openContext) throws Exception {
        MapStateDescriptor<Long, EnrichedTransaction> descriptor = new MapStateDescriptor<>(
                "transactions-state",
                Types.LONG,
                Types.POJO(EnrichedTransaction.class)
        );
        txnState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void processElement(EnrichedTransaction txn, Context ctx, Collector<CustomerFeatureVector> out) throws Exception {
        long currentTs = txn.transaction.txn_timestamp.toEpochMilli();
        
        txnState.put(currentTs, txn);
        ctx.timerService().registerEventTimeTimer(currentTs + WINDOW_DURATION_MS);
        out.collect(computeFeatures(ctx.getCurrentKey(), ctx.timestamp()));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<CustomerFeatureVector> out) throws Exception {
        long expiredElementTs = timestamp - WINDOW_DURATION_MS;
        txnState.remove(expiredElementTs);
        out.collect(computeFeatures(ctx.getCurrentKey(), timestamp));
    }

    private CustomerFeatureVector computeFeatures(String customerId, long currentTimestamp) throws Exception {
        CustomerFeatureVector vector = new CustomerFeatureVector(customerId, currentTimestamp);
        
        BigDecimal totalAmount = BigDecimal.ZERO;
        long windowStart = currentTimestamp - WINDOW_DURATION_MS;
        
        Iterator<Map.Entry<Long, EnrichedTransaction>> iterator = txnState.entries().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, EnrichedTransaction> entry = iterator.next();
            Long timestamp = entry.getKey();
            
            if (timestamp < windowStart) {
                iterator.remove();
                continue;
            }
            
            EnrichedTransaction t = entry.getValue();
            vector.total_txn_count_7d++;
            totalAmount = totalAmount.add(t.transaction.amount);
            
            if ("debit".equals(t.transaction.direction) && "success".equals(t.transaction.status)) {
                String mcc = t.transaction.merchant_category_code;
                if ("5691".equals(mcc) || "5732".equals(mcc) || "7011".equals(mcc) || "5812".equals(mcc)
                        || "5944".equals(mcc) || "5945".equals(mcc) || "7832".equals(mcc)) {
                    vector.discretionary_spend_7d = vector.discretionary_spend_7d.add(t.transaction.amount);
                }
                
                if ("ATM".equals(t.transaction.txn_type) || "6011".equals(mcc)) {
                    vector.atm_withdrawals_count_7d++;
                }
                
                if ("6051".equals(mcc) || "6012".equals(mcc) || "critical".equals(t.merchant_risk_category)) {
                    vector.lending_app_txn_count_7d++;
                    vector.weighted_lending_risk_7d = vector.weighted_lending_risk_7d.add(t.merchant_risk_score);
                }
            }
            
            if ("auto_debit".equals(t.transaction.txn_type) && "failed".equals(t.transaction.status)) {
                vector.failed_autodebits_count_7d++;
            }
        }
        
        if (vector.total_txn_count_7d > 0) {
            vector.avg_txn_amount_7d = totalAmount.divide(new BigDecimal(vector.total_txn_count_7d), 2, RoundingMode.HALF_UP);
        }
        
        return vector;
    }
}
