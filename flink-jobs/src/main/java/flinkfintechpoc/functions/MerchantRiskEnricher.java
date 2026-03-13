package flinkfintechpoc.functions;

import flinkfintechpoc.models.EnrichedTransaction;
import flinkfintechpoc.models.TransactionEvent;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Enriches transactions with merchant risk scores using a broadcast stream pattern.
 */
public class MerchantRiskEnricher extends BroadcastProcessFunction<TransactionEvent, Map<String, Object>, EnrichedTransaction> {

    private final MapStateDescriptor<String, Map<String, Object>> riskScoreDescriptor;

    public MerchantRiskEnricher(MapStateDescriptor<String, Map<String, Object>> riskScoreDescriptor) {
        this.riskScoreDescriptor = riskScoreDescriptor;
    }

    /**
     * Processes individual transactions.
     */
    @Override
    public void processElement(TransactionEvent txn, ReadOnlyContext ctx, Collector<EnrichedTransaction> out) throws Exception {
        ReadOnlyBroadcastState<String, Map<String, Object>> broadcastState = ctx.getBroadcastState(riskScoreDescriptor);
        
        // Default to medium risk if unseen
        BigDecimal riskScore = new BigDecimal("0.50");
        String riskCategory = "medium";
        
        if (txn.merchant_category_code != null) {
            Map<String, Object> riskData = broadcastState.get(txn.merchant_category_code);
            if (riskData != null) {
                // Handle possible Number formats returned from JDBC
                Object scoreObj = riskData.get("risk_score");
                if (scoreObj instanceof BigDecimal) {
                    riskScore = (BigDecimal) scoreObj;
                } else if (scoreObj instanceof Number) {
                    riskScore = BigDecimal.valueOf(((Number) scoreObj).doubleValue());
                } else if (scoreObj instanceof String) {
                    riskScore = new BigDecimal((String) scoreObj);
                }
                
                riskCategory = (String) riskData.get("risk_category");
            }
        }
        
        out.collect(new EnrichedTransaction(txn, riskScore, riskCategory));
    }

    /**
     * Processes updates to the merchant risk lookup table.
     */
    @Override
    public void processBroadcastElement(Map<String, Object> riskRecord, Context ctx, Collector<EnrichedTransaction> out) throws Exception {
        BroadcastState<String, Map<String, Object>> broadcastState = ctx.getBroadcastState(riskScoreDescriptor);
        String mcc = (String) riskRecord.get("mcc");
        if (mcc != null) {
            broadcastState.put(mcc, riskRecord);
        }
    }
}
