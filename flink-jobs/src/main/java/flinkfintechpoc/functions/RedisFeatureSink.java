package flinkfintechpoc.functions;

import flinkfintechpoc.models.CustomerFeatureVector;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Flink 2.0 compatible Sink that writes feature vectors to Redis.
 * Uses the new Sink2 API (SinkWriter) instead of the removed RichSinkFunction.
 */
public class RedisFeatureSink implements Sink<CustomerFeatureVector> {

    private final String redisHost;
    private final int redisPort;

    public RedisFeatureSink(String redisHost, int redisPort) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }

    @Override
    public SinkWriter<CustomerFeatureVector> createWriter(WriterInitContext context) throws IOException {
        return new RedisFeatureWriter(redisHost, redisPort);
    }

    /**
     * Inner SinkWriter that does the actual Redis writes.
     */
    private static class RedisFeatureWriter implements SinkWriter<CustomerFeatureVector> {

        private final JedisPool jedisPool;

        RedisFeatureWriter(String redisHost, int redisPort) {
            this.jedisPool = new JedisPool(redisHost, redisPort);
        }

        @Override
        public void write(CustomerFeatureVector value, Context context) throws IOException {
            try (Jedis jedis = jedisPool.getResource()) {
                String redisKey = "customer_features:" + value.customer_id;

                Map<String, String> hash = new HashMap<>();
                hash.put("customer_id", value.customer_id);
                hash.put("timestamp_ms", String.valueOf(value.timestamp_ms));
                hash.put("discretionary_spend_7d", value.discretionary_spend_7d.toString());
                hash.put("atm_withdrawals_count_7d", String.valueOf(value.atm_withdrawals_count_7d));
                hash.put("lending_app_txn_count_7d", String.valueOf(value.lending_app_txn_count_7d));
                hash.put("weighted_lending_risk_7d", value.weighted_lending_risk_7d.toString());
                hash.put("failed_autodebits_count_7d", String.valueOf(value.failed_autodebits_count_7d));
                hash.put("total_txn_count_7d", String.valueOf(value.total_txn_count_7d));
                hash.put("avg_txn_amount_7d", value.avg_txn_amount_7d.toString());

                jedis.hset(redisKey, hash);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException {
            // Redis writes are synchronous, no buffering needed
        }

        @Override
        public void close() throws Exception {
            if (jedisPool != null) {
                jedisPool.close();
            }
        }
    }
}
