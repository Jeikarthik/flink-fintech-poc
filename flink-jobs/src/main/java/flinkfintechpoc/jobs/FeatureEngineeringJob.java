package flinkfintechpoc.jobs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import flinkfintechpoc.functions.FeatureAggregator;
import flinkfintechpoc.functions.MerchantRiskEnricher;
import flinkfintechpoc.functions.RedisFeatureSink;
import flinkfintechpoc.models.CustomerFeatureVector;
import flinkfintechpoc.models.EnrichedTransaction;
import flinkfintechpoc.models.TransactionEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Flink Feature Engineering Job — the core real-time processing pipeline.
 * 
 * Pipeline:
 * 1. Read transactions from Kafka topic
 * 2. Load merchant risk reference data from PostgreSQL (as a bounded source)
 * 3. Enrich transactions with merchant risk scores via broadcast state
 * 4. Compute 7-day rolling features per customer via keyed state
 * 5. Write feature vectors to Redis (Feast online store)
 */
public class FeatureEngineeringJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

        // ── 1. Transaction Source (Kafka) ────────────────────────────────────
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("transactions")
                .setGroupId("flink-feature-engineering")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                "Kafka Transactions Source"
        );

        DataStream<TransactionEvent> txnStream = rawStream
                .map(json -> mapper.readValue(json, TransactionEvent.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TransactionEvent>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                                .withTimestampAssigner((event, ts) -> event.txn_timestamp.toEpochMilli())
                );

        // ── 2. Merchant Risk Reference Data (Loaded from JDBC) ───────────────
        // Load once at startup into a finite collection, then broadcast.
        List<Map<String, Object>> merchantRiskData = loadMerchantRiskScores();
        DataStream<Map<String, Object>> merchantRiskStream = env.fromCollection(merchantRiskData)
                .returns(new MapTypeInfo<>(String.class, Object.class));

        MapStateDescriptor<String, Map<String, Object>> riskScoreDescriptor = new MapStateDescriptor<>(
                "MerchantRiskScores",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<Map<String, Object>>() {})
        );

        BroadcastStream<Map<String, Object>> broadcastRiskStream = merchantRiskStream.broadcast(riskScoreDescriptor);

        // ── 3. Enrich Transactions ───────────────────────────────────────────
        DataStream<EnrichedTransaction> enrichedStream = txnStream
                .connect(broadcastRiskStream)
                .process(new MerchantRiskEnricher(riskScoreDescriptor));

        // ── 4. Compute Rolling Features ──────────────────────────────────────
        DataStream<CustomerFeatureVector> featureStream = enrichedStream
                .keyBy(txn -> txn.transaction.customer_id)
                .process(new FeatureAggregator());

        // ── 5. Sink to Redis ─────────────────────────────────────────────────
        featureStream.sinkTo(new RedisFeatureSink("redis", 6379))
                .name("Redis Feature Sink");

        // Also print for visibility in TaskManager logs
        featureStream.print("Feature Vector Update");

        env.execute("Feature Engineering & Rolling Windows");
    }

    /**
     * Loads merchant risk scores from PostgreSQL into an in-memory list.
     * This is called once at job submission time (on the client/JobManager).
     */
    private static List<Map<String, Object>> loadMerchantRiskScores() {
        List<Map<String, Object>> results = new ArrayList<>();
        String url = "jdbc:postgresql://postgres:5432/outbox_demo";
        
        try (Connection conn = DriverManager.getConnection(url, "postgres", "postgres");
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT mcc, category_name, risk_score, risk_category FROM merchant_risk_scores")) {
            
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("mcc", rs.getString("mcc"));
                row.put("category_name", rs.getString("category_name"));
                row.put("risk_score", rs.getBigDecimal("risk_score"));
                row.put("risk_category", rs.getString("risk_category"));
                results.add(row);
            }
            System.out.println("Loaded " + results.size() + " merchant risk scores from PostgreSQL.");
        } catch (SQLException e) {
            System.err.println("WARNING: Could not load merchant risk scores from PostgreSQL: " + e.getMessage());
            System.err.println("Proceeding with empty risk data (all transactions will get default risk).");
        }
        
        return results;
    }
}
