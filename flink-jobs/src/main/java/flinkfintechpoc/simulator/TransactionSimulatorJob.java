package flinkfintechpoc.simulator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.javafaker.Faker;
import flinkfintechpoc.models.AccountUpdateEvent;
import flinkfintechpoc.models.TransactionEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.util.*;

public class TransactionSimulatorJob {

    private static final String KAFKA_BROKERS = "localhost:9092";
    private static final String TXN_TOPIC = "transactions";
    private static final String ACC_TOPIC = "account_updates";
    
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/outbox_demo";
    private static final String DB_USER = "postgres";
    private static final String DB_PASS = "postgres";

    private static final Faker faker = new Faker(Locale.of("en", "IN"));
    private static final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private static final Random random = new Random();

    // In-memory state for simulation
    private static final List<String> customerIds = new ArrayList<>();
    private static final Map<String, String> customerToAccount = new HashMap<>(); // simplification: 1 account per customer
    private static final Map<String, Double> accountBalances = new HashMap<>();

    // Some common MCCs for simulation
    private static final String[] MCCS = {
            "5411", // Grocery (low)
            "5812", // Restaurants (low)
            "4900", // Utilities (low)
            "5691", // Clothing (medium)
            "5732", // Electronics (medium)
            "7011", // Hotels (medium)
            "6011", // ATM (high)
            "6051", // Quasi Cash/Lending (critical)
            "7801"  // Gambling (critical)
    };

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Transaction Simulator...");

        // 1. Seed PostgreSQL database with customers
        seedDatabase(50);

        // 2. Setup Kafka Producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("Kafka Producer connected. Starting continuous simulation...");
            
            while (true) {
                // Simulate a transaction
                String customerId = customerIds.get(random.nextInt(customerIds.size()));
                String accountId = customerToAccount.get(customerId);
                Double currentBalance = accountBalances.get(accountId);

                TransactionEvent txn = generateRandomTransaction(customerId, accountId);
                
                // Update balance
                if (txn.direction.equals("debit")) {
                    currentBalance -= txn.amount.doubleValue();
                } else {
                    currentBalance += txn.amount.doubleValue();
                }
                accountBalances.put(accountId, currentBalance);

                // Publish Transaction
                String txnJson = mapper.writeValueAsString(txn);
                producer.send(new ProducerRecord<>(TXN_TOPIC, customerId, txnJson));

                // Publish Account Update
                AccountUpdateEvent accUpdate = new AccountUpdateEvent();
                accUpdate.account_id = accountId;
                accUpdate.customer_id = customerId;
                accUpdate.account_type = "savings";
                accUpdate.balance = BigDecimal.valueOf(currentBalance).setScale(2, RoundingMode.HALF_UP);
                accUpdate.currency = "INR";
                accUpdate.updated_at = Instant.now();
                
                String accJson = mapper.writeValueAsString(accUpdate);
                producer.send(new ProducerRecord<>(ACC_TOPIC, customerId, accJson));

                System.out.println("Sent TXN: " + txn.txn_id + " for " + customerId + " (" + txn.merchant_category_code + ") " + txn.amount + " INR");

                // Sleep to simulate real-time flow (e.g. 500ms between events across all users)
                Thread.sleep(200 + random.nextInt(800));
            }
        }
    }

    private static void seedDatabase(int numCustomers) {
        System.out.println("Seeding database with " + numCustomers + " customers...");
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            // Check if customers already exist
            var checkStmt = conn.createStatement();
            var rs = checkStmt.executeQuery("SELECT count(*) FROM customers");
            rs.next();
            if (rs.getInt(1) > 0) {
                System.out.println("Database already seeded. Loading existing customers into memory...");
                var rs_cust = conn.createStatement().executeQuery("SELECT c.customer_id, a.account_id, a.balance FROM customers c JOIN accounts a ON c.customer_id = a.customer_id");
                while (rs_cust.next()) {
                    String cid = rs_cust.getString("customer_id");
                    String aid = rs_cust.getString("account_id");
                    Double bal = rs_cust.getDouble("balance");
                    customerIds.add(cid);
                    customerToAccount.put(cid, aid);
                    accountBalances.put(aid, bal);
                }
                return;
            }

            String insertCustomer = "INSERT INTO customers (customer_id, name, age, expected_salary_day) VALUES (?, ?, ?, ?)";
            String insertAccount = "INSERT INTO accounts (account_id, customer_id, account_type, balance) VALUES (?, ?, ?, ?)";

            try (PreparedStatement custStmt = conn.prepareStatement(insertCustomer);
                 PreparedStatement accStmt = conn.prepareStatement(insertAccount)) {
                
                for (int i = 1; i <= numCustomers; i++) {
                    String customerId = String.format("CUST-%04d", i);
                    String name = faker.name().fullName();
                    int age = 18 + random.nextInt(60);
                    int salaryDay = 1 + random.nextInt(28);

                    custStmt.setString(1, customerId);
                    custStmt.setString(2, name);
                    custStmt.setInt(3, age);
                    custStmt.setInt(4, salaryDay);
                    custStmt.addBatch();

                    String accountId = String.format("ACC-%04d-SAV", i);
                    double initialBalance = 1000 + random.nextDouble() * 50000;
                    
                    accStmt.setString(1, accountId);
                    accStmt.setString(2, customerId);
                    accStmt.setString(3, "savings");
                    accStmt.setDouble(4, initialBalance);
                    accStmt.addBatch();

                    customerIds.add(customerId);
                    customerToAccount.put(customerId, accountId);
                    accountBalances.put(accountId, initialBalance);
                }
                
                custStmt.executeBatch();
                accStmt.executeBatch();
                System.out.println("Seeding complete.");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Failed to seed database. Note: ensure docker is running and postgres is ready.");
        }
    }

    private static TransactionEvent generateRandomTransaction(String customerId, String accountId) {
        TransactionEvent txn = new TransactionEvent();
        txn.txn_id = UUID.randomUUID().toString();
        txn.customer_id = customerId;
        txn.account_id = accountId;
        txn.txn_timestamp = Instant.now();
        txn.currency = "INR";
        txn.channel = "mobile";
        
        // 90% debit, 10% credit
        if (random.nextDouble() > 0.90) {
            txn.direction = "credit";
            txn.txn_type = "NEFT";
            txn.amount = BigDecimal.valueOf(5000 + random.nextInt(50000)).setScale(2, RoundingMode.HALF_UP);
            txn.merchant_id = "EMPLOYER";
            txn.merchant_category_code = "0000";
            txn.merchant_name = "Salary Credit";
            txn.status = "success";
            txn.description = "Monthly Salary";
        } else {
            txn.direction = "debit";
            txn.txn_type = random.nextBoolean() ? "UPI" : "Card";
            
            // Generate some failing auto-debits / normal failed txns
            if (random.nextDouble() < 0.05) {
                txn.status = "failed";
                txn.txn_type = "auto_debit";
            } else {
                txn.status = "success";
            }

            String mcc = MCCS[random.nextInt(MCCS.length)];
            txn.merchant_category_code = mcc;
            txn.merchant_id = "MERCHANT-" + random.nextInt(1000);
            
            double amount;
            switch(mcc) {
                case "6011": // ATM
                    amount = (1 + random.nextInt(10)) * 1000;
                    txn.merchant_name = "ATM Withdrawal";
                    txn.txn_type = "ATM";
                    break;
                case "6051": // Lending
                case "7801": // Gambling
                    amount = 500 + random.nextInt(20000);
                    txn.merchant_name = "High Risk Service " + random.nextInt(10);
                    break;
                case "4900": // Utility
                    amount = 500 + random.nextInt(3000);
                    txn.merchant_name = "Utility Bill";
                    txn.txn_type = "auto_debit";
                    break;
                default: // Groceries, restaurants, etc
                    amount = 50 + random.nextInt(5000);
                    txn.merchant_name = "Merchant " + random.nextInt(100);
            }
            txn.amount = BigDecimal.valueOf(amount).setScale(2, RoundingMode.HALF_UP);
            txn.description = "Payment to " + txn.merchant_name;
        }

        return txn;
    }
}
