package ufc.victor;

import ufc.victor.db.mariadb.MariaDbTpccNewOrderPlanner;
import ufc.victor.db.mariadb.MariaDbTpccTransactionRunner;
import ufc.victor.db.mariadb.MariaDbWalMetricsCollector;
import ufc.victor.db.mariadb.MariaDbXaConfig;
import ufc.victor.db.mariadb.TpccNewOrderSpec;
import ufc.victor.db.mariadb.TpccNewOrderSpecFactory;
import ufc.victor.db.mariadb.WarehouseRouter;
import ufc.victor.experiment.ParticipantTrialMetrics;
import ufc.victor.experiment.TransactionTrialRecord;
import ufc.victor.protocol.SelectedProtocol;
import ufc.victor.protocol.coordinator.node.NodeId;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/** Runs logical New-Order trials with XA work split across warehouse shards. */
public final class MariaDbTpccClusterMain {

    private static final int NODE_COUNT = 4;

    private MariaDbTpccClusterMain() {
    }

    public static void main(String[] args) throws InterruptedException, SQLException {
        SelectedProtocol protocol = SelectedProtocol.valueOf(
                envOrDefault("ACP_PROTOCOL", SelectedProtocol.TWO_PC.name()).toUpperCase()
        );
        int transactions = positiveIntEnv("ACP_TPCC_TRANSACTIONS", 1);
        boolean forceRollback = Boolean.parseBoolean(
                envOrDefault("ACP_TPCC_FORCE_ROLLBACK", "false")
        );
        long timeoutMs = positiveLongEnv("ACP_TPCC_TIMEOUT_MS", 60_000L);
        Map<Integer, MariaDbXaConfig> configs = configs();
        MariaDbTpccNewOrderPlanner planner = new MariaDbTpccNewOrderPlanner(
                new WarehouseRouter(configs)
        );
        MariaDbTpccTransactionRunner runner = new MariaDbTpccTransactionRunner(
                planner,
                new MariaDbWalMetricsCollector(redoConfigs(configs)),
                timeoutMs
        );

        System.out.println("=== MARIADB CLUSTER / BENCHMARKSQL NEW-ORDER ===");
        System.out.println("protocol=" + protocol + ", transactions=" + transactions
                + ", forceRollback=" + forceRollback);
        System.out.println("Shard map: warehouse N is stored exclusively on mariadb-N.");

        int committed = 0;
        long totalLatencyMs = 0L;
        for (int index = 0; index < transactions; index++) {
            TpccNewOrderSpec spec = TpccNewOrderSpecFactory.controlled(List.of(1, 2, 3, 4), index);
            if (forceRollback) spec = TpccNewOrderSpecFactory.withInvalidFinalItem(spec);
            TransactionTrialRecord trial = runner.run(spec, protocol);
            committed += trial.committed() ? 1 : 0;
            totalLatencyMs += trial.totalLatency().toMillis();
            printTrial(index + 1, trial);
        }

        System.out.println("Summary: committed=" + committed + "/" + transactions
                + ", averageLatencyMs=" + (totalLatencyMs / (double) transactions));
    }

    private static void printTrial(int index, TransactionTrialRecord trial) {
        System.out.println("tx=" + index
                + ", transactionId=" + trial.transactionId()
                + ", specificationId=" + trial.specificationId()
                + ", outcome=" + trial.outcome()
                + ", latencyMs=" + trial.totalLatency().toMillis()
                + ", preparePhaseLatencyMs=" + trial.preparePhaseLatency().map(value -> value.toMillis()).orElse(null)
                + ", completionPhaseLatencyMs=" + trial.completionPhaseLatency().map(value -> value.toMillis()).orElse(null)
                + ", messages=" + trial.messageCount()
                + ", protocolLogWrites=" + trial.totalProtocolLogWrites()
                + ", resetDetected=" + trial.resetDetected()
                + ", unavailableRedo=" + trial.hasUnavailableRedoMetric());
        trial.participants().values().forEach(MariaDbTpccClusterMain::printParticipant);
    }

    private static void printParticipant(ParticipantTrialMetrics metrics) {
        var delta = metrics.redo();
        System.out.println("  redo node=" + metrics.nodeId()
                + ", lsnAdvanced=" + value(delta.lsnAdvanced())
                + ", flushedLsnAdvanced=" + value(delta.flushedLsnAdvanced())
                + ", bytesWritten=" + value(delta.redoBytesWritten())
                + ", writeRequests=" + value(delta.logWriteRequests())
                + ", logWrites=" + value(delta.logWrites())
                + ", logWaits=" + value(delta.logWaits())
                + ", checkpointAgeBefore=" + value(delta.checkpointAgeBeforeBytes())
                + ", checkpointAgeAfter=" + value(delta.checkpointAgeAfterBytes())
                + ", protocolLogWrites=" + metrics.protocolLogWrites()
                + ", resetDetected=" + delta.counterResetDetected()
                + ", unavailable=" + metrics.hasUnavailableRedoMetric());
    }

    private static String value(OptionalLong value) {
        return value.isPresent() ? Long.toString(value.getAsLong()) : "unavailable";
    }

    static Map<NodeId, MariaDbXaConfig> redoConfigs(Map<Integer, MariaDbXaConfig> configs) {
        Map<NodeId, MariaDbXaConfig> byNode = new LinkedHashMap<>();
        configs.forEach((warehouse, config) -> byNode.put(new NodeId("P" + warehouse), config));
        return Map.copyOf(byNode);
    }

    static Map<Integer, MariaDbXaConfig> configs() {
        String user = envOrDefault("ACP_MARIADB_USER", "acp");
        String password = envOrDefault("ACP_MARIADB_PASSWORD", "acp_secret");
        String database = envOrDefault("ACP_MARIADB_DATABASE", "acp");
        Map<Integer, MariaDbXaConfig> configs = new LinkedHashMap<>();
        for (int node = 1; node <= NODE_COUNT; node++) {
            String defaultUrl = "jdbc:mariadb://127.0.0.1:" + (33_060 + node) + "/" + database;
            configs.put(node, new MariaDbXaConfig(
                    envOrDefault("ACP_MARIADB_" + node + "_URL", defaultUrl), user, password
            ));
        }
        return Map.copyOf(configs);
    }

    static int positiveIntEnv(String name, int defaultValue) {
        int value = Integer.parseInt(envOrDefault(name, Integer.toString(defaultValue)));
        if (value < 1) throw new IllegalArgumentException(name + " must be at least 1");
        return value;
    }

    static long positiveLongEnv(String name, long defaultValue) {
        long value = Long.parseLong(envOrDefault(name, Long.toString(defaultValue)));
        if (value < 1L) throw new IllegalArgumentException(name + " must be at least 1");
        return value;
    }

    static String envOrDefault(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
