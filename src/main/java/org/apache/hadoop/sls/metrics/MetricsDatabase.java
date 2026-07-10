package org.apache.hadoop.sls.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * SQLite 持久化层。使用批处理写入和定时过期清理。
 * 实现 AutoCloseable 以便在 try-with-resources 或关闭钩子中安全释放。
 */
public class MetricsDatabase implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsDatabase.class);

    private final Connection connection;
    private final int batchSize;
    private final int retentionDays;
    private final ScheduledExecutorService cleanupExecutor;

    // 批处理 PreparedStatement（每个表一个）
    private PreparedStatement clusterPs;
    private PreparedStatement containerPs;
    private PreparedStatement applicationPs;
    private PreparedStatement heartbeatPs;
    private PreparedStatement queuePs;

    private int batchCount;

    /**
     * @param dbPath            SQLite 数据库文件路径
     * @param batchSize         批处理大小
     * @param retentionDays     数据保留天数（过期清理）
     * @param cleanupIntervalMs 清理任务执行间隔（毫秒）
     */
    public MetricsDatabase(String dbPath, int batchSize, int retentionDays, long cleanupIntervalMs) {
        this.batchSize = batchSize;
        this.retentionDays = retentionDays;
        this.batchCount = 0;

        Connection conn = null;
        ScheduledExecutorService exec = null;
        try {
            // 加载 SQLite JDBC 驱动
            Class.forName("org.sqlite.JDBC");
            conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);

            // 设置 PRAGMA 优化
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("PRAGMA journal_mode=WAL");
                stmt.execute("PRAGMA synchronous=NORMAL");
                stmt.execute("PRAGMA auto_vacuum=INCREMENTAL");
            }

            // 创建表
            createTables(conn);

            // 初始化批处理 PreparedStatement
            initPreparedStatements(conn);

            LOG.info("MetricsDatabase initialized: dbPath={}, batchSize={}, retentionDays={}, cleanupIntervalMs={}",
                    dbPath, batchSize, retentionDays, cleanupIntervalMs);

            // 启动定时清理
            exec = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "metrics-db-cleanup");
                t.setDaemon(true);
                return t;
            });
            exec.scheduleAtFixedRate(this::cleanupExpiredData,
                    cleanupIntervalMs, cleanupIntervalMs, TimeUnit.MILLISECONDS);

            this.connection = conn;
            this.cleanupExecutor = exec;

        } catch (Exception e) {
            // 连接泄漏保护：初始化失败时关闭已打开的数据库连接
            if (conn != null) {
                try { conn.close(); } catch (SQLException ignored) {}
            }
            if (exec != null) {
                exec.shutdown();
            }
            throw new RuntimeException("Failed to initialize MetricsDatabase", e);
        }
    }

    private void createTables(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // 1. 集群资源快照
            stmt.execute("CREATE TABLE IF NOT EXISTS cluster_resource_snapshots (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp BIGINT NOT NULL, " +
                    "total_nodes INTEGER NOT NULL, total_memory BIGINT NOT NULL, " +
                    "total_vcores INTEGER NOT NULL, allocated_memory BIGINT NOT NULL, " +
                    "allocated_vcores INTEGER NOT NULL, available_memory BIGINT NOT NULL, " +
                    "available_vcores INTEGER NOT NULL, mem_utilization REAL, vcore_utilization REAL)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_cluster_ts ON cluster_resource_snapshots(timestamp)");

            // 2. 容器调度快照
            stmt.execute("CREATE TABLE IF NOT EXISTS container_scheduling_snapshots (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp BIGINT NOT NULL, " +
                    "total_allocated BIGINT NOT NULL, total_released BIGINT NOT NULL, " +
                    "active_containers BIGINT NOT NULL, pending_containers INTEGER NOT NULL, " +
                    "reserved_containers INTEGER NOT NULL, allocate_rate REAL, release_rate REAL)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_container_ts ON container_scheduling_snapshots(timestamp)");

            // 3. 应用调度快照
            stmt.execute("CREATE TABLE IF NOT EXISTS application_snapshots (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp BIGINT NOT NULL, " +
                    "active_apps INTEGER NOT NULL, completed_apps INTEGER NOT NULL, " +
                    "failed_apps INTEGER NOT NULL, submitted_apps INTEGER NOT NULL)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_app_ts ON application_snapshots(timestamp)");

            // 4. 心跳快照
            stmt.execute("CREATE TABLE IF NOT EXISTS heartbeat_snapshots (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp BIGINT NOT NULL, " +
                    "success_count BIGINT NOT NULL, failed_count BIGINT NOT NULL, " +
                    "success_rate REAL, avg_latency REAL, max_latency BIGINT, throughput REAL)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_heartbeat_ts ON heartbeat_snapshots(timestamp)");

            // 5. 队列快照
            stmt.execute("CREATE TABLE IF NOT EXISTS queue_snapshots (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp BIGINT NOT NULL, " +
                    "queue_name TEXT NOT NULL, used_capacity REAL, abs_capacity REAL, " +
                    "pending_apps INTEGER NOT NULL, active_apps INTEGER NOT NULL)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_queue_ts ON queue_snapshots(timestamp)");
        }
    }

    private void initPreparedStatements(Connection conn) throws SQLException {
        clusterPs = conn.prepareStatement(
                "INSERT INTO cluster_resource_snapshots (timestamp, total_nodes, total_memory, total_vcores, " +
                        "allocated_memory, allocated_vcores, available_memory, available_vcores, " +
                        "mem_utilization, vcore_utilization) VALUES (?,?,?,?,?,?,?,?,?,?)");

        containerPs = conn.prepareStatement(
                "INSERT INTO container_scheduling_snapshots (timestamp, total_allocated, total_released, " +
                        "active_containers, pending_containers, reserved_containers, allocate_rate, release_rate) " +
                        "VALUES (?,?,?,?,?,?,?,?)");

        applicationPs = conn.prepareStatement(
                "INSERT INTO application_snapshots (timestamp, active_apps, completed_apps, " +
                        "failed_apps, submitted_apps) VALUES (?,?,?,?,?)");

        heartbeatPs = conn.prepareStatement(
                "INSERT INTO heartbeat_snapshots (timestamp, success_count, failed_count, " +
                        "success_rate, avg_latency, max_latency, throughput) VALUES (?,?,?,?,?,?,?)");

        queuePs = conn.prepareStatement(
                "INSERT INTO queue_snapshots (timestamp, queue_name, used_capacity, abs_capacity, " +
                        "pending_apps, active_apps) VALUES (?,?,?,?,?,?)");
    }

    /**
     * 添加快照到批处理。当达到 batchSize 时自动刷新。
     */
    public synchronized void add(MetricsSnapshot snapshot) {
        if (snapshot == null) {
            LOG.warn("Attempted to add null snapshot to database, ignoring");
            return;
        }
        try {
            // cluster_resource_snapshots
            clusterPs.setLong(1, snapshot.getTimestamp());
            clusterPs.setInt(2, snapshot.getTotalNodes());
            clusterPs.setLong(3, snapshot.getTotalMemory());
            clusterPs.setInt(4, snapshot.getTotalVCores());
            clusterPs.setLong(5, snapshot.getAllocatedMemory());
            clusterPs.setInt(6, snapshot.getAllocatedVCores());
            clusterPs.setLong(7, snapshot.getAvailableMemory());
            clusterPs.setInt(8, snapshot.getAvailableVCores());
            clusterPs.setDouble(9, snapshot.getClusterMemoryUtilization());
            clusterPs.setDouble(10, snapshot.getClusterVCoreUtilization());
            clusterPs.addBatch();

            // container_scheduling_snapshots
            containerPs.setLong(1, snapshot.getTimestamp());
            containerPs.setLong(2, snapshot.getTotalContainersAllocated());
            containerPs.setLong(3, snapshot.getTotalContainersReleased());
            containerPs.setLong(4, snapshot.getActiveContainers());
            containerPs.setInt(5, snapshot.getPendingContainers());
            containerPs.setInt(6, snapshot.getReservedContainers());
            containerPs.setDouble(7, snapshot.getContainerAllocateRate());
            containerPs.setDouble(8, snapshot.getContainerReleaseRate());
            containerPs.addBatch();

            // application_snapshots
            applicationPs.setLong(1, snapshot.getTimestamp());
            applicationPs.setInt(2, snapshot.getActiveApplications());
            applicationPs.setInt(3, snapshot.getCompletedApplications());
            applicationPs.setInt(4, snapshot.getFailedApplications());
            applicationPs.setInt(5, snapshot.getSubmittedApplications());
            applicationPs.addBatch();

            // heartbeat_snapshots
            heartbeatPs.setLong(1, snapshot.getTimestamp());
            heartbeatPs.setLong(2, snapshot.getSuccessfulHeartbeats());
            heartbeatPs.setLong(3, snapshot.getFailedHeartbeats());
            heartbeatPs.setDouble(4, snapshot.getHeartbeatSuccessRate());
            heartbeatPs.setDouble(5, snapshot.getAvgHeartbeatLatency());
            heartbeatPs.setLong(6, snapshot.getMaxHeartbeatLatency());
            heartbeatPs.setDouble(7, snapshot.getHeartbeatThroughput());
            heartbeatPs.addBatch();

            // queue_snapshots
            queuePs.setLong(1, snapshot.getTimestamp());
            queuePs.setString(2, snapshot.getQueueName() != null ? snapshot.getQueueName() : "default");
            queuePs.setDouble(3, snapshot.getQueueUsedCapacity());
            queuePs.setDouble(4, snapshot.getQueueAbsoluteCapacity());
            queuePs.setInt(5, snapshot.getQueuePendingApps());
            queuePs.setInt(6, snapshot.getQueueActiveApps());
            queuePs.addBatch();

            batchCount++;

            if (batchCount >= batchSize) {
                flush();
            }
        } catch (SQLException e) {
            LOG.error("Failed to add snapshot to database batch", e);
        }
    }

    /**
     * 立即刷新批处理，将缓存的数据写入数据库。
     */
    public synchronized void flush() {
        if (batchCount == 0) {
            return;
        }
        try {
            connection.setAutoCommit(false);
            clusterPs.executeBatch();
            containerPs.executeBatch();
            applicationPs.executeBatch();
            heartbeatPs.executeBatch();
            queuePs.executeBatch();
            connection.commit();
            batchCount = 0;
            LOG.debug("Flushed batch to database (snapshots in this batch: {})", batchCount);
        } catch (SQLException e) {
            LOG.error("Failed to flush batch to database, attempting rollback", e);
            try {
                connection.rollback();
            } catch (SQLException rollbackEx) {
                LOG.error("Rollback failed", rollbackEx);
            }
        } finally {
            try {
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                LOG.error("Failed to reset auto-commit", e);
            }
        }
    }

    /**
     * 清理过期数据：删除超过 retentionDays 天数的记录并执行增量 VACUUM。
     */
    private synchronized void cleanupExpiredData() {
        long cutoff = System.currentTimeMillis() - (retentionDays * 86400000L);
        LOG.info("Running cleanup for data older than {} ms (retentionDays={})", cutoff, retentionDays);
        try {
            int totalDeleted = 0;
            totalDeleted += executeDelete("cluster_resource_snapshots", cutoff);
            totalDeleted += executeDelete("container_scheduling_snapshots", cutoff);
            totalDeleted += executeDelete("application_snapshots", cutoff);
            totalDeleted += executeDelete("heartbeat_snapshots", cutoff);
            totalDeleted += executeDelete("queue_snapshots", cutoff);

            if (totalDeleted > 0) {
                try (Statement stmt = connection.createStatement()) {
                    stmt.execute("PRAGMA incremental_vacuum");
                }
                LOG.info("Cleanup complete: deleted {} expired records, vacuum executed", totalDeleted);
            } else {
                LOG.debug("Cleanup: no expired records found");
            }
        } catch (SQLException e) {
            LOG.error("Error during cleanup of expired data", e);
        }
    }

    private int executeDelete(String table, long cutoffTimestamp) throws SQLException {
        String sql = "DELETE FROM " + table + " WHERE timestamp < ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, cutoffTimestamp);
            return ps.executeUpdate();
        }
    }

    @Override
    public void close() {
        LOG.info("Closing MetricsDatabase");
        try {
            flush();
        } catch (Exception e) {
            LOG.warn("Error flushing before close", e);
        }
        if (cleanupExecutor != null && !cleanupExecutor.isShutdown()) {
            cleanupExecutor.shutdown();
            try {
                if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    cleanupExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                cleanupExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        try {
            if (clusterPs != null) clusterPs.close();
            if (containerPs != null) containerPs.close();
            if (applicationPs != null) applicationPs.close();
            if (heartbeatPs != null) heartbeatPs.close();
            if (queuePs != null) queuePs.close();
        } catch (SQLException e) {
            LOG.warn("Error closing prepared statements", e);
        }
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            LOG.warn("Error closing database connection", e);
        }
    }
}
