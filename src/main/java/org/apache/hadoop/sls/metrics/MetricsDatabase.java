package org.apache.hadoop.sls.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.*;
import java.util.*;

/**
 * SQLite 持久化层。
 * 建表、批量写入、按时间范围查询、定时清理过期数据。
 */
public class MetricsDatabase implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsDatabase.class);

    private final String dbPath;
    private final int batchSize;
    private final int retentionDays;

    private Connection connection;
    private int batchCount;

    // 合法表名白名单（防止 SQL 注入）
    private static final Set<String> VALID_TABLES = Set.of(
            "cluster_resource_snapshots",
            "container_scheduling_snapshots",
            "application_snapshots",
            "heartbeat_snapshots",
            "queue_snapshots");

    // 批处理 PreparedStatement
    private PreparedStatement clusterPs;
    private PreparedStatement containerPs;
    private PreparedStatement applicationPs;
    private PreparedStatement heartbeatPs;
    private PreparedStatement queuePs;

    /**
     * @param dbPath        SQLite 数据库文件路径
     * @param batchSize     批处理大小
     * @param retentionDays 数据保留天数（过期清理）
     */
    public MetricsDatabase(String dbPath, int batchSize, int retentionDays) {
        this.dbPath = dbPath;
        this.batchSize = batchSize;
        this.retentionDays = retentionDays;
        this.batchCount = 0;
    }

    /**
     * 初始化——建表（如果不存在）。
     */
    public void init() throws SQLException {
        // 确保父目录存在
        File dbFile = new File(dbPath);
        File parentDir = dbFile.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            if (parentDir.mkdirs()) {
                LOG.debug("Created parent directories for database: {}", parentDir.getAbsolutePath());
            }
        }

        connection = DriverManager.getConnection("jdbc:sqlite:" + dbPath);

        // 设置 PRAGMA 优化
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("PRAGMA journal_mode=WAL");
            stmt.execute("PRAGMA synchronous=NORMAL");
        }

        // 建表
        createTables(connection);

        // 迁移：为已存在的表添加 pending_containers 列（兼容旧数据库）
        migrateQueueSnapshots(connection);

        // 初始化 PreparedStatement
        initPreparedStatements(connection);

        LOG.info("MetricsDatabase initialized: dbPath={}, batchSize={}, retentionDays={}",
                dbPath, batchSize, retentionDays);
    }

    private void createTables(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // 1. 集群资源快照
            stmt.execute("CREATE TABLE IF NOT EXISTS cluster_resource_snapshots (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "timestamp BIGINT NOT NULL, " +
                    "total_nodes INTEGER, " +
                    "total_memory_mb BIGINT, " +
                    "total_vcores INTEGER, " +
                    "allocated_memory_mb BIGINT, " +
                    "allocated_vcores INTEGER, " +
                    "available_memory_mb BIGINT, " +
                    "available_vcores INTEGER, " +
                    "cluster_utilization DOUBLE)");

            // 2. 容器调度快照
            stmt.execute("CREATE TABLE IF NOT EXISTS container_scheduling_snapshots (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "timestamp BIGINT NOT NULL, " +
                    "total_containers_allocated BIGINT, " +
                    "total_containers_released BIGINT, " +
                    "active_containers BIGINT, " +
                    "pending_containers INTEGER, " +
                    "reserved_containers INTEGER)");

            // 3. 应用状态快照
            stmt.execute("CREATE TABLE IF NOT EXISTS application_snapshots (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "timestamp BIGINT NOT NULL, " +
                    "active_applications INTEGER, " +
                    "completed_applications INTEGER, " +
                    "failed_applications INTEGER, " +
                    "submitted_applications INTEGER)");

            // 4. 心跳快照
            stmt.execute("CREATE TABLE IF NOT EXISTS heartbeat_snapshots (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "timestamp BIGINT NOT NULL, " +
                    "successful_heartbeats BIGINT, " +
                    "failed_heartbeats BIGINT, " +
                    "heartbeat_success_rate DOUBLE, " +
                    "avg_heartbeat_latency DOUBLE, " +
                    "max_heartbeat_latency DOUBLE, " +
                    "heartbeat_throughput DOUBLE)");

            // 5. 队列快照
            stmt.execute("CREATE TABLE IF NOT EXISTS queue_snapshots (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, " +
                    "timestamp BIGINT NOT NULL, " +
                    "queue_name VARCHAR(255), " +
                    "absolute_capacity DOUBLE, " +
                    "used_capacity DOUBLE, " +
                    "pending_apps INTEGER, " +
                    "active_apps INTEGER, " +
                    "pending_containers INTEGER DEFAULT 0)");

            // 索引
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_cluster_timestamp ON cluster_resource_snapshots(timestamp)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_container_timestamp ON container_scheduling_snapshots(timestamp)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_app_timestamp ON application_snapshots(timestamp)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_heartbeat_timestamp ON heartbeat_snapshots(timestamp)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_queue_timestamp ON queue_snapshots(timestamp)");
        }
    }

    /**
     * 迁移：为已存在的 queue_snapshots 表添加 pending_containers 列。
     * 不影响新建表（建表语句已包含该列）。
     */
    private void migrateQueueSnapshots(Connection conn) {
        try (Statement stmt = conn.createStatement()) {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getColumns(null, null, "queue_snapshots", "pending_containers")) {
                if (!rs.next()) {
                    stmt.execute("ALTER TABLE queue_snapshots ADD COLUMN pending_containers INTEGER DEFAULT 0");
                    LOG.info("Migration: added pending_containers column to queue_snapshots");
                }
            }
        } catch (SQLException e) {
            LOG.warn("Migration check for pending_containers failed: {}", e.getMessage());
        }
    }

    private void initPreparedStatements(Connection conn) throws SQLException {
        clusterPs = conn.prepareStatement(
                "INSERT INTO cluster_resource_snapshots " +
                        "(timestamp, total_nodes, total_memory_mb, total_vcores, " +
                        "allocated_memory_mb, allocated_vcores, " +
                        "available_memory_mb, available_vcores, cluster_utilization) " +
                        "VALUES (?,?,?,?,?,?,?,?,?)");

        containerPs = conn.prepareStatement(
                "INSERT INTO container_scheduling_snapshots " +
                        "(timestamp, total_containers_allocated, total_containers_released, " +
                        "active_containers, pending_containers, reserved_containers) " +
                        "VALUES (?,?,?,?,?,?)");

        applicationPs = conn.prepareStatement(
                "INSERT INTO application_snapshots " +
                        "(timestamp, active_applications, completed_applications, " +
                        "failed_applications, submitted_applications) " +
                        "VALUES (?,?,?,?,?)");

        heartbeatPs = conn.prepareStatement(
                "INSERT INTO heartbeat_snapshots " +
                        "(timestamp, successful_heartbeats, failed_heartbeats, " +
                        "heartbeat_success_rate, avg_heartbeat_latency, " +
                        "max_heartbeat_latency, heartbeat_throughput) " +
                        "VALUES (?,?,?,?,?,?,?)");

        queuePs = conn.prepareStatement(
                "INSERT INTO queue_snapshots " +
                        "(timestamp, queue_name, absolute_capacity, " +
                        "used_capacity, pending_apps, active_apps, pending_containers) " +
                        "VALUES (?,?,?,?,?,?,?)");
    }

    /**
     * 批量写入快照列表。
     *
     * @param snapshots 要写入的快照列表
     */
    public synchronized void insertBatch(List<MetricsSnapshot> snapshots) {
        if (snapshots == null || snapshots.isEmpty()) {
            return;
        }
        if (connection == null) {
            LOG.warn("Database not initialized, skipping batch insert");
            return;
        }
        try {
            connection.setAutoCommit(false);

            for (MetricsSnapshot snapshot : snapshots) {
                // cluster_resource_snapshots
                clusterPs.setLong(1, snapshot.getTimestamp());
                clusterPs.setInt(2, snapshot.getTotalNodes());
                clusterPs.setLong(3, snapshot.getTotalMemoryMB());
                clusterPs.setInt(4, snapshot.getTotalVCores());
                clusterPs.setLong(5, snapshot.getAllocatedMemoryMB());
                clusterPs.setInt(6, snapshot.getAllocatedVCores());
                clusterPs.setLong(7, snapshot.getAvailableMemoryMB());
                clusterPs.setInt(8, snapshot.getAvailableVCores());
                clusterPs.setDouble(9, snapshot.getClusterUtilizationPercent());
                clusterPs.addBatch();

                // container_scheduling_snapshots
                containerPs.setLong(1, snapshot.getTimestamp());
                containerPs.setLong(2, snapshot.getTotalContainersAllocated());
                containerPs.setLong(3, snapshot.getTotalContainersReleased());
                containerPs.setLong(4, snapshot.getActiveContainers());
                containerPs.setInt(5, snapshot.getPendingContainers());
                containerPs.setInt(6, snapshot.getReservedContainers());
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
                heartbeatPs.setDouble(5, snapshot.getAvgHeartbeatLatencyMs());
                heartbeatPs.setDouble(6, snapshot.getMaxHeartbeatLatencyMs());
                heartbeatPs.setDouble(7, snapshot.getHeartbeatThroughput());
                heartbeatPs.addBatch();

                // queue_snapshots
                if (snapshot.getQueueMetrics() != null && !snapshot.getQueueMetrics().isEmpty()) {
                    for (MetricsSnapshot.QueueMetrics qm : snapshot.getQueueMetrics().values()) {
                        queuePs.setLong(1, snapshot.getTimestamp());
                        queuePs.setString(2, qm.getQueueName());
                        queuePs.setDouble(3, qm.getAbsoluteCapacity());
                        queuePs.setDouble(4, qm.getUsedCapacity());
                        queuePs.setInt(5, qm.getPendingApps());
                        queuePs.setInt(6, qm.getActiveApps());
                        queuePs.setInt(7, qm.getPendingContainers());
                        queuePs.addBatch();
                    }
                }
            }

            clusterPs.executeBatch();
            containerPs.executeBatch();
            applicationPs.executeBatch();
            heartbeatPs.executeBatch();
            queuePs.executeBatch();

            connection.commit();
            batchCount += snapshots.size();

            LOG.debug("Inserted batch of {} snapshots into database", snapshots.size());
        } catch (SQLException e) {
            LOG.error("Failed to insert batch, attempting rollback", e);
            SQLException suppressed = null;
            try {
                if (connection != null) {
                    connection.rollback();
                }
            } catch (SQLException rollbackEx) {
                suppressed = rollbackEx;
                LOG.error("Rollback also failed", rollbackEx);
            }
            if (suppressed != null) {
                e.addSuppressed(suppressed);
            }
        } finally {
            try {
                if (connection != null) {
                    connection.setAutoCommit(true);
                }
            } catch (SQLException autoCommitEx) {
                LOG.error("Failed to reset auto-commit", autoCommitEx);
            }
        }
    }

    /**
     * 按时间范围查询，重构为 MetricsSnapshot 列表。
     *
     * @param startTime 起始时间戳（毫秒，含）
     * @param endTime   结束时间戳（毫秒，含）
     * @return 按时间升序排列的快照列表
     */
    public synchronized List<MetricsSnapshot> queryByTimeRange(long startTime, long endTime) {
        if (connection == null) {
            LOG.warn("Database not initialized, returning empty results");
            return List.of();
        }
        List<MetricsSnapshot> results = new ArrayList<>();

        // 先查询 cluster 表获取基础数据和 timestamp
        String clusterSql = "SELECT * FROM cluster_resource_snapshots " +
                "WHERE timestamp >= ? AND timestamp <= ? ORDER BY timestamp ASC";
        try (PreparedStatement ps = connection.prepareStatement(clusterSql)) {
            ps.setLong(1, startTime);
            ps.setLong(2, endTime);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    MetricsSnapshot snapshot = new MetricsSnapshot();
                    snapshot.setTimestamp(rs.getLong("timestamp"));
                    snapshot.setTotalNodes(rs.getInt("total_nodes"));
                    snapshot.setTotalMemoryMB(rs.getLong("total_memory_mb"));
                    snapshot.setTotalVCores(rs.getInt("total_vcores"));
                    snapshot.setAllocatedMemoryMB(rs.getLong("allocated_memory_mb"));
                    snapshot.setAllocatedVCores(rs.getInt("allocated_vcores"));
                    snapshot.setAvailableMemoryMB(rs.getLong("available_memory_mb"));
                    snapshot.setAvailableVCores(rs.getInt("available_vcores"));
                    snapshot.setClusterUtilizationPercent(rs.getDouble("cluster_utilization"));
                    results.add(snapshot);
                }
            }
        } catch (SQLException e) {
            LOG.error("Failed to query cluster snapshots by time range", e);
            return List.of();
        }

        if (results.isEmpty()) {
            return results;
        }

        // 构建时间戳→索引映射，替代脆弱的顺序索引匹配
        Map<Long, Integer> tsIndex = new LinkedHashMap<>();
        for (int i = 0; i < results.size(); i++) {
            tsIndex.put(results.get(i).getTimestamp(), i);
        }

        // 补充容器调度数据
        String containerSql = "SELECT * FROM container_scheduling_snapshots " +
                "WHERE timestamp >= ? AND timestamp <= ? ORDER BY timestamp ASC";
        try (PreparedStatement ps = connection.prepareStatement(containerSql)) {
            ps.setLong(1, startTime);
            ps.setLong(2, endTime);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Long ts = rs.getLong("timestamp");
                    Integer idx = tsIndex.get(ts);
                    if (idx != null) {
                        MetricsSnapshot snap = results.get(idx);
                        snap.setTotalContainersAllocated(rs.getLong("total_containers_allocated"));
                        snap.setTotalContainersReleased(rs.getLong("total_containers_released"));
                        snap.setActiveContainers(rs.getLong("active_containers"));
                        snap.setPendingContainers(rs.getInt("pending_containers"));
                        snap.setReservedContainers(rs.getInt("reserved_containers"));
                    }
                }
            }
        } catch (SQLException e) {
            LOG.error("Failed to query container snapshots", e);
        }

        // 补充应用数据
        String appSql = "SELECT * FROM application_snapshots " +
                "WHERE timestamp >= ? AND timestamp <= ? ORDER BY timestamp ASC";
        try (PreparedStatement ps = connection.prepareStatement(appSql)) {
            ps.setLong(1, startTime);
            ps.setLong(2, endTime);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Long ts = rs.getLong("timestamp");
                    Integer idx = tsIndex.get(ts);
                    if (idx != null) {
                        MetricsSnapshot snap = results.get(idx);
                        snap.setActiveApplications(rs.getInt("active_applications"));
                        snap.setCompletedApplications(rs.getInt("completed_applications"));
                        snap.setFailedApplications(rs.getInt("failed_applications"));
                        snap.setSubmittedApplications(rs.getInt("submitted_applications"));
                    }
                }
            }
        } catch (SQLException e) {
            LOG.error("Failed to query application snapshots", e);
        }

        // 补充心跳数据
        String heartbeatSql = "SELECT * FROM heartbeat_snapshots " +
                "WHERE timestamp >= ? AND timestamp <= ? ORDER BY timestamp ASC";
        try (PreparedStatement ps = connection.prepareStatement(heartbeatSql)) {
            ps.setLong(1, startTime);
            ps.setLong(2, endTime);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Long ts = rs.getLong("timestamp");
                    Integer idx = tsIndex.get(ts);
                    if (idx != null) {
                        MetricsSnapshot snap = results.get(idx);
                        snap.setSuccessfulHeartbeats(rs.getLong("successful_heartbeats"));
                        snap.setFailedHeartbeats(rs.getLong("failed_heartbeats"));
                        snap.setHeartbeatSuccessRate(rs.getDouble("heartbeat_success_rate"));
                        snap.setAvgHeartbeatLatencyMs(rs.getDouble("avg_heartbeat_latency"));
                        snap.setMaxHeartbeatLatencyMs(rs.getDouble("max_heartbeat_latency"));
                        snap.setHeartbeatThroughput(rs.getDouble("heartbeat_throughput"));
                    }
                }
            }
        } catch (SQLException e) {
            LOG.error("Failed to query heartbeat snapshots", e);
        }

        return results;
    }

    /**
     * 删除过期数据 + VACUUM。
     */
    public synchronized void cleanup() {
        if (connection == null) {
            return;
        }
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
                    stmt.execute("VACUUM");
                }
                LOG.info("Cleanup complete: deleted {} expired records, VACUUM executed", totalDeleted);
            } else {
                LOG.debug("Cleanup: no expired records found");
            }
        } catch (SQLException e) {
            LOG.error("Error during cleanup of expired data", e);
        }
    }

    private int executeDelete(String table, long cutoffTimestamp) throws SQLException {
        // 表名白名单校验，防止 SQL 注入
        if (!VALID_TABLES.contains(table)) {
            LOG.error("Invalid table name for DELETE: {}", table);
            return 0;
        }
        String sql = "DELETE FROM " + table + " WHERE timestamp < ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, cutoffTimestamp);
            return ps.executeUpdate();
        }
    }

    @Override
    public void close() {
        LOG.info("Closing MetricsDatabase");
        if (connection == null) {
            return;
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
            if (!connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            LOG.warn("Error closing database connection", e);
        }
    }
}
