package com.example.replicationmysqltestcontainers;

import com.example.replicationmysqltestcontainers.slavestatus.SlaveStatusRow;
import lombok.Getter;
import lombok.Setter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class MySQLService {
    private static final String REPL_USER = "repl";

    @Getter
    private final String id;
    @Getter
    private final String hostName;
    @Getter
    private final String replicationUser;
    @Getter
    private final String replicationPass = "replicapass";
    @Setter @Getter
    private String masterLogFile;

    private final ConnectionPool connectionPool;

    public MySQLService(String id, String hostNamePrefix, ConnectionPool connectionPool) {
        this.id = id;
        this.hostName = hostNamePrefix + id;
        this.replicationUser = REPL_USER + id;
        this.connectionPool = connectionPool;
    }

    public String getServerId() throws SQLException {
        try (Connection connection = this.connectionPool.getConnection()) {
            String query = "SELECT @@server_id as SERVER_ID;";
            PreparedStatement ps = connection.prepareStatement(query);
            ResultSet rs = ps.executeQuery();
            rs.next();
            return rs.getString("server_id");
        }
    }

    public void setReplicationUser() throws SQLException {
        performUpdate("CREATE USER '" + replicationUser + "'@'%' IDENTIFIED BY '" + replicationPass + "';");
        performUpdate("GRANT REPLICATION SLAVE ON *.* TO '" + replicationUser + "'@'%';");
    }

    public Map<MasterStatus, String> showMasterStatus() throws SQLException {
        try (Connection connection = this.connectionPool.getConnection()) {
            String query = "SHOW MASTER STATUS;";
            PreparedStatement ps = connection.prepareStatement(query);
            ResultSet rs = ps.executeQuery();
            rs.next();

            Map<MasterStatus, String> map = new HashMap<>();
            for (MasterStatus val : MasterStatus.values()) {
                map.put(val, rs.getString(val.name()));
            }
            return map;
        }
    }

    public void setReplicationSlave(String host, String user, String pass, String logFile) throws SQLException {
        String query = String.format("CHANGE MASTER TO MASTER_HOST='%s', MASTER_USER='%s', MASTER_PASSWORD='%s', MASTER_LOG_FILE='%s';",
                host, user, pass, logFile);
        performUpdate(query);
    }

    public void setReplicationSlave(String host, String user, String pass) throws SQLException {
        String query = String.format("CHANGE MASTER TO MASTER_HOST='%s', MASTER_USER='%s', MASTER_PASSWORD='%s', MASTER_AUTO_POSITION=1 FOR CHANNEL '%s';",
                host, user, pass, host);
        performUpdate(query);
    }

    public void startSlave() throws SQLException {
        performUpdate("START SLAVE;");
    }

    public void resetMaster() throws SQLException {
        performUpdate("RESET MASTER;");
    }

    public Map<Integer, SlaveStatusRow> showSlaveStatus() throws SQLException {
        try (Connection connection = this.connectionPool.getConnection()) {
            String query = "SHOW SLAVE STATUS;";
            PreparedStatement ps = connection.prepareStatement(query);
            ResultSet rs = ps.executeQuery();

            int idx = 1;
            Map<Integer, SlaveStatusRow> map = new HashMap<>();
            while (rs.next()) {
                map.put(idx++, SlaveStatusRow.create(rs));
            }
            return map;
        }
    }

    public void createDatabase(String name) throws SQLException {
        String createUserSql = "CREATE DATABASE " + name;
        try (Connection connection = this.connectionPool.getConnection()) {
            Statement createStatement = connection.createStatement();
            createStatement.execute(createUserSql);
        }
    }

    public Set<String> showDatabases() throws SQLException {
        try (Connection connection = this.connectionPool.getConnection()) {
            String query = "SHOW DATABASES;";
            PreparedStatement ps = connection.prepareStatement(query);
            ResultSet rs = ps.executeQuery();

            Set<String> set = new HashSet<>();
            while (rs.next()) {
                set.add(rs.getString("Database"));
            }
            return set;
        }
    }

    public String getGlobalGtidExecuted() throws SQLException {
        try (Connection connection = this.connectionPool.getConnection()) {
            String query = "SELECT @@global.gtid_executed;";
            PreparedStatement ps = connection.prepareStatement(query);
            ResultSet rs = ps.executeQuery();
            rs.next();
            return rs.getString(1);
        }
    }

    private void performUpdate(String query) throws SQLException {
        try (Connection connection = this.connectionPool.getConnection()) {
            PreparedStatement ps = connection.prepareStatement(query);
            ps.executeUpdate();
        }
    }
}
