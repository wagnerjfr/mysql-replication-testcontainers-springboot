package com.example.replicationmysqltestcontainers;

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
    private final ConnectionPool connectionPool;

    public MySQLService(ConnectionPool connectionPool) {
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

    public void setReplicationUser(String user, String pass) throws SQLException {
        try (Connection connection = this.connectionPool.getConnection()) {
            String query = "CREATE USER '" + user + "'@'%' IDENTIFIED BY '" + pass + "';";
            PreparedStatement ps = connection.prepareStatement(query);
            ps.executeUpdate();

            query = "GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';";
            ps = connection.prepareStatement(query);
            ps.executeUpdate();
        }
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
        try (Connection connection = this.connectionPool.getConnection()) {
            String query = String.format("CHANGE MASTER TO MASTER_HOST='%s', MASTER_USER='%s', MASTER_PASSWORD='%s', MASTER_LOG_FILE='%s';",
                    host, user, pass, logFile);
            PreparedStatement ps = connection.prepareStatement(query);
            ps.executeUpdate();

            query = "START SLAVE;";
            ps = connection.prepareStatement(query);
            ps.executeUpdate();
        }
    }

    public Map<SlaveStatus, String> showSlaveStatus() throws SQLException {
        try (Connection connection = this.connectionPool.getConnection()) {
            String query = "SHOW SLAVE STATUS;";
            PreparedStatement ps = connection.prepareStatement(query);
            ResultSet rs = ps.executeQuery();
            rs.next();

            Map<SlaveStatus, String> map = new HashMap<>();
            for (SlaveStatus val : SlaveStatus.values()) {
                map.put(val, rs.getString(val.name()));
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
}
