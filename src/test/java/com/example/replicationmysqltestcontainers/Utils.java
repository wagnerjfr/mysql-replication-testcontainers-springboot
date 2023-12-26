package com.example.replicationmysqltestcontainers;

import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;

public class Utils {

    public static MySQLContainer<?> getContainer(String dockerImage, int id, String hostNamePrefix, String logFile, Network network) {
        return new MySQLContainer<>(dockerImage)
                //.withLogConsumer(new Slf4jLogConsumer(logger))
                .withCommand(getCommand(id, logFile))
                .withUsername("root")
                .withPassword("mypass")
                .withCreateContainerCmdModifier(it -> it.withHostName(hostNamePrefix + id))
                .withNetwork(network);
    }

    private static String getCommand(int id, String logFile) {
        return String.format("mysqld --server-id=%s --log-bin=%s --relay_log_info_repository=TABLE " +
                "--master-info-repository=TABLE --gtid-mode=on --enforce-gtid-consistency", id, logFile);
    }

    public static MySQLService startMySQLService(String id, String hostNamePrefix, MySQLContainer<?> container) {
        container.start();
        String url = container.getJdbcUrl();
        ConnectionPool connectionPool =  new ConnectionPool(url, container.getUsername(), container.getPassword());
        return new MySQLService(id, hostNamePrefix, connectionPool);
    }

    public static void stopMySQLService(MySQLContainer<?> container) {
        if (container != null) {
            container.stop();
        }
    }

    public static void sleep(long timeInSeconds) {
        try {
            Thread.sleep(timeInSeconds * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
