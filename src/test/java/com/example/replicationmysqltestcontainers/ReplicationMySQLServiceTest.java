package com.example.replicationmysqltestcontainers;

import lombok.extern.slf4j.Slf4j;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@SpringBootTest
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ReplicationMySQLServiceTest {

	private static final Logger logger = LoggerFactory.getLogger(ReplicationMySQLServiceTest.class);

	private static final String DOCKER_IMAGE = "mysql:5.7";
	private static final String LOG_BIN = "mysql-bin-1";
	private static final String REPL_USER = "repl";
	private static final String REPL_PASS = "replicapass";
	private static final String DATABASE_NAME = "Testcontainers";
	private static String SOURCE_HOST_NAME;
	private static int testCount;

	private static String logFile;
	private static boolean replicationSetInSource;

	@ClassRule
	static Network network = Network.newNetwork();

	@ClassRule
	static MySQLContainer<?> source = new MySQLContainer<>(DOCKER_IMAGE)
			//.withLogConsumer(new Slf4jLogConsumer(logger))
			.withCommand("mysqld --server-id=1 --log-bin=" + LOG_BIN +".log")
			.withUsername("root")
			.withPassword("")
			.withNetwork(network);

	@ClassRule
	static MySQLContainer<?> replica1 = new MySQLContainer<>(DOCKER_IMAGE)
			//.withLogConsumer(new Slf4jLogConsumer(logger))
			.withUsername("root")
			.withPassword("")
			.withCommand("mysqld --server-id=2")
			.withNetwork(network);

	@ClassRule
	static MySQLContainer<?> replica2 = new MySQLContainer<>(DOCKER_IMAGE)
			//.withLogConsumer(new Slf4jLogConsumer(logger))
			.withUsername("root")
			.withPassword("")
			.withCommand("mysqld --server-id=3")
			.withNetwork(network);

	static MySQLService sourceMySQLService, replicaMySQLService1, replicaMySQLService2;

	@BeforeAll
	static void startDb() {
		sourceMySQLService = startMySQLService(source);
	}

	@AfterAll
	static void stopDb(){
		stopMySQLService(source);
		stopMySQLService(replica1);
		stopMySQLService(replica2);
		network.close();
	}

	@BeforeEach
	void beforeEach(TestInfo testInfo) {
		log.info("Test {}: {}", ++testCount, testInfo.getDisplayName());
	}

	@Test
	@Order(1)
	@DisplayName("Source running")
	public void sourceContainerRunning() throws SQLException {
		assertTrue(source.isRunning());
		assertEquals("1", sourceMySQLService.getServerId());
		SOURCE_HOST_NAME = source.getNetworkAliases().get(0);
	}

	@Test
	@Order(2)
	@DisplayName("Configuring Source")
	public void configuringSource() throws SQLException {
		sourceMySQLService.setReplicationUser(REPL_USER, REPL_PASS);
		Map<MasterStatus, String> map = sourceMySQLService.showMasterStatus();

		assertEquals(2, map.size());
		logFile = map.get(MasterStatus.File);
		assertTrue(logFile.contains(LOG_BIN));

		final int position = Integer.parseInt(map.get(MasterStatus.Position));
		assertTrue(position >= 0);
		replicationSetInSource = true;
	}

	@Test
	@Order(3)
	@DisplayName("Replicas running")
	public void replicasContainerRunning() throws SQLException {
		Assumptions.assumeTrue(replicationSetInSource);

		replicaMySQLService1 = startMySQLService(replica1);
		replicaMySQLService2 = startMySQLService(replica2);
		assertTrue(replica1.isRunning());
		assertTrue(replica2.isRunning());
		assertEquals("2", replicaMySQLService1.getServerId());
		assertEquals("3", replicaMySQLService2.getServerId());
	}

	@Test
	@Order(4)
	@DisplayName("Configuring Source")
	public void configuringReplicas() throws SQLException {
		Assumptions.assumeTrue(replicationSetInSource);

		List<MySQLService> replicas = Arrays.asList(replicaMySQLService1, replicaMySQLService2);
		for (MySQLService mySQLService : replicas) {
			mySQLService.setReplicationSlave(SOURCE_HOST_NAME, REPL_USER, REPL_PASS, logFile);
		}

		final String YES = "Yes";
		final String ZERO = "0";

		for (MySQLService mySQLService : replicas) {
			Map<SlaveStatus, String> map = mySQLService.showSlaveStatus();

			log.info("Slave Status: {}", map);
			assertEquals(SOURCE_HOST_NAME, map.get(SlaveStatus.Master_Host), "Master_Host doesn't match");
			assertEquals(logFile, map.get(SlaveStatus.Master_Log_File), "Master_Log_File doesn't match");
			assertEquals(YES, map.get(SlaveStatus.Slave_IO_Running), "Slave_IO_Running doesn't match");
			assertEquals(YES, map.get(SlaveStatus.Slave_SQL_Running), "Slave_SQL_Running doesn't match");
			assertEquals(ZERO, map.get(SlaveStatus.Last_IO_Errno), "Last_IO_Errno doesn't match");
			assertTrue(map.get(SlaveStatus.Last_IO_Error).isEmpty(), "Last_IO_Error doesn't match");
			assertEquals(ZERO, map.get(SlaveStatus.Last_SQL_Errno), "Last_SQL_Errno doesn't match");
			assertTrue(map.get(SlaveStatus.Last_SQL_Error).isEmpty(), "Last_SQL_Error doesn't match");
		}
	}

	@Test
	@Order(5)
	@DisplayName("Creating database in Source")
	public void createDatabaseInSource() throws SQLException {
		sourceMySQLService.createDatabase(DATABASE_NAME);
		Set<String> databases = sourceMySQLService.showDatabases();
		log.info("Source databases: {}", databases);
		assertTrue(databases.contains(DATABASE_NAME));
	}

	@Test
	@Order(6)
	@DisplayName("Checking database in Replicas")
	public void checkDatabaseInReplicas() throws SQLException {
		List<MySQLService> replicas = Arrays.asList(replicaMySQLService1, replicaMySQLService2);
		for (MySQLService mySQLService : replicas) {
			Set<String> databases = mySQLService.showDatabases();
			log.info("Replica{} databases: {}", replicas.indexOf(mySQLService) + 1, databases);
			assertTrue(databases.contains(DATABASE_NAME));
		}
	}

	private static MySQLService startMySQLService(MySQLContainer<?> container) {
		container.start();
		String url = container.getJdbcUrl();
		ConnectionPool connectionPool =  new ConnectionPool(url, container.getUsername(), container.getPassword());
		return new MySQLService(connectionPool);
	}

	private static void stopMySQLService(MySQLContainer<?> container) {
		if (container != null) {
			container.stop();
		}
	}
}
