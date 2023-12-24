package com.example.replicationmysqltestcontainers;

import com.example.replicationmysqltestcontainers.slavestatus.SlaveStatusRow;
import lombok.extern.slf4j.Slf4j;
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
class SingleSourceMySQLReplicationTest {

	private static final Logger logger = LoggerFactory.getLogger(SingleSourceMySQLReplicationTest.class);

	private static final String DOCKER_IMAGE = "mysql:5.7";
	private static final String LOG_BIN = "mysql-bin-1";
	private static final String DATABASE_NAME = "Testcontainers";
	private static final String SOURCE_HOST_NAME = "source";
	private static final String REPLICA_HOST_NAME = "replica";
	private static final String PASSWORD = "mypass";
	private static int testCount;
	private static boolean replicationSetInSource;

	static Network network = Network.newNetwork();

	static MySQLContainer<?> sourceMySQLContainer = new MySQLContainer<>(DOCKER_IMAGE)
			//.withLogConsumer(new Slf4jLogConsumer(logger))
			.withCommand("mysqld --server-id=1 --log-bin=" + LOG_BIN +".log")
			.withUsername("root")
			.withPassword(PASSWORD)
			.withCreateContainerCmdModifier(it -> it.withHostName(SOURCE_HOST_NAME + 1))
			.withNetwork(network);

	static MySQLContainer<?> replicaMySQLContainer1 = new MySQLContainer<>(DOCKER_IMAGE)
			//.withLogConsumer(new Slf4jLogConsumer(logger))
			.withUsername("root")
			.withPassword(PASSWORD)
			.withCommand("mysqld --server-id=2")
			.withNetwork(network);

	static MySQLContainer<?> replicaMySQLContainer2 = new MySQLContainer<>(DOCKER_IMAGE)
			//.withLogConsumer(new Slf4jLogConsumer(logger))
			.withUsername("root")
			.withPassword(PASSWORD)
			.withCommand("mysqld --server-id=3")
			.withNetwork(network);

	static MySQLService source, replica1, replica2;

	@BeforeAll
	static void startDb() {
		source = Utils.startMySQLService("1", SOURCE_HOST_NAME, sourceMySQLContainer);
	}

	@AfterAll
	static void stopDb(){
		Utils.stopMySQLService(sourceMySQLContainer);
		Utils.stopMySQLService(replicaMySQLContainer1);
		Utils.stopMySQLService(replicaMySQLContainer2);
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
		assertTrue(sourceMySQLContainer.isRunning());
		assertEquals("1", source.getServerId());
	}

	@Test
	@Order(2)
	@DisplayName("Configuring Source")
	public void configuringSource() throws SQLException {
		source.setReplicationUser();
		Map<MasterStatus, String> map = source.showMasterStatus();

		assertEquals(2, map.size());
		String logFile = map.get(MasterStatus.File);
		assertTrue(logFile.contains(LOG_BIN));
		source.setMasterLogFile(logFile);

		final int position = Integer.parseInt(map.get(MasterStatus.Position));
		assertTrue(position >= 0);
		replicationSetInSource = true;
	}

	@Test
	@Order(3)
	@DisplayName("Replicas running")
	public void replicasContainerRunning() throws SQLException {
		Assumptions.assumeTrue(replicationSetInSource);

		replica1 = Utils.startMySQLService("2", REPLICA_HOST_NAME, replicaMySQLContainer1);
		replica2 = Utils.startMySQLService("3", REPLICA_HOST_NAME, replicaMySQLContainer2);
		assertTrue(replicaMySQLContainer1.isRunning());
		assertTrue(replicaMySQLContainer2.isRunning());
		assertEquals(replica1.getId(), replica1.getServerId());
		assertEquals(replica2.getId(), replica2.getServerId());
	}

	@Test
	@Order(4)
	@DisplayName("Configuring Replicas")
	public void configuringReplicas() throws SQLException {
		Assumptions.assumeTrue(replicationSetInSource);

		List<MySQLService> replicas = Arrays.asList(replica1, replica2);
		for (MySQLService mySQLService : replicas) {
			mySQLService.setReplicationSlave(source.getHostName(), source.getReplicationUser(), source.getReplicationPass(), source.getMasterLogFile());
			mySQLService.startSlave();
		}

		final String YES = "Yes";
		final String ZERO = "0";

		for (MySQLService mySQLService : replicas) {
			Map<Integer, SlaveStatusRow> map = mySQLService.showSlaveStatus();

			for (SlaveStatusRow status : map.values()) {
				log.info("Slave Status: {}", map);
				assertEquals(source.getHostName(), status.masterHost(), "Master_Host doesn't match");
				assertEquals(source.getMasterLogFile(), status.masterLogFile(), "Master_Log_File doesn't match");
				assertEquals(YES, status.slaveIORunning(), "Slave_IO_Running doesn't match");
				assertEquals(YES, status.slaveSQLRunning(), "Slave_SQL_Running doesn't match");
				assertEquals(ZERO, status.lastIOErrno(), "Last_IO_Errno doesn't match");
				assertTrue(status.lastIOError().isEmpty(), "Last_IO_Error doesn't match");
				assertEquals(ZERO, status.lastSQLErrno(), "Last_SQL_Errno doesn't match");
				assertTrue(status.lastSQLError().isEmpty(), "Last_SQL_Error doesn't match");
			}
		}
	}

	@Test
	@Order(5)
	@DisplayName("Creating database in Source")
	public void createDatabaseInSource() throws SQLException {
		source.createDatabase(DATABASE_NAME);
		Set<String> databases = source.showDatabases();
		log.info("Source databases: {}", databases);
		assertTrue(databases.contains(DATABASE_NAME), "Database doesn't exist.");
		Utils.sleep(5);
	}

	@Test
	@Order(6)
	@DisplayName("Checking database in Replicas")
	public void checkDatabaseInReplicas() throws SQLException {
		List<MySQLService> replicas = Arrays.asList(replica1, replica2);
		for (MySQLService mySQLService : replicas) {
			Set<String> databases = mySQLService.showDatabases();
			log.info("Replica{} databases: {}", replicas.indexOf(mySQLService) + 1, databases);
			assertTrue(databases.contains(DATABASE_NAME), "Database doesn't exist.");
		}
	}
}
