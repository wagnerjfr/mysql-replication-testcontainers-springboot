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
class MultiSourceMySQLReplicationTest {

	private static final String DOCKER_IMAGE = "mysql:5.7";
	private static final String LOG_BIN = "mysql-bin-1";
	private static final String LOG_FILE = LOG_BIN + ".log";
	private static final List<String> DATABASES = Arrays.asList("DB1", "DB2");
	private static final String SOURCE_HOST_NAME = "source";
	private static final String REPLICA_HOST_NAME = "replica";
	private static List<MySQLService> sources;
	private static int testCount;

	private static boolean replicationSetInSources;
	private static boolean replicationSetInReplica;

	static Network network = Network.newNetwork();

	static MySQLContainer<?> sourceMySQLContainer1 = Utils.getContainer(DOCKER_IMAGE, 1, SOURCE_HOST_NAME, LOG_FILE, network);

	static MySQLContainer<?> sourceMySQLContainer2 = Utils.getContainer(DOCKER_IMAGE, 2, SOURCE_HOST_NAME, LOG_FILE, network);

	static MySQLContainer<?> replicaMySQLContainer = Utils.getContainer(DOCKER_IMAGE, 3, REPLICA_HOST_NAME, LOG_FILE, network);

	static MySQLService source1, source2, replica;

	@BeforeAll
	static void startDbs() throws SQLException {
		source1 = Utils.startMySQLService("1", SOURCE_HOST_NAME, sourceMySQLContainer1);
		source2 = Utils.startMySQLService("2", SOURCE_HOST_NAME, sourceMySQLContainer2);
		sources = Arrays.asList(source1, source2);

		for (MySQLService source : sources) {
			source.resetMaster();
			assertTrue(source.getGlobalGtidExecuted().isEmpty(), "There are transactions in the database");
		}
	}

	@AfterAll
	static void stopDbs(){
		Utils.stopMySQLService(sourceMySQLContainer1);
		Utils.stopMySQLService(sourceMySQLContainer2);
		Utils.stopMySQLService(replicaMySQLContainer);
		network.close();
	}

	@BeforeEach
	void beforeEach(TestInfo testInfo) {
		log.info("Test {}: {}", ++testCount, testInfo.getDisplayName());
	}

	@Test
	@Order(1)
	@DisplayName("Sources running")
	public void sourcesContainerRunning() throws SQLException {
		assertTrue(sourceMySQLContainer1.isRunning());
		assertEquals(source1.getId(), source1.getServerId());

		assertTrue(sourceMySQLContainer2.isRunning());
		assertEquals(source2.getId(), source2.getServerId());
	}

	@Test
	@Order(2)
	@DisplayName("Configuring Sources")
	public void configuringSources() throws SQLException {
		for (MySQLService source : sources) {
			source.setReplicationUser();
			Map<MasterStatus, String> map = source.showMasterStatus();

			assertEquals(2, map.size());
			String logFile = map.get(MasterStatus.File);
			assertTrue(logFile.contains(LOG_BIN));
			source.setMasterLogFile(logFile);

			final int position = Integer.parseInt(map.get(MasterStatus.Position));
			assertTrue(position >= 0);
		}
		replicationSetInSources = true;
	}

	@Test
	@Order(3)
	@DisplayName("Replica running")
	public void replicaContainerRunning() throws SQLException {
		Assumptions.assumeTrue(replicationSetInSources);

		replica = Utils.startMySQLService("3", REPLICA_HOST_NAME, replicaMySQLContainer);
		assertTrue(replicaMySQLContainer.isRunning());
		assertEquals(replica.getId(), replica.getServerId());
		replica.resetMaster();
		assertTrue(replica.getGlobalGtidExecuted().isEmpty(), "There are transactions in the database");
	}

	@Test
	@Order(4)
	@DisplayName("Configuring Replica")
	public void configuringReplica() throws SQLException {
		Assumptions.assumeTrue(replicationSetInSources);

		for (MySQLService source : sources) {
			replica.setReplicationSlave(source.getHostName(), source.getReplicationUser(), source.getReplicationPass());
		}
		replica.startSlave();

		Utils.sleep(5);

		final String YES = "Yes";
		final String ZERO = "0";

		Map<Integer, SlaveStatusRow> map = replica.showSlaveStatus();

		int idx = 0;
		log.info("Slave Status: {}", map);
		for (SlaveStatusRow row : map.values()) {
			final MySQLService source = sources.get(idx++);

			assertEquals(source.getHostName(), row.masterHost(), "Master_Host doesn't match");
			assertEquals(source.getMasterLogFile(), row.masterLogFile(), "Master_Log_File doesn't match");
			assertEquals(YES, row.slaveIORunning(), "Slave_IO_Running doesn't match");
			assertEquals(YES, row.slaveSQLRunning(), "Slave_SQL_Running doesn't match");
			assertEquals(ZERO, row.lastIOErrno(), "Last_IO_Errno doesn't match");
			assertTrue(row.lastIOError().isEmpty(), "Last_IO_Error doesn't match");
			assertEquals(ZERO, row.lastSQLErrno(), "Last_SQL_Errno doesn't match");
			assertTrue(row.lastSQLError().isEmpty(), "Last_SQL_Error doesn't match");
		}
		replicationSetInReplica = true;
	}

	@Test
	@Order(5)
	@DisplayName("Creating database in Sources")
	public void createDatabaseInSources() throws SQLException {
		Assumptions.assumeTrue(replicationSetInReplica);

		int idx = 0;
		for (MySQLService source : sources) {
			final String database = DATABASES.get(idx++);
			source.createDatabase(database);
			Set<String> databases = source.showDatabases();
			log.info("Source databases: {}", databases);
			assertTrue(databases.contains(database), "Database doesn't exist.");
		}
		Utils.sleep(5);
	}

	@Test
	@Order(6)
	@DisplayName("Checking databases in Replica")
	public void checkDatabaseInReplica() throws SQLException {
		Set<String> databases = replica.showDatabases();
		log.info("Replica databases: {}", databases);
		assertTrue(databases.containsAll(DATABASES), "Database doesn't exist.");
	}
}
