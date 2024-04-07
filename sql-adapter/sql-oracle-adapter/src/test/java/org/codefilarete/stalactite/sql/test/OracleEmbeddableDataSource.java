package org.codefilarete.stalactite.sql.test;

import java.io.Closeable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.codefilarete.stalactite.sql.UrlAwareDataSource;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Simple MariaDB DataSource for tests
 *
 * @author Guillaume Mary
 */
public class OracleEmbeddableDataSource extends UrlAwareDataSource implements Closeable {
	
	public static final int ORACLE_DEFAULT_PORT = 1521;
	
	public static final int DEFAULT_PORT = 1522;
	
	// we reuse containers on same ports : databases/schemas will be created on-demand 
	private static final Map<Integer, OracleContainer> USED_PORTS = new HashMap<>();
	
	private OracleContainer container;
	
	/** DB port. Stored because configuration of DB is not accessible. */
	private final int port;
	
	public OracleEmbeddableDataSource() {
		this(DEFAULT_PORT);
	}
	
	public OracleEmbeddableDataSource(int port) {
		// "random" URL to avoid collision in tests
		this(port, "test" + Integer.toHexString(new Random().nextInt()));
	}
	
	private OracleEmbeddableDataSource(int port, String databaseName) {
		super("jdbc:oracle:thin:@localhost:" + port + ":" + databaseName);
		this.port = port;
		start(databaseName);
		setDelegate(new OracleDataSource("localhost", port, "localSID", container.getUsername(), container.getPassword()));
	}
	
	private void start(String databaseName) {
		container = USED_PORTS.get(port);
		if (container == null) {
			OracleContainer oracleContainer = new OracleContainer(DockerImageName.parse("gvenzl/oracle-free:23-slim-faststart").asCompatibleSubstituteFor("gvenzl/oracle-xe"));
			oracleContainer
					.withDatabaseName("localSID")
					// Oracle schema is tightly coupled to user so me use same thing for both
					.withUsername(databaseName)
					.withPassword(databaseName)
					.setPortBindings(Arrays.asList(this.port + ":" + ORACLE_DEFAULT_PORT));
			oracleContainer.start();
			USED_PORTS.put(port, oracleContainer);
			container = oracleContainer;
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				OracleContainer existingContainer = USED_PORTS.get(port);
				if (existingContainer != null && existingContainer.isRunning()) {
					existingContainer.stop();
				}
			}));
		}
	}
	
	@Override
	public void close() {
		USED_PORTS.get(port).stop();
		USED_PORTS.remove(port);
	}
}
