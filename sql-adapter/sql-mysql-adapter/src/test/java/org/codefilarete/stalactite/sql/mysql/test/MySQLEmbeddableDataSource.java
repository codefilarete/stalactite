package org.codefilarete.stalactite.sql.mysql.test;

import java.io.Closeable;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.codefilarete.stalactite.sql.UrlAwareDataSource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Simple MariaDB DataSource for tests
 *
 * @author Guillaume Mary
 */
public class MySQLEmbeddableDataSource extends UrlAwareDataSource implements Closeable {
	
	public static final int MYSQL_DEFAULT_PORT = 3306;
	
	public static final int DEFAULT_PORT = 3306;
	
	// we reuse containers on same ports : databases/schemas will be created on-demand 
	private static final Map<Integer, MySQLContainer> USED_PORTS = new HashMap<>();
	
	private MySQLContainer container;
	
	/** DB port. Stored because configuration of DB is not accessible. */
	private final int port;
	
	public MySQLEmbeddableDataSource() {
		this(DEFAULT_PORT);
	}
	
	public MySQLEmbeddableDataSource(int port) {
		// "random" URL to avoid collision in tests
		this(port, "test" + Integer.toHexString(new Random().nextInt()));
	}
	
	private MySQLEmbeddableDataSource(int port, String databaseName) {
		super("jdbc:mariadb://localhost:" + port + "/" + databaseName);
		this.port = port;
		start(databaseName);
		setDelegate(new MySQLDataSource("localhost:" + port, databaseName, container.getUsername(), container.getPassword()));
	}
	
	private void start(String databaseName) {
		container = USED_PORTS.get(port);
		if (container == null) {
			MySQLContainer<?> mariaDBContainer = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"));
			mariaDBContainer
					.withUsername("root")
					.withPassword("")
					.setPortBindings(Arrays.asList(this.port + ":" + MYSQL_DEFAULT_PORT));
			mariaDBContainer.start();
			USED_PORTS.put(port, mariaDBContainer);
			container = mariaDBContainer;
		}
		try {
			container.createConnection("").prepareStatement("create schema " + databaseName).execute();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void close() {
		USED_PORTS.get(port).stop(); //optional, as there is a shutdown hook
	}
}
