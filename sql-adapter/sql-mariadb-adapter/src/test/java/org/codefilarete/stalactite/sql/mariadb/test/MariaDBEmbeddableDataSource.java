package org.codefilarete.stalactite.sql.mariadb.test;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.codefilarete.stalactite.sql.UrlAwareDataSource;
import org.codefilarete.tool.exception.Exceptions;
import org.mariadb.jdbc.MariaDbDataSource;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Simple MariaDB DataSource for tests
 *
 * @author Guillaume Mary
 */
public class MariaDBEmbeddableDataSource extends UrlAwareDataSource implements Closeable {
	
	public static final int MARIADB_DEFAULT_PORT = 3306;
	
	// we shift default port to avoid being in conflict with MySQL wen running all tests through maven
	public static final int DEFAULT_PORT = 3406;
	
	// we reuse containers on same ports : databases/schemas will be created on-demand 
	private static final Map<Integer, MariaDBContainer> USED_PORTS = new HashMap<>();
	
	private MariaDBContainer container;
	
	/** DB port. Stored because configuration of DB is not accessible. */
	private final int port;
	
	public MariaDBEmbeddableDataSource() {
		this(DEFAULT_PORT);
	}
	
	public MariaDBEmbeddableDataSource(int port) {
		// "random" URL to avoid collision in tests
		this(port, "test" + Integer.toHexString(new Random().nextInt()));
	}
	
	private MariaDBEmbeddableDataSource(int port, String databaseName) {
		super("jdbc:mariadb://localhost:" + port + "/" + databaseName);
		this.port = port;
		start(databaseName);
		MariaDbDataSource delegate;
		try {
			delegate = new MariaDbDataSource(getUrl());
			delegate.setUser(container.getUsername());
			delegate.setPassword(container.getPassword());
		} catch (SQLException e) {
			throw Exceptions.asRuntimeException(e);
		}
		setDelegate(delegate);
	}
	
	private void start(String databaseName) {
		container = USED_PORTS.get(port);
		if (container == null) {
			MariaDBContainer<?> mariaDBContainer = new MariaDBContainer<>(DockerImageName.parse("mariadb:10.4"));
			mariaDBContainer
					.withUsername("root")
					.withPassword("")
					.setPortBindings(Arrays.asList(this.port + ":" + MARIADB_DEFAULT_PORT));
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
	public void close() throws IOException {
		 // NB: if something goes wrong, we rely on MariaDB4J to close things properly.
			container.stop();
			USED_PORTS.remove(port);
	}
}
