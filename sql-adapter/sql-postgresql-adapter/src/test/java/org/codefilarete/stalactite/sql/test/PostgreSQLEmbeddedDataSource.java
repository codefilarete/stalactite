package org.codefilarete.stalactite.sql.test;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.codefilarete.stalactite.sql.UrlAwareDataSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Simple PostgreSQL DataSource for tests
 * 
 * @author Guillaume Mary
 */
public class PostgreSQLEmbeddedDataSource extends UrlAwareDataSource implements Closeable {
	
	public static final int DEFAULT_PORT = 5432;
	
	// we reuse containers on same ports : databases/schemas will be created on-demand 
	private static final Map<Integer, PostgreSQLContainer> USED_PORTS = new HashMap<>();
	
	private PostgreSQLContainer container;
	private final int port;
	private final String databaseName;
	
	public PostgreSQLEmbeddedDataSource() {
		this(DEFAULT_PORT);
	}
	
	public PostgreSQLEmbeddedDataSource(int port) {
		// "random" URL to avoid collision in tests
		this(port, "test" + Integer.toHexString(new Random().nextInt()));
	}
	
	private PostgreSQLEmbeddedDataSource(int port, String databaseName) {
		super("jdbc:postgresql://localhost:" + port + "/" + databaseName);
		this.port = port;
		this.databaseName = databaseName;
		start(databaseName);
		PGSimpleDataSource delegate = new PGSimpleDataSource();
		delegate.setDatabaseName(container.getDatabaseName());
		delegate.setServerNames(new String[] { "localhost" });
		delegate.setPortNumbers(new int[] { port });
		delegate.setUser(container.getUsername());
		delegate.setPassword(container.getPassword());
		
		delegate.setCurrentSchema(databaseName);
		setDelegate(delegate);
	}

	@Override
	public Connection getConnection() throws SQLException {
		Connection conn = super.getConnection();
		conn.createStatement().execute("SET search_path TO " + databaseName + ";");
		return conn;
	}
	
	private void start(String databaseName) {
		container = USED_PORTS.get(port);
		if (container == null) {
			PostgreSQLContainer<?> mariaDBContainer = new PostgreSQLContainer<>(DockerImageName.parse("postgres:14.11"));
			mariaDBContainer
					.withReuse(true)
					.withDatabaseName("test")
					.withUsername("test")
					.withPassword("test")
					.setPortBindings(Arrays.asList(this.port + ":" + DEFAULT_PORT))
			;
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
		// stop Postgres
		container.stop();
	}
}
