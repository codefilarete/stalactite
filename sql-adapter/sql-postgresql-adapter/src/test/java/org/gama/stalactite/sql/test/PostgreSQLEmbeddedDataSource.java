package org.codefilarete.stalactite.sql.test;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.codefilarete.tool.exception.Exceptions;
import org.codefilarete.stalactite.sql.UrlAwareDataSource;
import org.postgresql.ds.PGSimpleDataSource;
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;

import static ru.yandex.qatools.embed.postgresql.distribution.Version.Main.V9_6;

/**
 * Simple PostgreSQL DataSource for tests
 * 
 * @author Guillaume Mary
 */
public class PostgreSQLEmbeddedDataSource extends UrlAwareDataSource implements Closeable {
	
	public static final int DEFAULT_PORT = 5432;
	
	private static final Map<Integer, EmbeddedPostgres> usedPorts = new HashMap<>();
	
	private EmbeddedPostgres db;
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
		start();
		final Connection conn;
		try {
			conn = DriverManager.getConnection(db.getConnectionUrl().get());
			conn.createStatement().execute("CREATE SCHEMA " + databaseName + ";");
		} catch (SQLException e) {
			throw Exceptions.asRuntimeException(e);
		}
		PGSimpleDataSource delegate = new PGSimpleDataSource();
		delegate.setDatabaseName("test");
		delegate.setServerNames(new String[] { "localhost" });
		delegate.setPortNumbers(new int[] { port });
		delegate.setUser(EmbeddedPostgres.DEFAULT_USER);
		delegate.setPassword(EmbeddedPostgres.DEFAULT_PASSWORD);
		delegate.setCurrentSchema(databaseName);
		setDelegate(delegate);
	}

	@Override
	public Connection getConnection() throws SQLException {
		Connection conn = super.getConnection();
		conn.createStatement().execute("SET search_path TO " + databaseName + ";");
		return conn;
	}

	private void start() {
		db = usedPorts.get(port);
		if (db == null) {
			try {
				db = new EmbeddedPostgres(V9_6);
				db.start("localhost", port, "test", EmbeddedPostgres.DEFAULT_USER, EmbeddedPostgres.DEFAULT_PASSWORD);
				usedPorts.put(port, db);
			} catch (IOException e) {
				throw Exceptions.asRuntimeException(e);
			}
		}
	}
	
	@Override
	public void close() throws IOException {
		// stop Postgres
		db.stop();
	}
}
