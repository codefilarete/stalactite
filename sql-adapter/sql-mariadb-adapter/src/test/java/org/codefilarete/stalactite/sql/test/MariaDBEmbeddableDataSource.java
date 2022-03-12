package org.codefilarete.stalactite.sql.test;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import ch.vorburger.mariadb4j.DBConfigurationBuilder;
import org.codefilarete.stalactite.sql.UrlAwareDataSource;
import org.codefilarete.tool.exception.Exceptions;
import org.mariadb.jdbc.MariaDbDataSource;

/**
 * Simple MariaDB DataSource for tests
 *
 * @author Guillaume Mary
 */
public class MariaDBEmbeddableDataSource extends UrlAwareDataSource implements Closeable {
	
	public static final int DEFAULT_PORT = 3306;
	
	private static final Map<Integer, DB> USED_PORTS = new HashMap<>();
	
	private DB db;
	
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
		start();
		MariaDbDataSource delegate;
		try {
			delegate = new MariaDbDataSource("localhost", port, databaseName);
		} catch (SQLException e) {
			throw Exceptions.asRuntimeException(e);
		}
		setDelegate(delegate);
		try {
			db.createDB(databaseName);
		} catch (ManagedProcessException e) {
			throw Exceptions.asRuntimeException(e);
		}
	}
	
	private void start() {
		db = USED_PORTS.get(port);
		if (db == null) {
			try {
				DBConfigurationBuilder configBuilder = DBConfigurationBuilder.newBuilder();
				configBuilder.setPort(port);
				// for linux system with message "Fatal error: Please read "Security" section of the manual to find out how to run mysqld as root!" 
				configBuilder.addArg("--user=root");
				db = DB.newEmbeddedDB(configBuilder.build());
				db.start();
				USED_PORTS.put(port, db);
			} catch (ManagedProcessException e) {
				throw Exceptions.asRuntimeException(e);
			}
		}
	}
	
	@Override
	public void close() throws IOException {
		 // NB: if something goes wrong, we rely on MariaDB4J to close things properly.
		try {
			db.stop();
			USED_PORTS.remove(port);
		} catch (ManagedProcessException e) {
			throw new IOException(e);
		}
	}
}
