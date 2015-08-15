package org.gama.sql.test;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import org.gama.lang.exception.Exceptions;
import org.gama.sql.UrlAwareDataSource;
import org.mariadb.jdbc.MySQLDataSource;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Simple DataSource MariaDB for tests
 *
 * @author Guillaume Mary
 */
public class MariaDBEmbeddableDataSource extends UrlAwareDataSource implements Closeable {
	
	public static final int DEFAULT_PORT = 3306;
	
	private static final Map<Integer, DB> usedPorts = new HashMap<>();
	
	private DB db;
	
	/** DB port. Stored because configuration of DB is not accessible. */
	private int port;
	
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
		MySQLDataSource delegate = new MySQLDataSource("localhost", port, databaseName);
		setDelegate(delegate);
		try {
			db.createDB(databaseName);
		} catch (ManagedProcessException e) {
			Exceptions.throwAsRuntimeException(e);
		}
	}
	
	@Override
	public String toString() {
		return getUrl();
	}
	
	private void start() {
		db = usedPorts.get(port);
		if (db == null) {
			try {
				db = DB.newEmbeddedDB(port);
				db.start();
				usedPorts.put(port, db);
			} catch (ManagedProcessException e) {
				Exceptions.throwAsRuntimeException(e);
			}
		}
	}
	
	@Override
	public void close() throws IOException {
		 // NB: if something goes wrong, we rely on MariaDB4J to close things properly.
		try {
			db.stop();
			usedPorts.remove(port);
		} catch (ManagedProcessException e) {
			throw new IOException(e);
		}
	}
}