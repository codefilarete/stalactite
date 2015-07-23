package org.gama.sql.test;

import ch.vorburger.exec.ManagedProcessException;
import ch.vorburger.mariadb4j.DB;
import org.gama.lang.exception.Exceptions;
import org.gama.sql.UrlAwareDataSource;
import org.mariadb.jdbc.MySQLDataSource;

import java.util.Random;

/**
 * Simple DataSource MariaDB for tests
 *
 * @author Guillaume Mary
 */
public class MariaDBInMemoryDataSource extends UrlAwareDataSource {
	
	public static final int PORT = 3306;
	
	private static DB db;
	
	/**
	 * We start only one database to have faster tests.
	 * This is done statically to be simplier. We rely on MariaDB4J to close things properly.
	 */
	static {
		try {
			db = DB.newEmbeddedDB(PORT);
			db.start();
		} catch (ManagedProcessException e) {
			Exceptions.throwAsRuntimeException(e);
		}
	}
	
	public MariaDBInMemoryDataSource() {
		// URL "aléatoire" pour éviter des percussions dans les tests
		this(PORT, "test" + Integer.toHexString(new Random().nextInt()));
	}
	
	private MariaDBInMemoryDataSource(int port, String databaseName) {
		super("jdbc:mariadb://localhost:" + port + "/" + databaseName);
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
}
