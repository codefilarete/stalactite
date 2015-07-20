package org.gama.sql.test;

import org.apache.derby.jdbc.EmbeddedDataSource;
import org.gama.sql.UrlAwareDataSource;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.Random;

/**
 * Simple DataSource Derby pour les tests
 * 
 * @author Guillaume Mary
 */
public class DerbyInMemoryDataSource extends UrlAwareDataSource {
	
	/** No operation logger to get rid of derby.log, see System.setProperty("derby.stream.error.field") */
	public static PrintWriter NOOP_LOGGER = new PrintWriter(new StringWriter(512));
	
	public DerbyInMemoryDataSource() {
		// URL "aléatoire" pour éviter des percussions dans les tests
		this("memory:"+Integer.toHexString(new Random().nextInt()));
	}
	
	private DerbyInMemoryDataSource(String databaseName) {
		super("jdbc:derby:" + databaseName);
		EmbeddedDataSource delegate = new EmbeddedDataSource();
		delegate.setDatabaseName(databaseName);
		delegate.setCreateDatabase("create");
		try {
			delegate.setLogWriter(new PrintWriter(new StringWriter(512)));
		} catch (SQLException e) {
			e.printStackTrace();
		}
		// get rid of derby.log (setLogWriter doesn't work)
		System.setProperty("derby.stream.error.field", "org.gama.sql.test.DerbyInMemoryDataSource.NOOP_LOGGER" );
		setDelegate(delegate);
	}
}
