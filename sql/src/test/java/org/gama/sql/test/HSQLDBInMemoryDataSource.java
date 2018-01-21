package org.gama.sql.test;

import java.util.Random;

import org.gama.sql.UrlAwareDataSource;
import org.hsqldb.jdbc.JDBCDataSource;

/**
 * Simple HSQLDB DataSource for tests
 * 
 * @author Guillaume Mary
 */
public class HSQLDBInMemoryDataSource extends UrlAwareDataSource {
	
	public HSQLDBInMemoryDataSource() {
		super("jdbc:hsqldb:mem:test" + Integer.toHexString(new Random().nextInt()));
		JDBCDataSource delegate = new JDBCDataSource();
		// random URL to avoid conflict between tests
		delegate.setUrl(getUrl());
		delegate.setUser("sa");
		delegate.setPassword("");
		setDelegate(delegate);
	}
}
