package org.gama.stalactite.sql.test;

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;

import org.gama.stalactite.sql.UrlAwareDataSource;
import org.hsqldb.jdbc.JDBCDataSource;

/**
 * Simple HSQLDB DataSource for tests
 * 
 * @author Guillaume Mary
 */
public class HSQLDBInMemoryDataSource extends UrlAwareDataSource implements Closeable {
	
	public HSQLDBInMemoryDataSource() {
		// random URL to avoid conflict between tests
		super("jdbc:hsqldb:mem:test" + Integer.toHexString(new Random().nextInt()));
		JDBCDataSource delegate = new JDBCDataSource();
		delegate.setUrl(getUrl());
		delegate.setUser("sa");
		delegate.setPassword("");
		setDelegate(delegate);
	}
	
	@Override
	public void close() throws IOException {
		// nothing to do because HSQLDataSource doesn't need to be closed 
	}
}
