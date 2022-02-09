package org.codefilarete.stalactite.sql.test;

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;

import org.codefilarete.stalactite.sql.UrlAwareDataSource;
import org.h2.jdbcx.JdbcDataSource;

/**
 * Simple HSQLDB DataSource for tests
 * 
 * @author Guillaume Mary
 */
public class H2InMemoryDataSource extends UrlAwareDataSource implements Closeable {
	
	public H2InMemoryDataSource() {
		// random URL to avoid conflict between tests
		super("jdbc:h2:mem:test" + Integer.toHexString(new Random().nextInt()));
		JdbcDataSource delegate = new JdbcDataSource();
		delegate.setUrl(getUrl());
		delegate.setUser("sa");
		delegate.setPassword("");
		setDelegate(delegate);
	}
	
	@Override
	public void close() throws IOException {
		// nothing to do because H2DataSource doesn't need to be closed 
	}
}
