package org.codefilarete.stalactite.sql.h2.test;

import org.codefilarete.stalactite.sql.UrlAwareDataSource;
import org.codefilarete.tool.bean.Randomizer;
import org.h2.jdbcx.JdbcDataSource;

/**
 * Simple HSQLDB DataSource for tests
 * 
 * @author Guillaume Mary
 */
public class H2InMemoryDataSource extends UrlAwareDataSource {
	
	public H2InMemoryDataSource() {
		// random URL to avoid conflict between tests
		this("test" + Randomizer.INSTANCE.randomHexString(8));
	}
	
	public H2InMemoryDataSource(String databaseName) {
		super("jdbc:h2:mem:" + databaseName);
		JdbcDataSource delegate = new JdbcDataSource();
		delegate.setUrl(getUrl());
		delegate.setUser("sa");
		delegate.setPassword("");
		setDelegate(delegate);
	}
}
