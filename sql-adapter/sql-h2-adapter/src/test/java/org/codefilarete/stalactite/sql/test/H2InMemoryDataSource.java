package org.codefilarete.stalactite.sql.test;

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
		super("jdbc:h2:mem:test" + Randomizer.INSTANCE.randomHexString(8));
		JdbcDataSource delegate = new JdbcDataSource();
		delegate.setUrl(getUrl());
		delegate.setUser("sa");
		delegate.setPassword("");
		setDelegate(delegate);
	}
}
