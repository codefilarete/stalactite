package org.codefilarete.stalactite.sql.test;

import org.codefilarete.stalactite.sql.UrlAwareDataSource;
import org.codefilarete.tool.bean.Randomizer;
import org.hsqldb.jdbc.JDBCDataSource;

/**
 * Simple HSQLDB DataSource for tests
 * 
 * @author Guillaume Mary
 */
public class HSQLDBInMemoryDataSource extends UrlAwareDataSource {
	
	public HSQLDBInMemoryDataSource() {
		// random URL to avoid conflict between tests
		super("jdbc:hsqldb:mem:test" + Randomizer.INSTANCE.randomHexString(8));
		JDBCDataSource delegate = new JDBCDataSource();
		delegate.setUrl(getUrl());
		delegate.setUser("sa");
		delegate.setPassword("");
		setDelegate(delegate);
	}
}
