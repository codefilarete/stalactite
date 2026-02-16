package org.codefilarete.stalactite.sql.hsqldb.test;

import java.sql.Connection;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.UrlAwareDataSource;
import org.codefilarete.tool.bean.Randomizer;
import org.hsqldb.jdbc.JDBCDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple HSQLDB DataSource for tests
 * 
 * @author Guillaume Mary
 */
public class HSQLDBInMemoryDataSource extends UrlAwareDataSource {
	
	public static final Logger LOGGER = LoggerFactory.getLogger(HSQLDBInMemoryDataSource.class);
	
	public HSQLDBInMemoryDataSource() {
		// random URL to avoid conflict between tests
		super("jdbc:hsqldb:mem:test" + Randomizer.INSTANCE.randomHexString(8));
		JDBCDataSource delegate = new JDBCDataSource();
		delegate.setUrl(getUrl());
		delegate.setUser("sa");
		delegate.setPassword("");
		setDelegate(delegate);
	}
	
	@Override
	public Connection getConnection() throws SQLException {
		Connection connection = super.getConnection();
		LOGGER.info("giving connection " + connection);
		return connection;
	}
}
