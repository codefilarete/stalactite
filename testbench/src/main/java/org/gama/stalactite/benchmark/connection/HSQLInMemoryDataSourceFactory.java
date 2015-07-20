package org.gama.stalactite.benchmark.connection;

import org.gama.sql.UrlAwareDataSource;
import org.hsqldb.jdbc.JDBCDataSource;

import java.util.EnumMap;

/**
 * @author Guillaume Mary
 */
public class HSQLInMemoryDataSourceFactory implements IDataSourceFactory {
	
	@Override
	public UrlAwareDataSource newDataSource(String host /* ignored */, String schema, String user, String password, EnumMap properties) {
		String url = "jdbc:hsqldb:mem:" + schema;
		JDBCDataSource jdbcDataSource = new JDBCDataSource();
		jdbcDataSource.setUrl(url);
		jdbcDataSource.setUser(user);
		jdbcDataSource.setPassword(password);
		return new UrlAwareDataSource(url, jdbcDataSource);
	}
}
