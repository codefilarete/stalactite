package org.stalactite.benchmark.connection;

import java.util.EnumMap;
import java.util.HashMap;

import org.hsqldb.jdbc.JDBCDataSource;

/**
 * @author Guillaume Mary
 */
public class HSQLInMemoryDataSourceFactory implements IDataSourceFactory {
	
	@Override
	public VerboseDataSource newDataSource(String host /* ignored */, String schema, String user, String password, EnumMap properties) {
		String url = "jdbc:hsqldb:mem:" + schema;
		JDBCDataSource jdbcDataSource = new JDBCDataSource();
		jdbcDataSource.setUrl(url);
		jdbcDataSource.setUser(user);
		jdbcDataSource.setPassword(password);
		return new VerboseDataSource(url, new HashMap<String, String>(), jdbcDataSource);
	}
}
