package org.stalactite.benchmark.connection;

import java.sql.SQLException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.sql.DataSource;

import org.gama.lang.bean.Objects;
import org.gama.lang.exception.Exceptions;
import org.stalactite.benchmark.connection.MySQLDataSourceFactory.Property;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

/**
 * @author Guillaume Mary
 */
public class MySQLDataSourceFactory implements IDataSourceFactory<Property> {
	
	public enum Property {
		rewriteBatchedStatements,
		cachePreparedStatements,
		preparedStatementCacheSize,
		preparedStatementCacheSqlLimit,
	}
	
	private static final EnumMap<Property, Object> DEFAULT_PROPERTIES = new EnumMap<>(Property.class);
	
	static {
		DEFAULT_PROPERTIES.put(Property.rewriteBatchedStatements, true);
		DEFAULT_PROPERTIES.put(Property.cachePreparedStatements, true);
		DEFAULT_PROPERTIES.put(Property.preparedStatementCacheSize, 100);
		DEFAULT_PROPERTIES.put(Property.preparedStatementCacheSqlLimit, 2048);
	}
	
	@Override
	public VerboseDataSource newDataSource(String host, String schema, String user, String password, EnumMap<Property, Object> properties) {
		DataSource delegate = null;
		String url = "jdbc:mysql://" + host + "/" + schema;
		try {
			MysqlDataSource mysqlDataSource = new MysqlDataSource();
			mysqlDataSource.setUrl(url);
			mysqlDataSource.setUser(user);
			mysqlDataSource.setPassword(password);
			// merging properties
			EnumMap<Property, Object> propertiesToApply = new EnumMap<>(DEFAULT_PROPERTIES);
			propertiesToApply.putAll(properties);
			// apply properties to DataSource
			applyProperties(mysqlDataSource, propertiesToApply);
			delegate = mysqlDataSource;
		} catch (SQLException e) {
			Exceptions.throwAsRuntimeException(e);
		}
		return new VerboseDataSource(url, toString(properties), delegate);
	}
	
	private void applyProperties(MysqlDataSource mysqlDataSource, EnumMap<Property, Object> properties) throws SQLException {
		for (Entry<Property, Object> entry : properties.entrySet()) {
			switch (entry.getKey()) {
				case rewriteBatchedStatements:
					mysqlDataSource.setRewriteBatchedStatements(Objects.preventNull((Boolean) entry.getValue(), true));
					break;
				case cachePreparedStatements:
					mysqlDataSource.setCachePreparedStatements(Objects.preventNull((Boolean) entry.getValue(), true));
					break;
				case preparedStatementCacheSize:
					mysqlDataSource.setPreparedStatementCacheSize(Objects.preventNull((Integer) entry.getValue(), 100));
					break;
				case preparedStatementCacheSqlLimit:
					mysqlDataSource.setPreparedStatementCacheSqlLimit(Objects.preventNull((Integer) entry.getValue(), 2048));
					break;
			}
		}
	}
	
	private Map<String, String> toString(EnumMap<Property, Object> properties) {
		Map<String, String> result = new HashMap<>(properties.size());
		for (Entry<Property, Object> entry : properties.entrySet()) {
			result.put(entry.getKey().name(), String.valueOf(entry.getValue()));
		}
		return result;
	}
}
