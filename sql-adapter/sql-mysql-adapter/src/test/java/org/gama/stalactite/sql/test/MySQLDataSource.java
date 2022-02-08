package org.gama.stalactite.sql.test;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.EnumMap;
import java.util.Map;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.exception.Exceptions;
import org.gama.stalactite.sql.UrlAwareDataSource;

/**
 * A {@link DataSource} that wraps MySQL one to ease configuration.
 * Activates some parameters to get better performances. 
 * 
 * @author Guillaume Mary
 */
public class MySQLDataSource extends UrlAwareDataSource {
	
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
	
	private MySQLDataSource(String url, DataSource delegate) {
		super(url, delegate);
	}
	
	public MySQLDataSource(String schema, String user, String password) {
		this("localhost", schema, user, password, DEFAULT_PROPERTIES);
	}
	
	public MySQLDataSource(String host, String schema, String user, String password) {
		this(host, schema, user, password, DEFAULT_PROPERTIES);
	}
	
	public MySQLDataSource(String host, String schema, String user, String password, EnumMap<Property, Object> properties) {
		this("jdbc:mysql://" + host + "/" + schema, null);
		try {
			MysqlDataSource mysqlDataSource = new MysqlDataSource();
			mysqlDataSource.setUrl(getUrl());
			mysqlDataSource.setUser(user);
			mysqlDataSource.setPassword(password);
			// merging properties
			EnumMap<Property, Object> propertiesToApply = new EnumMap<>(DEFAULT_PROPERTIES);
			propertiesToApply.putAll(properties);
			// apply properties to DataSource
			applyProperties(mysqlDataSource, propertiesToApply);
			setDelegate(mysqlDataSource);
		} catch (SQLException e) {
			throw Exceptions.asRuntimeException(e);
		}
	}
	
	private void applyProperties(MysqlDataSource mysqlDataSource, EnumMap<Property, Object> properties) throws SQLException {
		for (Map.Entry<Property, Object> entry : properties.entrySet()) {
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
}
