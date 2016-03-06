package org.gama.stalactite.benchmark.connection;

import org.gama.lang.exception.Exceptions;
import org.gama.sql.UrlAwareDataSource;
import org.gama.stalactite.benchmark.connection.MySQLDataSourceFactory.Property;
import org.mariadb.jdbc.MariaDbDataSource;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.EnumMap;

/**
 * @author Guillaume Mary
 */
public class MariaDBDataSourceFactory implements IDataSourceFactory<Property> {
	
	@Override
	public UrlAwareDataSource newDataSource(String host, String schema, String user, String password, EnumMap<Property, Object> properties) {
		DataSource delegate = null;
		String url = "jdbc:mariadb://" + host + "/" + schema;
		MariaDbDataSource mariadbDataSource = new MariaDbDataSource();
		try {
			mariadbDataSource.setUrl(url);
		} catch (SQLException e) {
			throw Exceptions.asRuntimeException(e);
		}
		mariadbDataSource.setUser(user);
		mariadbDataSource.setPassword(password);
		delegate = mariadbDataSource;
		return new UrlAwareDataSource(url, delegate);
	}
}
