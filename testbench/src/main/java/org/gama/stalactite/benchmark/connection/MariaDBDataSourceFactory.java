package org.gama.stalactite.benchmark.connection;

import org.gama.sql.UrlAwareDataSource;
import org.gama.stalactite.benchmark.connection.MySQLDataSourceFactory.Property;
import org.mariadb.jdbc.MySQLDataSource;

import javax.sql.DataSource;
import java.util.EnumMap;

/**
 * @author Guillaume Mary
 */
public class MariaDBDataSourceFactory implements IDataSourceFactory<Property> {
	
	@Override
	public UrlAwareDataSource newDataSource(String host, String schema, String user, String password, EnumMap<Property, Object> properties) {
		DataSource delegate = null;
		String url = "jdbc:mariadb://" + host + "/" + schema;
		MySQLDataSource mariadbDataSource = new MySQLDataSource();
		mariadbDataSource.setUrl(url);
		mariadbDataSource.setUser(user);
		mariadbDataSource.setPassword(password);
		delegate = mariadbDataSource;
		return new UrlAwareDataSource(url, delegate);
	}
}
