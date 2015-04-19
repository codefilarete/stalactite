package org.stalactite.benchmark.connection;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.sql.DataSource;

import org.mariadb.jdbc.MySQLDataSource;
import org.stalactite.benchmark.connection.MySQLDataSourceFactory.Property;

/**
 * @author Guillaume Mary
 */
public class MariaDBDataSourceFactory implements IDataSourceFactory<Property> {
	
	@Override
	public VerboseDataSource newDataSource(String host, String schema, String user, String password, EnumMap<Property, Object> properties) {
		DataSource delegate = null;
		String url = "jdbc:mariadb://" + host + "/" + schema;
		MySQLDataSource mariadbDataSource = new MySQLDataSource();
		mariadbDataSource.setUrl(url);
		mariadbDataSource.setUser(user);
		mariadbDataSource.setPassword(password);
		delegate = mariadbDataSource;
		return new VerboseDataSource(url, toString(properties), delegate);
	}
	
	private Map<String, String> toString(EnumMap<Property, Object> properties) {
		Map<String, String> result = new HashMap<>(properties.size());
		for (Entry<Property, Object> entry : properties.entrySet()) {
			result.put(entry.getKey().name(), String.valueOf(entry.getValue()));
		}
		return result;
	}
}
