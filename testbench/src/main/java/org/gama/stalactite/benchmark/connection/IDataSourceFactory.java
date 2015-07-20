package org.gama.stalactite.benchmark.connection;

import org.gama.sql.UrlAwareDataSource;

import java.util.EnumMap;

/**
 * @author Guillaume Mary
 */
public interface IDataSourceFactory<E extends Enum<E>> {
	
	UrlAwareDataSource newDataSource(String host, String schema, String user, String password, EnumMap<E, Object> properties);
}
