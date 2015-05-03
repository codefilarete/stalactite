package org.gama.stalactite.benchmark.connection;

import java.util.EnumMap;

/**
 * @author Guillaume Mary
 */
public interface IDataSourceFactory<E extends Enum<E>> {
	
	VerboseDataSource newDataSource(String host, String schema, String user, String password, EnumMap<E, Object> properties);
}
