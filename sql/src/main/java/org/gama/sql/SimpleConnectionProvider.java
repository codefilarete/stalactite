package org.gama.sql;

import java.sql.Connection;

/**
 * Naïve implementation of {@link ConnectionProvider} that stores the {@link Connection}
 * 
 * @author Guillaume Mary
 */
public class SimpleConnectionProvider implements ConnectionProvider {
	
	private final Connection connection;
	
	public SimpleConnectionProvider(Connection connection) {
		this.connection = connection;
	}
	
	@Override
	public Connection getCurrentConnection() {
		return this.connection;
	}
	
}
