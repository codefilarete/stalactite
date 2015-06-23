package org.gama.sql;

import java.sql.Connection;

/**
 * Naïve implementation of {@link IConnectionProvider} that stores the {@link Connection}
 * 
 * @author Guillaume Mary
 */
public class SimpleConnectionProvider implements IConnectionProvider {
	
	private final Connection connection;
	
	public SimpleConnectionProvider(Connection connection) {
		this.connection = connection;
	}
	
	@Override
	public Connection getConnection() {
		return this.connection;
	}
	
}
