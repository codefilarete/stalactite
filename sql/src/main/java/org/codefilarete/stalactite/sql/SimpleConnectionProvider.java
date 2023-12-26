package org.codefilarete.stalactite.sql;

import java.sql.Connection;

/**
 * Naïve implementation of {@link ConnectionProvider} that stores the {@link Connection}
 * Please note that as a difference of expected behavior given by {@link ConnectionProvider} contract, this class
 * doesn't handle closed {@link Connection} : it always returns initial one even if closed, because it's purpose is
 * either to mark a {@link Connection} as a "unit of work", either for test 
 * 
 * @author Guillaume Mary
 */
public class SimpleConnectionProvider implements ConnectionProvider {
	
	private final Connection connection;
	
	public SimpleConnectionProvider(Connection connection) {
		this.connection = connection;
	}
	
	/**
	 * Gives connection given at construction time
	 * 
	 * @return connection given at construction time
	 */
	@Override
	public Connection giveConnection() {
		return this.connection;
	}
	
}
