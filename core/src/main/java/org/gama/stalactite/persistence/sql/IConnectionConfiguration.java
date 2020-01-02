package org.gama.stalactite.persistence.sql;

import org.gama.stalactite.sql.ConnectionProvider;

/**
 * An abstraction for current JDBC connection configuration 
 * 
 * @author Guillaume Mary
 */
public interface IConnectionConfiguration {
	
	ConnectionProvider getConnectionProvider();
	
	int getBatchSize();
	
	/**
	 * Default implementation of {@link IConnectionConfiguration} that keeps and gives values provided at instanciation time
	 * 
	 * @author Guillaume Mary
	 */
	class ConnectionConfigurationSupport implements IConnectionConfiguration {
		
		private final ConnectionProvider connectionProvider;
		private final int batchSize;
		
		public ConnectionConfigurationSupport(ConnectionProvider connectionProvider, int batchSize) {
			this.connectionProvider = connectionProvider;
			this.batchSize = batchSize;
		}
		
		@Override
		public ConnectionProvider getConnectionProvider() {
			return connectionProvider;
		}
		
		@Override
		public int getBatchSize() {
			return batchSize;
		}
	}
}
