package org.codefilarete.stalactite.sql;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * A simple contract to give the eventually existing {@link Connection}
 * 
 * @author Guillaume Mary
 */
public interface ConnectionProvider {
	
	/**
	 * Gives an eventually existing {@link Connection} or opens a new one if it doesn't exist or current one is closed.
	 * 
	 * @return neither null nor a closed connection
	 */
	Connection giveConnection();
	
	/**
	 * Simple {@link ConnectionProvider} based on {@link DataSource#getConnection()}
	 * 
	 * @author Guillaume Mary
	 */
	class DataSourceConnectionProvider implements ConnectionProvider {
		
		private final DataSource dataSource;
		
		public DataSourceConnectionProvider(DataSource dataSource) {
			this.dataSource = dataSource;
		}
		
		@Override
		public Connection giveConnection() {
			try {
				return dataSource.getConnection();
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
