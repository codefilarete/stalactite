package org.gama.sql;

import javax.annotation.Nonnull;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Implementation that gives the conneciton from an underlying {@link DataSource}
 * 
 * @author Guillaume Mary
 */
public class DataSourceConnectionProvider implements ConnectionProvider {
	
	private final DataSource dataSource;
	
	public DataSourceConnectionProvider(DataSource dataSource) {
		this.dataSource = dataSource;
	}
	
	@Nonnull
	@Override
	public Connection getCurrentConnection() {
		try {
			return dataSource.getConnection();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
