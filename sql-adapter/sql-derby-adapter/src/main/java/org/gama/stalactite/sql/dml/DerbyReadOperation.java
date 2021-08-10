package org.gama.stalactite.sql.dml;

import java.sql.SQLException;

import org.apache.derby.impl.jdbc.EmbedConnection;
import org.gama.stalactite.sql.ConnectionProvider;

/**
 * {@link ReadOperation} dedicated to Derby for cancel operation particularity
 */
public class DerbyReadOperation<ParamType> extends ReadOperation<ParamType> {
	
	public DerbyReadOperation(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider) {
		super(sqlGenerator, connectionProvider);
	}
	
	/**
	 * Overriden to use Derby special {@link EmbedConnection#cancelRunningStatement()} method
	 * 
	 * @throws SQLException if cancellation fails
	 */
	@Override
	public void cancel() throws SQLException {
		final EmbedConnection conn = preparedStatement.getConnection().unwrap(EmbedConnection.class);
		conn.cancelRunningStatement();
	}
}
