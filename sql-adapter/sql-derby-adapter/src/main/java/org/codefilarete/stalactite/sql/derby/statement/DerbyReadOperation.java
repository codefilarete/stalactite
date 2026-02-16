package org.codefilarete.stalactite.sql.derby.statement;

import java.sql.SQLException;

import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.jdbc.EmbedStatement;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLStatement;

/**
 * {@link ReadOperation} dedicated to Derby for cancel operation particularity
 */
public class DerbyReadOperation<ParamType> extends ReadOperation<ParamType> {
	
	public DerbyReadOperation(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider) {
		this(sqlGenerator, connectionProvider, null);
	}
	
	public DerbyReadOperation(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider, Integer fetchSize) {
		super(sqlGenerator, connectionProvider, fetchSize);
	}

	/**
	 * Overridden to use Derby special {@link EmbedConnection#cancelRunningStatement()} method
	 * to avoid exception "ERROR 0A000: Feature not implemented: cancel" (see {@link EmbedStatement#cancel()} implementation).
	 * 
	 * @throws SQLException if cancellation fails
	 */
	@Override
	public void cancel() throws SQLException {
		EmbedConnection conn = preparedStatement.getConnection().unwrap(EmbedConnection.class);
		conn.cancelRunningStatement();
	}
}
