package org.codefilarete.stalactite.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.stalactite.sql.statement.WriteOperation.RowCountListener;
import org.codefilarete.stalactite.sql.statement.WriteOperationFactory;
import org.codefilarete.tool.function.ThrowingBiFunction;

/**
 * {@link WriteOperationFactory} dedicated to MySQL. In particular adds a {@link InnoDBLockRetryer} to created {@link WriteOperation}s
 *
 * @author Guillaume Mary
 */
public class MySQLWriteOperationFactory extends WriteOperationFactory {
	
	/** Instance that helps to retry update statements on error, should not be null */
	private final InnoDBLockRetryer retryer;
	
	public MySQLWriteOperationFactory() {
		this.retryer = new InnoDBLockRetryer();
	}
	
	@Override
	protected <ParamType> WriteOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator,
																   ConnectionProvider connectionProvider,
																   ThrowingBiFunction<Connection, String, PreparedStatement, SQLException> statementProvider,
																   RowCountListener rowCountListener) {
		return new MySQLWriteOperation<>(sqlGenerator, connectionProvider, rowCountListener, retryer, statementProvider);
	}
	
}
