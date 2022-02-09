package org.codefilarete.stalactite.persistence.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.codefilarete.tool.function.ThrowingBiFunction;
import org.codefilarete.stalactite.persistence.sql.dml.WriteOperationFactory;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.dml.SQLStatement;
import org.codefilarete.stalactite.sql.dml.WriteOperation;
import org.codefilarete.stalactite.sql.dml.WriteOperation.RowCountListener;

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
