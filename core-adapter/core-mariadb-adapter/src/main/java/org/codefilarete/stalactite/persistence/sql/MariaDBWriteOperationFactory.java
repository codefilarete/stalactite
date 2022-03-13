package org.codefilarete.stalactite.persistence.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.codefilarete.tool.function.ThrowingBiFunction;
import org.codefilarete.stalactite.persistence.sql.statement.WriteOperationFactory;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.sql.statement.WriteOperation;
import org.codefilarete.stalactite.sql.statement.WriteOperation.RowCountListener;

/**
 * {@link WriteOperationFactory} dedicated to MariaDB. In particular adds a {@link InnoDBLockRetryer} to created {@link WriteOperation}s
 * 
 * @author Guillaume Mary
 */
public class MariaDBWriteOperationFactory extends WriteOperationFactory {
	
	/** Instance that helps to retry update statements on error, should not be null */
	private final InnoDBLockRetryer retryer;
	
	public MariaDBWriteOperationFactory() {
		this.retryer = new InnoDBLockRetryer();
	}
	
	@Override
	protected <ParamType> WriteOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator,
																   ConnectionProvider connectionProvider,
																   ThrowingBiFunction<Connection, String, PreparedStatement, SQLException> statementProvider,
																   RowCountListener rowCountListener) {
		return new MariaDBWriteOperation<>(sqlGenerator, connectionProvider, rowCountListener, retryer, statementProvider);
	}
}
