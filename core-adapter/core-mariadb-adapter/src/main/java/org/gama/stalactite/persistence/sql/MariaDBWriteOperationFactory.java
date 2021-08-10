package org.gama.stalactite.persistence.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.gama.lang.Retryer;
import org.gama.lang.function.ThrowingBiFunction;
import org.gama.stalactite.persistence.sql.dml.WriteOperationFactory;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.dml.SQLStatement;
import org.gama.stalactite.sql.dml.WriteOperation;

/**
 * @author Guillaume Mary
 */
public class MariaDBWriteOperationFactory extends WriteOperationFactory {
	
	/** Instance that helps to retry update statements on error, should not be null */
	private final Retryer retryer;
	
	public MariaDBWriteOperationFactory() {
		this(new MariaDBRetryer());
	}
	
	public MariaDBWriteOperationFactory(Retryer retryer) {
		this.retryer = retryer;
	}
	
	@Override
	public <ParamType> WriteOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider) {
		return new MariaDBWriteOperation<>(sqlGenerator, connectionProvider, retryer);
	}
	
	@Override
	public <ParamType> WriteOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider,
																ThrowingBiFunction<Connection, String, PreparedStatement, SQLException> statementProvider) {
		return new MariaDBWriteOperation<>(sqlGenerator, connectionProvider, retryer, statementProvider);
	}
}
