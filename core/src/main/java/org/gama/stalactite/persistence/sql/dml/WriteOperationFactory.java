package org.gama.stalactite.persistence.sql.dml;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.gama.lang.function.ThrowingBiFunction;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.dml.SQLStatement;
import org.gama.stalactite.sql.dml.WriteOperation;

/**
 * As its name mention it, this class is a factory for {@link WriteOperation}, introduced to be overriden for database specific behavior.
 * 
 * @author Guillaume Mary
 */
public class WriteOperationFactory {
	
	public <ParamType> WriteOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider) {
		return new WriteOperation<>(sqlGenerator, connectionProvider);
	}
	
	public <ParamType> WriteOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider,
																ThrowingBiFunction<Connection, String, PreparedStatement, SQLException> statementProvider) {
		return new WriteOperation<ParamType>(sqlGenerator, connectionProvider) {
			@Override
			protected void prepareStatement(Connection connection) throws SQLException {
				this.preparedStatement = statementProvider.apply(connection, getSQL());
			}
		};
	}
}
