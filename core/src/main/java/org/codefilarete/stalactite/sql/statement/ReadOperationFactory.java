package org.codefilarete.stalactite.sql.statement;

import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.statement.ReadOperation;
import org.codefilarete.stalactite.sql.statement.SQLStatement;

/**
 * As its name mention it, this class is a factory for {@link ReadOperation}, introduced to be overridden for database specific behavior
 * 
 * @author Guillaume Mary
 */
public class ReadOperationFactory {
	
	public <ParamType>ReadOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider) {
		return new ReadOperation<>(sqlGenerator, connectionProvider);
	}
}
