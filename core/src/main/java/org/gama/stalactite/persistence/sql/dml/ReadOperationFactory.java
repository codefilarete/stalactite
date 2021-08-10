package org.gama.stalactite.persistence.sql.dml;

import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.dml.ReadOperation;
import org.gama.stalactite.sql.dml.SQLStatement;

/**
 * As its name mention it, this class is a factory for {@link ReadOperation}, introduced to be overriden for database specific behavior
 * 
 * @author Guillaume Mary
 */
public class ReadOperationFactory {
	
	public <ParamType>ReadOperation<ParamType> createInstance(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider) {
		return new ReadOperation<>(sqlGenerator, connectionProvider);
	}
}
