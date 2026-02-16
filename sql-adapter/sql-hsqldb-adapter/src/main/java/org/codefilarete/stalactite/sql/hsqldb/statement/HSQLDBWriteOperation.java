package org.codefilarete.stalactite.sql.hsqldb.statement;

import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.statement.SQLStatement;
import org.codefilarete.stalactite.sql.statement.WriteOperation;

/**
 * @author Guillaume Mary
 */
public class HSQLDBWriteOperation<ParamType> extends WriteOperation<ParamType> {
	
	public HSQLDBWriteOperation(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider, RowCountListener rowCountListener) {
		super(sqlGenerator, connectionProvider, rowCountListener);
	}
}
