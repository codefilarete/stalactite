package org.codefilarete.stalactite.sql.statement;

import org.codefilarete.stalactite.sql.ConnectionProvider;

/**
 * @author Guillaume Mary
 */
public class HSQLDBWriteOperation<ParamType> extends WriteOperation<ParamType> {
	
	public HSQLDBWriteOperation(SQLStatement<ParamType> sqlGenerator, ConnectionProvider connectionProvider, RowCountListener rowCountListener) {
		super(sqlGenerator, connectionProvider, rowCountListener);
	}
}
