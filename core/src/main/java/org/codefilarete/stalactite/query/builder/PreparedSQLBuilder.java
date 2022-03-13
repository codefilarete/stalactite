package org.codefilarete.stalactite.query.builder;

import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.persistence.sql.statement.binder.ColumnBinderRegistry;

/**
 * @author Guillaume Mary
 */
public interface PreparedSQLBuilder {
	
	PreparedSQL toPreparedSQL(ColumnBinderRegistry parameterBinderRegistry);
}
