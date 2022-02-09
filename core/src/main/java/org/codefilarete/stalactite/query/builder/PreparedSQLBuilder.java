package org.codefilarete.stalactite.query.builder;

import org.codefilarete.stalactite.sql.dml.PreparedSQL;
import org.codefilarete.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;

/**
 * @author Guillaume Mary
 */
public interface PreparedSQLBuilder {
	
	PreparedSQL toPreparedSQL(ColumnBinderRegistry parameterBinderRegistry);
}
