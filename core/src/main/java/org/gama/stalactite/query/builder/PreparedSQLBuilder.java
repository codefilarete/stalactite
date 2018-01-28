package org.gama.stalactite.query.builder;

import org.gama.sql.dml.PreparedSQL;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;

/**
 * @author Guillaume Mary
 */
public interface PreparedSQLBuilder {
	
	PreparedSQL toPreparedSQL(ColumnBinderRegistry parameterBinderRegistry);
}
