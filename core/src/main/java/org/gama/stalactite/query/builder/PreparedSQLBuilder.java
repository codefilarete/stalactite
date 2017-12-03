package org.gama.stalactite.query.builder;

import org.gama.sql.binder.ParameterBinderRegistry;
import org.gama.sql.dml.PreparedSQL;

/**
 * @author Guillaume Mary
 */
public interface PreparedSQLBuilder {
	
	PreparedSQL toPreparedSQL(ParameterBinderRegistry parameterBinderRegistry);
}
