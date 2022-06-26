package org.codefilarete.stalactite.query.model;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.query.model.Query.FluentGroupByClause;

/**
 * @author Guillaume Mary
 */
public interface GroupByAware {
	
	FluentGroupByClause groupBy(Column column, Column... columns);
	
	FluentGroupByClause groupBy(String column, String... columns);
}
