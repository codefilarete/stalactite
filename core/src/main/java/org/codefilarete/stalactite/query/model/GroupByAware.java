package org.codefilarete.stalactite.query.model;

import org.codefilarete.stalactite.persistence.sql.ddl.structure.Column;
import org.codefilarete.stalactite.query.model.Query.FluentGroupBy;

/**
 * @author Guillaume Mary
 */
public interface GroupByAware {
	
	FluentGroupBy groupBy(Column column, Column... columns);
	
	FluentGroupBy groupBy(String column, String... columns);
}
