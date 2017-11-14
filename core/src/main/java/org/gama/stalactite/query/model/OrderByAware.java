package org.gama.stalactite.query.model;

import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.query.model.SelectQuery.FluentOrderBy;

/**
 * @author Guillaume Mary
 */
public interface OrderByAware {
	
	FluentOrderBy orderBy(Column column, Column... columns);
	
	FluentOrderBy orderBy(String column, String... columns);
	
}
