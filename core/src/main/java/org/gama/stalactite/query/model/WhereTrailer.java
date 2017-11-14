package org.gama.stalactite.query.model;

import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.query.model.SelectQuery.FluentGroupBy;

/**
 * @author Guillaume Mary
 */
public interface WhereTrailer {
	
	FluentGroupBy groupBy(Column column, Column... columns);
	
	FluentGroupBy groupBy(String column, String... columns);
}
