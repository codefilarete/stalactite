package org.gama.stalactite.query.model;

import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.query.model.SelectQuery.FluentHaving;

/**
 * @author Guillaume Mary
 */
public interface HavingAware {
	
	FluentHaving having(Column column, String condition);
	
	FluentHaving having(Object... columns);
}
