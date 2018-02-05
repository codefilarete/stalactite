package org.gama.stalactite.query.model;

import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.query.model.Query.FluentHaving;

/**
 * @author Guillaume Mary
 */
public interface HavingAware {
	
	FluentHaving having(Column column, String condition);
	
	FluentHaving having(Object... columns);
}
