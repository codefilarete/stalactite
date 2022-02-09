package org.codefilarete.stalactite.query.model;

import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.query.model.Query.FluentHaving;

/**
 * @author Guillaume Mary
 */
public interface HavingAware {
	
	FluentHaving having(Column column, String condition);
	
	FluentHaving having(Object... columns);
}
