package org.codefilarete.stalactite.query.model;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.query.model.Query.FluentHavingClause;

/**
 * @author Guillaume Mary
 */
public interface HavingAware {
	
	FluentHavingClause having(Column column, String condition);
	
	FluentHavingClause having(Object... columns);
}
