package org.codefilarete.stalactite.query.model;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.query.model.OrderByChain.Order;
import org.codefilarete.stalactite.query.model.Query.FluentOrderByClause;

/**
 * @author Guillaume Mary
 */
public interface OrderByAware {
	
	FluentOrderByClause orderBy(Column column, Order order);
	
	FluentOrderByClause orderBy(Column col1, Order order1, Column col2, Order order2);
	
	FluentOrderByClause orderBy(Column col1, Order order1, Column col2, Order order2, Column col3, Order order3);
	
	FluentOrderByClause orderBy(String column, Order order);
	
	FluentOrderByClause orderBy(String col1, Order order1, String col2, Order order2);
	
	FluentOrderByClause orderBy(String col1, Order order1, String col2, Order order2, String col3, Order order3);
	
	FluentOrderByClause orderBy(Column column, Column... columns);
	
	FluentOrderByClause orderBy(String column, String... columns);
	
}
