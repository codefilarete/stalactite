package org.codefilarete.stalactite.query.model;

import org.codefilarete.stalactite.query.model.OrderByChain.Order;
import org.codefilarete.stalactite.query.model.Query.FluentOrderByClause;

/**
 * @author Guillaume Mary
 */
public interface OrderByAware {
	
	FluentOrderByClause orderBy(Selectable<?> column, Order order);
	
	FluentOrderByClause orderBy(Selectable<?> col1, Order order1, Selectable<?> col2, Order order2);
	
	FluentOrderByClause orderBy(Selectable<?> col1, Order order1, Selectable<?> col2, Order order2, Selectable<?> col3, Order order3);
	
	FluentOrderByClause orderBy(String column, Order order);
	
	FluentOrderByClause orderBy(String col1, Order order1, String col2, Order order2);
	
	FluentOrderByClause orderBy(String col1, Order order1, String col2, Order order2, String col3, Order order3);
	
	FluentOrderByClause orderBy(Selectable<?> column, Selectable<?>... columns);
	
	FluentOrderByClause orderBy(String column, String... columns);
	
}
