package org.gama.stalactite.query.model;

import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.query.model.OrderByChain.Order;
import org.gama.stalactite.query.model.Query.FluentOrderBy;

/**
 * @author Guillaume Mary
 */
public interface OrderByAware {
	
	FluentOrderBy orderBy(Column column, Order order);
	
	FluentOrderBy orderBy(Column col1, Order order1, Column col2, Order order2);
	
	FluentOrderBy orderBy(Column col1, Order order1, Column col2, Order order2, Column col3, Order order3);
	
	FluentOrderBy orderBy(String column, Order order);
	
	FluentOrderBy orderBy(String col1, Order order1, String col2, Order order2);
	
	FluentOrderBy orderBy(String col1, Order order1, String col2, Order order2, String col3, Order order3);
	
	FluentOrderBy orderBy(Column column, Column... columns);
	
	FluentOrderBy orderBy(String column, String... columns);
	
}
