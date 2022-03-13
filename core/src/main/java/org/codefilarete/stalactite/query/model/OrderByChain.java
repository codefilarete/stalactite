package org.codefilarete.stalactite.query.model;

import org.codefilarete.stalactite.sql.ddl.structure.Column;

/**
 * @author Guillaume Mary
 */
public interface OrderByChain<SELF extends OrderByChain<SELF>> {
	
	SELF add(Column column, Order order);
	
	SELF add(Column col1, Order order1, Column col2, Order order2);
	
	SELF add(Column col1, Order order1, Column col2, Order order2, Column col3, Order order3);
	
	SELF add(String column, Order order);
	
	SELF add(String col1, Order order1, String col2, Order order2);
	
	SELF add(String col1, Order order1, String col2, Order order2, String col3, Order order3);
	
	SELF add(Column column, Column... columns);
	
	SELF add(String column, String... columns);
	
	enum Order {
		ASC,
		DESC
	}
	
}
