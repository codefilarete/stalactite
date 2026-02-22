package org.codefilarete.stalactite.query.api;

/**
 * @author Guillaume Mary
 */
public interface OrderByChain<SELF extends OrderByChain<SELF>> {
	
	SELF add(Selectable column, Order order);
	
	SELF add(Selectable col1, Order order1, Selectable col2, Order order2);
	
	SELF add(Selectable col1, Order order1, Selectable col2, Order order2, Selectable col3, Order order3);
	
	SELF add(String column, Order order);
	
	SELF add(String col1, Order order1, String col2, Order order2);
	
	SELF add(String col1, Order order1, String col2, Order order2, String col3, Order order3);
	
	SELF add(Selectable column, Selectable... columns);
	
	SELF add(String column, String... columns);
	
	enum Order {
		ASC,
		DESC
	}
	
}
