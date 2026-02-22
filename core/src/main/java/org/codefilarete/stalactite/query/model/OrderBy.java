package org.codefilarete.stalactite.query.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.codefilarete.stalactite.query.api.OrderByChain;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.query.model.OrderBy.OrderedColumn;

/**
 * A support for the "order by" clause of a SQL query
 *
 * @author Guillaume Mary
 */
public class OrderBy implements Iterable<OrderedColumn>, OrderByChain<OrderBy> {
	
	/** String, Column */
	private List<OrderedColumn> columns = new ArrayList<>(5);
	
	private OrderBy add(OrderedColumn column) {
		this.columns.add(column);
		return this;
	}
	
	private OrderBy add(Selectable column) {
		return add(new OrderedColumn(column));
	}
	
	private OrderBy add(String column) {
		return add(new OrderedColumn(column));
	}
	
	public List<OrderedColumn> getColumns() {
		return columns;
	}
	
	@Override
	public OrderBy add(Selectable column, Order order) {
		return add(new OrderedColumn(column, order));
	}
	
	@Override
	public OrderBy add(Selectable col1, Order order1, Selectable col2, Order order2) {
		return add(col1, order1).add(col2, order2);
	}
	
	@Override
	public OrderBy add(Selectable col1, Order order1, Selectable col2, Order order2, Selectable col3, Order order3) {
		return add(col1, order1).add(col2, order2).add(col3, order3);
	}
	
	@Override
	public OrderBy add(String column, Order order) {
		return add(new OrderedColumn(column, order));
	}
	
	@Override
	public OrderBy add(String col1, Order order1, String col2, Order order2) {
		return add(col1, order1).add(col2, order2);
	}
	
	@Override
	public OrderBy add(String col1, Order order1, String col2, Order order2, String col3, Order order3) {
		return add(col1, order1).add(col2, order2).add(col3, order3);
	}
	
	@Override
	public OrderBy add(Selectable column, Selectable... columns) {
		add(column);
		for (Selectable col : columns) {
			add(col);
		}
		return this;
	}
	
	@Override
	public OrderBy add(String column, String... columns) {
		add(column);
		for (String col : columns) {
			add(col);
		}
		return this;
	}
	
	@Override
	public Iterator<OrderedColumn> iterator() {
		return this.columns.iterator();
	}
	
	public static class OrderedColumn {
		
		private final Object /* String or Column */ column;
		private final Order order;
		
		public OrderedColumn(Object column) {
			this(column, null);
		}
		
		public OrderedColumn(Object column, Order order) {
			this.column = column;
			this.order = order;
		}
		
		public Object getColumn() {
			return column;
		}
		
		/**
		 * @return maybe null
		 */
		public Order getOrder() {
			return order;
		}
	}
}
