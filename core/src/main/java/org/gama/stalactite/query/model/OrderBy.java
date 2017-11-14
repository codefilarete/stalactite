package org.gama.stalactite.query.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * A support for the "order by" clause of a SQL query
 *
 * @author Guillaume Mary
 */
public class OrderBy implements Iterable<Object /* String, Column */>, OrderByChain<OrderBy> {
	
	/** String, Column */
	private List<Object> columns = new ArrayList<>(5);
	
	private OrderBy add(Object column) {
		this.columns.add(column);
		return this;
	}
	
	public List<Object> getColumns() {
		return columns;
	}
	
	@Override
	public OrderBy add(Column column, Column... columns) {
		add(column);
		for (Column col : columns) {
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
	public Iterator<Object> iterator() {
		return this.columns.iterator();
	}
	
}
