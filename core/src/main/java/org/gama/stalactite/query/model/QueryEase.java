package org.gama.stalactite.query.model;

import java.util.Map;

import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.query.model.SelectQuery.FluentSelect;

/**
 * A simple class to avoid "new SelectQuery" syntax cahined with select
 * @author Guillaume Mary
 */
public class QueryEase {
	
	public static FluentSelect select(Object selectable, Object... columns) {
		return new SelectQuery().select(selectable, columns);
	}
	
	public static FluentSelect select(Column column, String alias) {
		return new SelectQuery().select(column, alias);
	}
	
	public static FluentSelect select(Column col1, String alias1, Column col2, String alias2) {
		return new SelectQuery().select(col1, alias1, col2, alias2);
	}
	
	public static FluentSelect select(Column col1, String alias1, Column col2, String alias2, Column col3, String alias3) {
		return new SelectQuery().select(col1, alias1, col2, alias2, col3, alias3);
	}
	
	public static FluentSelect select(Map<Column, String> aliasedColumns) {
		return new SelectQuery().select(aliasedColumns);
	}
	
	public static Where where(Column column, String condition) {
		return new Where(column, condition);
	}
	
	public static Where where(Column column, Operand condition) {
		return new Where(column, condition);
	}
	
	public static Criteria filter(Column column, String condition) {
		return new Criteria(column, condition);
	}
	
	public static Criteria filter(Column column, Operand condition) {
		return new Criteria(column, condition);
	}
	
	public static Criteria filter(Object ... columns) {
		return new Criteria(columns);
	}
}
