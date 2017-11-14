package org.gama.stalactite.query.model;

import java.util.Map;

import org.gama.stalactite.persistence.structure.Table.Column;
import org.gama.stalactite.query.model.SelectQuery.FluentSelect;

/**
 * A simple class to avoid "new SelectQuery" syntax cahined with select
 * @author Guillaume Mary
 */
public class QueryEase {
	
	public static FluentSelect select(String selectable) {
		return new SelectQuery().select(selectable);
	}
	
	public static FluentSelect select(Column column) {
		return new SelectQuery().select(column);
	}
	
	public static FluentSelect select(Column... columns) {
		return new SelectQuery().select(columns);
	}
	
	public static FluentSelect select(String... columns) {
		return new SelectQuery().select(columns);
	}
	
	public static FluentSelect select(Column column, String alias) {
		return new SelectQuery().select(column, alias);
	}
	
	public static FluentSelect select(Map<Column, String> aliasedColumns) {
		return new SelectQuery().select(aliasedColumns);
	}
	
	public static Where where(Column column, CharSequence condition) {
		return new Where(column, condition);
	}
	
	public static Criteria filter(Column column, CharSequence condition) {
		return new Criteria(column, condition);
	}
	
	public static Criteria filter(Object ... columns) {
		return new Criteria(columns);
	}
}
