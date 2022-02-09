package org.codefilarete.stalactite.query.model;

import java.util.Map;

import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.query.model.Query.FluentSelect;

/**
 * A simple class to avoid "new Query()" syntax chained with {@link Query#select(Column, String)}
 * 
 * @author Guillaume Mary
 */
public class QueryEase {
	
	public static FluentSelect select(Iterable<?> selectables) {
		return new Query().select(selectables);
	}
	
	public static FluentSelect select(Object selectable, Object... columns) {
		return new Query().select(selectable, columns);
	}
	
	public static FluentSelect select(Column column, String alias) {
		return new Query().select(column, alias);
	}
	
	public static FluentSelect select(Column col1, String alias1, Column col2, String alias2) {
		return new Query().select(col1, alias1, col2, alias2);
	}
	
	public static FluentSelect select(Column col1, String alias1, Column col2, String alias2, Column col3, String alias3) {
		return new Query().select(col1, alias1, col2, alias2, col3, alias3);
	}
	
	public static FluentSelect select(Map<Column, String> aliasedColumns) {
		return new Query().select(aliasedColumns);
	}
	
	public static Where where(Column column, String condition) {
		return new Where(column, condition);
	}
	
	public static Where where(Column column, AbstractRelationalOperator condition) {
		return new Where(column, condition);
	}
	
	/**
	 * Shortcut to create a {@link Criteria}.
	 * Combined with {@link org.codefilarete.stalactite.query.builder.WhereBuilder} it will add parenthesis around it.
	 * 
	 * @param column a {@link Column}
	 * @param condition the criteria on the {@link Column}
	 * @return a new {@link Criteria}
	 */
	public static Criteria filter(Column column, String condition) {
		return new Criteria(column, condition);
	}
	
	/**
	 * Shortcut to create a {@link Criteria}.
	 * Combined with {@link org.codefilarete.stalactite.query.builder.WhereBuilder} it will add parenthesis around it.
	 *
	 * @param column a {@link Column}
	 * @param condition the criteria on the {@link Column}
	 * @return a new {@link Criteria}
	 */
	public static Criteria filter(Column column, AbstractRelationalOperator condition) {
		return new Criteria(column, condition);
	}
	
	/**
	 * Shortcut to create a {@link Criteria}.
	 * Combined with {@link org.codefilarete.stalactite.query.builder.WhereBuilder} it will add parenthesis around it.
	 *
	 * @param columns a combination of objects describing the criteria
	 * @return a new {@link Criteria}
	 */
	public static Criteria filter(Object ... columns) {
		return new Criteria(columns);
	}
}
