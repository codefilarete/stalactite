package org.codefilarete.stalactite.query.model;

import java.util.Map;

import org.codefilarete.stalactite.query.api.Fromable;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.query.builder.WhereSQLBuilderFactory.WhereSQLBuilder;
import org.codefilarete.stalactite.query.model.Query.FluentSelectClause;
import org.codefilarete.stalactite.query.api.Selectable.SimpleSelectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;

/**
 * A simple class to avoid "new Query()" syntax chained with {@link Query#select(Selectable, String)}
 * 
 * @author Guillaume Mary
 */
public class QueryEase {
	
	public static FluentSelectClause select(Iterable<? extends Selectable<?>> selectables) {
		return new Query().select(selectables);
	}
	
	public static <C> FluentSelectClause select(String expression, Class<C> javaType) {
		return new Query().select(expression, javaType);
	}
	
	public static FluentSelectClause select(Selectable<?> expression, Selectable<?>... expressions) {
		return new Query().select(expression, expressions);
	}
	
	public static FluentSelectClause select(Selectable<?> column, String alias) {
		return new Query().select(column, alias);
	}
	
	public static FluentSelectClause select(Selectable<?> col1, String alias1, Selectable<?> col2, String alias2) {
		return new Query().select(col1, alias1, col2, alias2);
	}
	
	public static FluentSelectClause select(Selectable<?> col1, String alias1, Selectable<?> col2, String alias2, Selectable<?> col3, String alias3) {
		return new Query().select(col1, alias1, col2, alias2, col3, alias3);
	}
	
	public static FluentSelectClause select(Map<? extends Selectable<?>, String> aliasedColumns) {
		return new Query().select(aliasedColumns);
	}
	
	public static FluentSelectClause from(Fromable rootTable) {
		return new Query().from(rootTable).getQuery().getSelect();
	}
	
	public static Where<?> where(Selectable<?> column, String condition) {
		return new Where<>(column, condition);
	}
	
	public static <O> Where<?> where(Selectable<O> column, ConditionalOperator<? super O, ?> condition) {
		return new Where<>(column, condition);
	}
	
	public static Where<?> where(Object... criteria) {
		return new Where<>(criteria);
	}
	
	/**
	 * Shortcut to create a {@link Criteria}.
	 * Combined with {@link WhereSQLBuilder} it will add parenthesis around it.
	 * 
	 * @param column a {@link Column}
	 * @param condition the criteria on the {@link Column}
	 * @return a new {@link Criteria}
	 */
	public static Criteria<?> filter(Selectable<?> column, String condition) {
		return new Criteria<>(column, condition);
	}
	
	/**
	 * Shortcut to create a {@link Criteria}.
	 * Combined with {@link WhereSQLBuilder} it will add parenthesis around it.
	 *
	 * @param column a {@link Column}
	 * @param condition the criteria on the {@link Column}
	 * @return a new {@link Criteria}
	 */
	public static Criteria<?> filter(Selectable<?> column, ConditionalOperator condition) {
		return new Criteria<>(column, condition);
	}
	
	/**
	 * Shortcut to create a {@link Criteria}.
	 * Combined with {@link WhereSQLBuilder} it will add parenthesis around it.
	 *
	 * @param columns a combination of objects describing the criteria
	 * @return a new {@link Criteria}
	 */
	public static Criteria<?> filter(Object ... columns) {
		return new Criteria<>(columns);
	}
	
	public static <C> Selectable<Object> column(String name) {
		return new SimpleSelectable<>(name, Object.class);
	}
}
