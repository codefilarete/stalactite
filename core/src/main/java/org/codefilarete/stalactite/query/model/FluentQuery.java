package org.codefilarete.stalactite.query.model;

import java.util.Map;

import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.stalactite.query.api.CriteriaChain;
import org.codefilarete.stalactite.query.api.FromAware;
import org.codefilarete.stalactite.query.api.Fromable;
import org.codefilarete.stalactite.query.api.GroupByAware;
import org.codefilarete.stalactite.query.api.GroupByChain;
import org.codefilarete.stalactite.query.api.HavingAware;
import org.codefilarete.stalactite.query.api.JoinChain;
import org.codefilarete.stalactite.query.api.JoinLink;
import org.codefilarete.stalactite.query.api.LimitAware;
import org.codefilarete.stalactite.query.api.LimitChain;
import org.codefilarete.stalactite.query.api.OrderByAware;
import org.codefilarete.stalactite.query.api.OrderByChain;
import org.codefilarete.stalactite.query.api.QueryProvider;
import org.codefilarete.stalactite.query.api.QueryStatement;
import org.codefilarete.stalactite.query.api.SelectChain;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.query.api.UnionAware;
import org.codefilarete.stalactite.query.api.WhereAware;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.SerializableTriFunction;
import org.codefilarete.tool.reflect.MethodDispatcher;
import org.danekja.java.util.function.serializable.SerializableBiFunction;

import static org.codefilarete.stalactite.query.model.FluentQuery.FluentSelectClause;
import static org.codefilarete.stalactite.query.model.Query.FluentFromClause;
import static org.codefilarete.stalactite.query.model.Query.FluentGroupByClause;
import static org.codefilarete.stalactite.query.model.Query.FluentHavingClause;
import static org.codefilarete.stalactite.query.model.Query.FluentLimitClause;
import static org.codefilarete.stalactite.query.model.Query.FluentOrderByClause;
import static org.codefilarete.stalactite.query.model.Query.FluentWhereClause;

/**
 * Represents a fluent API for building SQL-like queries. It allows developers to construct and manage various parts
 * of a query such as SELECT, FROM, WHERE, HAVING, GROUP BY, ORDER BY, and LIMIT clauses using a cohesive and chainable API.
 * 
 * The API tries to be as closest as possible to a real select query syntax and implements the most simple/common usage. 
 * Meanwhile, no syntax validation is done.
 * Final printing can be made by {@link org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder}
 * 
 * Final {@link Query} can be obtained by calling {@link #getQuery()} method, however, thanks to implementing
 * the {@link QueryProvider} interface, most of the time the user shouldn't have to use it due to compatible consuming
 * methods.
 * 
 * Designed as a wrapper of an {@link Query}.
 * @author Guillaume Mary
 */
public class FluentQuery implements
		SelectAware<FluentSelectClause>,
		FromAware,
		WhereAware,
		HavingAware,
		OrderByAware,
		LimitAware<FluentLimitClause>,
		QueryProvider<Query>,
		QueryStatement,
		UnionAware {
	
	private final Query query;
	
	public FluentQuery() {
		this(new Query());
	}
	
	public FluentQuery(Query query) {
		this.query = query;
	}
	
	private FluentSelectClause wrapAsSelectClause() {
		Select delegate = this.query.getSelect();
		return new MethodReferenceDispatcher()
				.redirect(SelectChain.class, delegate, true)
				.redirect(SelectAware.class, this)
				.redirect((SerializableTriFunction<FluentSelectClause, String, Class, SelectAwareAliasExpression>)
						FluentSelectClause::add, (expression, aClass) -> {
					// we must return a proxy that will be capable of handling the "as" method invocation on its own
					// calls to the add(String, Class) method
					FluentSelectClause fallback = wrapAsSelectClause();
					delegate.add(expression, aClass);
					SelectAwareAliasExpression build = new MethodDispatcher()
							.redirect(SelectChain.Aliasable.class, alias -> {
								Selectable<?> column = delegate.findColumn(expression);
								delegate.setAlias(column, alias);
								return null;    // we don't care about the returned object since the proxy is returned
							}, true)
							.fallbackOn(fallback)
							.build(SelectAwareAliasExpression.class);
					return build;
				})
				.redirect((SerializableBiFunction<FluentSelectClause, Selectable, SelectAwareAliasExpression>)
						FluentSelectClause::add, (selectable) -> {
					// we must return a proxy that will be capable of handling the "as" method invocation on its own
					// calls to the add(String, Class) method
					FluentSelectClause fallback = wrapAsSelectClause();
					delegate.add(selectable);
					SelectAwareAliasExpression build = new MethodDispatcher()
							.redirect(SelectChain.Aliasable.class, alias -> {
								delegate.setAlias(selectable, alias);
								return null;    // we don't care about the returned object since the proxy is returned
							}, true)
							.fallbackOn(fallback)
							.build(SelectAwareAliasExpression.class);
					return build;
				})
				.redirect(FromAware.class, this)
				.redirect(WhereAware.class, this)
				.redirect(QueryProvider.class, this)
				.build(FluentSelectClause.class);
	}
	
	private FluentFromClause wrapAsFromCause() {
		return new MethodDispatcher()
				.redirect(JoinChain.class, this.query.getFrom(), true)
				.redirect(WhereAware.class, this)
				.redirect(GroupByAware.class, this)
				.redirect(OrderByAware.class, this)
				.redirect(LimitAware.class, this)
				.redirect(QueryProvider.class, this)
				.build(Query.FluentFromClause.class);
	}
	
	private FluentWhereClause wrapAsWhereClause() {
		Where delegate = this.query.getWhere();
		return new MethodDispatcher()
				.redirect(CriteriaChain.class, delegate, true)
				.redirect(Iterable.class, delegate)
				.redirect(GroupByAware.class, this)
				.redirect(OrderByAware.class, this)
				.redirect(LimitAware.class, this)
				.redirect(QueryProvider.class, this)
				.redirect(UnionAware.class, this)
				.build(Query.FluentWhereClause.class);
	}
	
	private FluentGroupByClause wrapAsGroupByClause() {
		return new MethodDispatcher()
				.redirect(GroupByChain.class, this.query.getGroupBy(), true)
				.redirect(HavingAware.class, this)
				.redirect(OrderByAware.class, this)
				.redirect(LimitAware.class, this)
				.redirect(QueryProvider.class, this)
				.redirect(UnionAware.class, this)
				.build(Query.FluentGroupByClause.class);
	}
	
	private FluentHavingClause wrapAsHavingClause() {
		return new MethodDispatcher()
				.redirect(CriteriaChain.class, this.query.getHaving(), true)
				.redirect(OrderByAware.class, this)
				.redirect(LimitAware.class, this)
				.redirect(QueryProvider.class, this)
				.redirect(UnionAware.class, this)
				.build(Query.FluentHavingClause.class);
	}
	
	private FluentOrderByClause wrapAsOrderByClause() {
		return new MethodDispatcher()
				.redirect(OrderByChain.class, this.query.getOrderBy(), true)
				.redirect(LimitAware.class, this)
				.redirect(QueryProvider.class, this)
				.redirect(UnionAware.class, this)
				.build(Query.FluentOrderByClause.class);
	}
	
	private FluentLimitClause wrapAsLimitClause() {
		return new MethodDispatcher()
				.redirect(LimitChain.class, this.query.getLimit(), true)
				.redirect(QueryProvider.class, this)
				.redirect(UnionAware.class, this)
				.build(Query.FluentLimitClause.class);
	}
	
	
	@Override
	public KeepOrderSet<Selectable<?>> getColumns() {
		return this.query.getColumns();
	}
	
	@Override
	public Map<Selectable<?>, String> getAliases() {
		return this.query.getAliases();
	}
	
	@Override
	public FluentSelectClause select(Iterable<? extends Selectable<?>> selectables) {
		return wrapAsSelectClause().add(selectables);
	}
	
	@Override
	public SelectAwareAliasExpression select(Selectable<?> expression) {
		return wrapAsSelectClause().add(expression);
	}
	
	@Override
	public FluentSelectClause select(Selectable<?> expression, Selectable<?>... expressions) {
		return wrapAsSelectClause().add(expression, expressions);
	}
	
	@Override
	public SelectAwareAliasExpression select(String expression, Class<?> javaType) {
		Select select = this.query.getSelect().add(expression, javaType);
		return new MethodDispatcher()
				.redirect(SelectChain.Aliasable.class, alias -> {
					Selectable<?> column = select.findColumn(expression);
					select.getAliases().put(column, alias);
					return null;    // we don't care about returned object since proxy is returned
				}, true)
				.fallbackOn(this)
				.build(SelectAwareAliasExpression.class);
	}
	
	@Override
	public FluentSelectClause select(String expression, Class<?> javaType, String alias) {
		return select(expression, javaType).as(alias);
	}
	
	@Override
	public FluentSelectClause select(Selectable<?> column, String alias) {
		return wrapAsSelectClause().add(column, alias);
	}
	
	@Override
	public FluentSelectClause select(Selectable<?> col1, String alias1, Selectable<?> col2, String alias2) {
		return wrapAsSelectClause().add(col1, alias1, col2, alias2);
	}
	
	@Override
	public FluentSelectClause select(Selectable<?> col1, String alias1, Selectable<?> col2, String alias2, Selectable<?> col3, String alias3) {
		return wrapAsSelectClause().add(col1, alias1, col2, alias2, col3, alias3);
	}
	
	@Override
	public FluentSelectClause select(Map<? extends Selectable<?>, String> aliasedColumns) {
		return wrapAsSelectClause().add(aliasedColumns);
	}
	
	@Override
	public FluentFromClause from(Fromable leftTable) {
		this.query.getFrom().setRoot(leftTable);
		return wrapAsFromCause();
	}
	
	@Override
	public FluentFromClause from(QueryProvider<?> query, String alias) {
		this.query.getFrom().setRoot(query.getQuery().asPseudoTable(), alias);
		return wrapAsFromCause();
	}
	
	@Override
	public FluentFromClause from(Fromable leftTable, String tableAlias) {
		this.query.getFrom().setRoot(leftTable, tableAlias);
		return wrapAsFromCause();
	}
	
	@Override
	public FluentFromClause from(Fromable leftTable, Fromable rightTable, String joinCondition) {
		return from(leftTable).innerJoin(rightTable, joinCondition);
	}
	
	@Override
	public FluentFromClause from(Fromable leftTable, String leftTableAlias, Fromable rightTable, String rightTableAlias, String joinCondition) {
		return from(leftTable).innerJoin(rightTable, rightTableAlias, joinCondition);
	}
	
	@Override
	public <I> FluentFromClause from(JoinLink<?, I> leftColumn, JoinLink<?, I> rightColumn) {
		return from(leftColumn.getOwner()).innerJoin(leftColumn, rightColumn);
	}
	
	@Override
	public <O> FluentWhereClause where(Column<?, O> column, CharSequence condition) {
		return wrapAsWhereClause().and(new ColumnCriterion(column, condition));
	}
	
	@Override
	public <O> FluentWhereClause where(Column<?, O> column, ConditionalOperator<? super O, ?> condition) {
		return wrapAsWhereClause().and(new ColumnCriterion(column, condition));
	}
	
	@Override
	public FluentWhereClause where(Criteria<?> criteria) {
		return wrapAsWhereClause().and(criteria);
	}
	
	@Override
	public FluentWhereClause where(Object... criteria) {
		return wrapAsWhereClause().and(criteria);
	}
	
	@Override
	public FluentGroupByClause groupBy(Column column, Column... columns) {
		return wrapAsGroupByClause().add(column, columns);
	}
	
	@Override
	public FluentGroupByClause groupBy(String column, String... columns) {
		return wrapAsGroupByClause().add(column, columns);
	}
	
	@Override
	public FluentHavingClause having(Column column, String condition) {
		return wrapAsHavingClause().and(column, condition);
	}
	
	@Override
	public FluentHavingClause having(Object... columns) {
		return wrapAsHavingClause().and(columns);
	}
	
	@Override
	public Query getQuery() {
		return this.query;
	}
	
	public FluentOrderByClause orderBy() {
		return wrapAsOrderByClause();
	}
	
	@Override
	public FluentOrderByClause orderBy(Selectable<?> column, OrderByChain.Order order) {
		return orderBy().add(column, order);
	}
	
	@Override
	public FluentOrderByClause orderBy(Selectable<?> col1, OrderByChain.Order order1, Selectable<?> col2, OrderByChain.Order order2) {
		return orderBy().add(col1, order1, col2, order2);
	}
	
	@Override
	public FluentOrderByClause orderBy(Selectable<?> col1, OrderByChain.Order order1, Selectable<?> col2, OrderByChain.Order order2, Selectable<?> col3, OrderByChain.Order order3) {
		return orderBy().add(col1, order1, col2, order2, col3, order3);
	}
	
	@Override
	public FluentOrderByClause orderBy(String column, OrderByChain.Order order) {
		return orderBy().add(column, order);
	}
	
	@Override
	public FluentOrderByClause orderBy(String col1, OrderByChain.Order order1, String col2, OrderByChain.Order order2) {
		return orderBy().add(col1, order1, col2, order2);
	}
	
	@Override
	public FluentOrderByClause orderBy(String col1, OrderByChain.Order order1, String col2, OrderByChain.Order order2, String col3, OrderByChain.Order order3) {
		return orderBy().add(col1, order1, col2, order2, col3, order3);
	}
	
	@Override
	public FluentOrderByClause orderBy(Selectable<?> column, Selectable<?>... columns) {
		return orderBy().add(column, columns);
	}
	
	@Override
	public FluentOrderByClause orderBy(String column, String... columns) {
		return orderBy().add(column, columns);
	}
	
	@Override
	public FluentLimitClause limit(int value) {
		return wrapAsLimitClause().setCount(value);
	}
	
	@Override
	public FluentLimitClause limit(int value, Integer offset) {
		return wrapAsLimitClause().setCount(value, offset);
	}
	
	@Override
	public Union unionAll(QueryProvider<Query> query) {
		return new Union(this.query, query.getQuery());
	}
	
	public interface FluentSelectClause extends SelectAwareChain<FluentSelectClause>, FromAware, WhereAware, QueryProvider<Query> {
		
		@Override
		FluentSelectClause select(Iterable<? extends Selectable<?>> selectables);
		
		/**
		 * A variation of {@link #select(Selectable, String)} that allows to specify an alias for the column by chaining
		 * it with a call to {@link org.codefilarete.stalactite.query.api.SelectChain.Aliasable#as(String)}.
		 * @param column the column to add
		 * @return an enhanced version of the current instance that allows chaining with some other methods
		 */
		@Override
		SelectAwareAliasExpression select(Selectable<?> column);
		
		@Override
		FluentSelectClause select(Selectable<?> column, String alias);
		
		@Override
		FluentSelectClause select(Selectable<?> expression, Selectable<?>... expressions);
		
		/**
		 * A variation of {@link #select(String, Class, String)} that allows to specify an alias for the column by chaining
		 * it with a call to {@link org.codefilarete.stalactite.query.api.SelectChain.Aliasable#as(String)}.
		 * @param expression the column to add
		 * @return an enhanced version of the current instance that allows chaining with some other methods
		 */
		@Override
		SelectAwareAliasExpression select(String expression, Class<?> javaType);
		
		@Override
		FluentSelectClause select(String expression, Class<?> javaType, String alias);
		
		@Override
		FluentSelectClause select(Selectable<?> col1, String alias1, Selectable<?> col2, String alias2);
		
		@Override
		FluentSelectClause select(Selectable<?> col1, String alias1, Selectable<?> col2, String alias2, Selectable<?> col3, String alias3);
		
		@Override
		FluentSelectClause select(Map<? extends Selectable<?>, String> aliasedColumns);
		
		/**
		 * A variation of {@link #add(Selectable, String)} that allows to specify an alias for the column by chaining
		 * it with a call to {@link org.codefilarete.stalactite.query.api.SelectChain.Aliasable#as(String)}.
		 * @param column the column to add
		 * @return an enhanced version of the current instance that allows chaining with some other methods
		 */
		@Override
		SelectAwareAliasExpression add(Selectable<?> column);
		
		/**
		 * A variation of {@link #add(String, Class, String)} that allows to specify an alias for the column by chaining
		 * it with a call to {@link org.codefilarete.stalactite.query.api.SelectChain.Aliasable#as(String)}.
		 * @param expression the expression to add
		 * @param javaType the Java type of the expression for reading it
		 * @return an enhanced version of the current instance that allows chaining with some other methods
		 */
		@Override
		SelectAwareAliasExpression add(String expression, Class<?> javaType);
		
		@Override
		FluentSelectClause add(String expression, Class<?> javaType, String alias);
		
	}
	
	interface SelectAwareChain<SELF extends SelectAwareChain<SELF>> extends SelectChain<SELF>, SelectAware<SELF> {
		
	}
	
	public interface SelectAwareAliasExpression extends FluentSelectClause, SelectChain.Aliasable<FluentSelectClause> {
		
		@Override
		FluentSelectClause as(String alias);
	}

}
