package org.codefilarete.stalactite.query.model;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.OrderByChain.Order;
import org.codefilarete.stalactite.query.model.Query.FluentUnionClause;
import org.codefilarete.stalactite.query.model.SelectChain.Aliasable;
import org.codefilarete.stalactite.query.model.SelectChain.AliasableExpression;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.SerializableTriFunction;
import org.codefilarete.tool.function.TriFunction;
import org.codefilarete.tool.reflect.MethodDispatcher;
import org.danekja.java.util.function.serializable.SerializableBiFunction;

/**
 * A support for a SQL query, trying to be closest as possible to a real select query syntax and implementing the most simple/common usage. 
 * No syntax validation is done. Final printing can be made by {@link QuerySQLBuilder}
 * 
 * @author Guillaume Mary
 * @see QuerySQLBuilder
 * @see QueryEase
 */
public class Query implements FromAware, WhereAware, HavingAware, OrderByAware, LimitAware<FluentUnionClause>, QueryProvider<Query>,
		QueryStatement, UnionAware {
	
	private final FluentSelectClause select;
	private final Select selectSurrogate;
	private final FluentFromClause from;
	private final From fromSurrogate;
	private final FluentWhereClause where;
	private final Where whereSurrogate;
	private final FluentGroupByClause groupBy;
	private final GroupBy groupBySurrogate;
	private final FluentHavingClause having;
	private final Having havingSurrogate;
	private final OrderBy orderBySurrogate;
	private final FluentOrderByClause orderBy;
	private final Limit limitSurrogate;
	private final FluentLimitClause limit;
	private final FluentUnionClause fluentUnionClause;
	
	public Query() {
		this(null);
	}
	
	public Query(Fromable rootTable) {
		this.selectSurrogate = new Select();
		this.select = new MethodReferenceDispatcher()
				.redirect(SelectChain.class, selectSurrogate, true)
				.redirect((SerializableTriFunction<FluentSelectClause, String, Class, FluentSelectClauseAliasableExpression>)
						FluentSelectClause::add, new BiFunction<String, Class, FluentSelectClauseAliasableExpression>() {
					@Override
					public FluentSelectClauseAliasableExpression apply(String s, Class aClass) {
						AliasableExpression<Select> add = selectSurrogate.add(s, aClass);
						return new MethodDispatcher()
								.redirect(Aliasable.class, alias -> {
									add.as(alias);
									return null;    // we don't care about returned object since proxy is returned
								}, true)
								.fallbackOn(select)
								.build(FluentSelectClauseAliasableExpression.class);
					}
				})
				.redirect(FromAware.class, this)
				.redirect(QueryProvider.class, this)
				.build(FluentSelectClause.class);
		this.fromSurrogate = new From(rootTable);
		this.from = new MethodDispatcher()
				.redirect(JoinChain.class, fromSurrogate, true)
				.redirect(WhereAware.class, this)
				.redirect(GroupByAware.class, this)
				.redirect(OrderByAware.class, this)
				.redirect(LimitAware.class, this)
				.redirect(QueryProvider.class, this)
				.build(FluentFromClause.class);
		this.whereSurrogate = new Where();
		this.where = new MethodDispatcher()
				.redirect(CriteriaChain.class, whereSurrogate, true)
				.redirect(Iterable.class, whereSurrogate)
				.redirect(GroupByAware.class, this)
				.redirect(OrderByAware.class, this)
				.redirect(LimitAware.class, this)
				.redirect(QueryProvider.class, this)
				.redirect(UnionAware.class, this)
				.build(FluentWhereClause.class);
		this.groupBySurrogate = new GroupBy();
		this.groupBy = new MethodDispatcher()
				.redirect(GroupByChain.class, groupBySurrogate, true)
				.redirect(HavingAware.class, this)
				.redirect(OrderByAware.class, this)
				.redirect(LimitAware.class, this)
				.redirect(QueryProvider.class, this)
				.redirect(UnionAware.class, this)
				.build(FluentGroupByClause.class);
		this.havingSurrogate = new Having();
		this.having = new MethodDispatcher()
				.redirect(CriteriaChain.class, havingSurrogate, true)
				.redirect(OrderByAware.class, this)
				.redirect(LimitAware.class, this)
				.redirect(QueryProvider.class, this)
				.redirect(UnionAware.class, this)
				.build(FluentHavingClause.class);
		this.orderBySurrogate = new OrderBy();
		this.orderBy = new MethodDispatcher()
				.redirect(OrderByChain.class, orderBySurrogate, true)
				.redirect(LimitAware.class, this)
				.redirect(QueryProvider.class, this)
				.redirect(UnionAware.class, this)
				.build(FluentOrderByClause.class);
		this.limitSurrogate = new Limit();
		this.limit = new MethodDispatcher()
				.redirect(LimitChain.class, limitSurrogate, true)
				.redirect(QueryProvider.class, this)
				.redirect(UnionAware.class, this)
				.build(FluentLimitClause.class);
		this.fluentUnionClause = new MethodDispatcher()
				.redirect(QueryProvider.class, this)
				.redirect(UnionAware.class, this)
				.build(FluentUnionClause.class);
	}
	
	public FluentSelectClause getSelect() {
		return this.select;
	}
	
	/**
	 * @return a concrete implementation of a select
	 */
	public Select getSelectSurrogate() {
		return selectSurrogate;
	}
	
	public JoinChain getFrom() {
		return this.from;
	}
	
	public From getFromSurrogate() {
		return fromSurrogate;
	}
	
	public FluentWhereClause getWhere() {
		return where;
	}
	
	public Where getWhereSurrogate() {
		return whereSurrogate;
	}
	
	public GroupBy getGroupBySurrogate() {
		return groupBySurrogate;
	}
	
	public Having getHavingSurrogate() {
		return havingSurrogate;
	}
	
	public OrderBy getOrderBySurrogate() {
		return orderBySurrogate;
	}
	
	public Limit getLimitSurrogate() {
		return limitSurrogate;
	}
	
	@Override
	public KeepOrderSet<Selectable<?>> getColumns() {
		return this.selectSurrogate.getColumns();
	}
	
	@Override
	public Map<Selectable<?>, String> getAliases() {
		return this.selectSurrogate.getAliases();
	}
	
	public FluentSelectClause select(Iterable<? extends Selectable<?>> selectables) {
		selectables.forEach(this.selectSurrogate::add);
		return this.select;
	}
	
	public FluentSelectClause select(Selectable<?> expression, Selectable<?>... expressions) {
		this.selectSurrogate.add(expression, expressions);
		return select;
	}
	
	public FluentSelectClauseAliasableExpression select(String expression, Class<?> javaType) {
		AliasableExpression<Select> add = this.selectSurrogate.add(expression, javaType);
		return new MethodDispatcher()
				.redirect(Aliasable.class, alias -> {
					add.as(alias);
					return null;    // we don't care about returned object since proxy is returned
				}, true)
				.redirect(FluentSelectClause.class, select)
				.build(FluentSelectClauseAliasableExpression.class);
	}
	
	public FluentSelectClause select(Selectable<?> column, String alias) {
		this.selectSurrogate.add(column, alias);
		return select;
	}
	
	public FluentSelectClause select(Selectable<?> col1, String alias1, Selectable<?> col2, String alias2) {
		this.selectSurrogate.add(col1, alias1, col2, alias2);
		return select;
	}
	
	public FluentSelectClause select(Selectable<?> col1, String alias1, Selectable<?> col2, String alias2, Selectable<?> col3, String alias3) {
		this.selectSurrogate.add(col1, alias1, col2, alias2, col3, alias3);
		return select;
	}
	
	public FluentSelectClause select(Map<? extends Selectable<?>, String> aliasedColumns) {
		this.selectSurrogate.add(aliasedColumns);
		return select;
	}
	
	@Override
	public FluentFromClause from(Fromable leftTable, Fromable rightTable, String joinCondition) {
		this.fromSurrogate.setRoot(leftTable).innerJoin(rightTable, joinCondition);
		return from;
	}
	
	@Override
	public FluentFromClause from(Fromable leftTable) {
		this.fromSurrogate.setRoot(leftTable);
		return from;
	}
	
	@Override
	public FluentFromClause from(Fromable leftTable, String tableAlias) {
		this.fromSurrogate.setRoot(leftTable, tableAlias);
		return from;
	}
	
	@Override
	public FluentFromClause from(Fromable leftTable, String leftTableAlias, Fromable rightTable, String rightTableAlias, String joinCondition) {
		this.fromSurrogate.setRoot(leftTable).innerJoin(rightTable, rightTableAlias, joinCondition);
		return from;
	}
	
	@Override
	public <I> FluentFromClause from(JoinLink<?, I> leftColumn, JoinLink<?, I> rightColumn) {
		this.fromSurrogate.setRoot(leftColumn.getOwner()).innerJoin(leftColumn, rightColumn);
		return from;
	}
	
	@Override
	public FluentWhereClause where(Column column, CharSequence condition) {
		this.whereSurrogate.and(new ColumnCriterion(column, condition));
		return where;
	}
	
	@Override
	public FluentWhereClause where(Column column, AbstractRelationalOperator condition) {
		this.whereSurrogate.and(new ColumnCriterion(column, condition));
		return where;
	}
	
	@Override
	public FluentWhereClause where(Criteria criteria) {
		this.whereSurrogate.and(criteria);
		return where;
	}
	
	@Override
	public FluentGroupByClause groupBy(Column column, Column... columns) {
		this.groupBySurrogate.add(column, columns);
		return groupBy;
	}
	
	@Override
	public FluentGroupByClause groupBy(String column, String... columns) {
		this.groupBySurrogate.add(column, columns);
		return groupBy;
	}
	
	@Override
	public FluentHavingClause having(Column column, String condition) {
		this.havingSurrogate.and(column, condition);
		return having;
	}
	
	@Override
	public FluentHavingClause having(Object... columns) {
		this.havingSurrogate.and(columns);
		return having;
	}
	
	@Override
	public Query getQuery() {
		return this;
	}
	
	@Override
	public FluentOrderByClause orderBy(Column column, Order order) {
		this.orderBySurrogate.add(column, order);
		return orderBy;
	}
	
	@Override
	public FluentOrderByClause orderBy(Column col1, Order order1, Column col2, Order order2) {
		this.orderBySurrogate.add(col1, order1, col2, order2);
		return orderBy;
	}
	
	@Override
	public FluentOrderByClause orderBy(Column col1, Order order1, Column col2, Order order2, Column col3, Order order3) {
		this.orderBySurrogate.add(col1, order1, col2, order2, col3, order3);
		return orderBy;
	}
	
	@Override
	public FluentOrderByClause orderBy(String column, Order order) {
		this.orderBySurrogate.add(column, order);
		return orderBy;
	}
	
	@Override
	public FluentOrderByClause orderBy(String col1, Order order1, String col2, Order order2) {
		this.orderBySurrogate.add(col1, order1, col2, order2);
		return orderBy;
	}
	
	@Override
	public FluentOrderByClause orderBy(String col1, Order order1, String col2, Order order2, String col3, Order order3) {
		this.orderBySurrogate.add(col1, order1, col2, order2, col3, order3);
		return orderBy;
	}
	
	@Override
	public FluentOrderByClause orderBy(Column column, Column... columns) {
		this.orderBySurrogate.add(column, columns);
		return orderBy;
	}
	
	@Override
	public FluentOrderByClause orderBy(String column, String... columns) {
		this.orderBySurrogate.add(column, columns);
		return orderBy;
	}
	
	@Override
	public FluentUnionClause limit(int value) {
		this.limit.setValue(value);
		return fluentUnionClause;
	}
	
	@Override
	public Union unionAll(QueryProvider<Query> query) {
		return new Union(this, query.getQuery());
	}
	
	public interface FluentSelectClause extends SelectChain<FluentSelectClause>, FromAware, QueryProvider<Query> {
		
		/**
		 * Overridden to return {@link FluentSelectClauseAliasableExpression} aimed at giving {@link FluentSelectClause} allowing to chain with
		 * methods of {@link SelectChain}
		 * 
		 * @param expression
		 * @param javaType
		 * @return an object that applies {@link AliasableExpression} methods and dispatches {@link FluentSelectClause} calls to surrounding instance
		 */
		@Override
		FluentSelectClauseAliasableExpression add(String expression, Class<?> javaType);
	}
	
	/**
	 * A mixin of {@link AliasableExpression} and {@link FluentSelectClause} to allow chaining of {@link FluentSelectClause} methods after {@link #as(String)}
	 */
	public interface FluentSelectClauseAliasableExpression extends AliasableExpression<FluentSelectClause>, FluentSelectClause {
		
		@Override
		FluentSelectClause as(String alias);
	}
	
	public interface FluentFromClause extends JoinChain<FluentFromClause>, WhereAware, GroupByAware, OrderByAware, LimitAware<FluentUnionClause>, QueryProvider<Query> {
		
	}
	
	public interface FluentWhereClause extends CriteriaChain<FluentWhereClause>, GroupByAware, OrderByAware, LimitAware<FluentUnionClause>, UnionAware, QueryProvider<Query> {
		
	}
	
	public interface FluentGroupByClause extends GroupByChain<FluentGroupByClause>, HavingAware, OrderByAware, LimitAware<FluentUnionClause>, UnionAware, QueryProvider<Query> {
		
	}
	
	public interface FluentHavingClause extends CriteriaChain<FluentHavingClause>, OrderByAware, LimitAware<FluentUnionClause>, UnionAware, QueryProvider<Query> {
		
	}
	
	public interface FluentOrderByClause extends OrderByChain<FluentOrderByClause>, LimitAware<FluentUnionClause>, UnionAware, QueryProvider<Query> {
		
	}
	
	public interface FluentLimitClause extends LimitChain<FluentLimitClause>, UnionAware, QueryProvider<Query> {
		
	}
	
	public interface FluentUnionClause extends UnionAware, QueryProvider<Union> {
		
	}
}
