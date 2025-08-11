package org.codefilarete.stalactite.query.model;

import java.util.Map;
import java.util.function.BiFunction;

import org.codefilarete.reflection.MethodReferenceDispatcher;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.OrderByChain.Order;
import org.codefilarete.stalactite.query.model.Query.FluentLimitClause;
import org.codefilarete.stalactite.query.model.SelectChain.Aliasable;
import org.codefilarete.stalactite.query.model.SelectChain.AliasableExpression;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.SerializableTriFunction;
import org.codefilarete.tool.reflect.MethodDispatcher;

/**
 * A support for a SQL query, trying to be closest as possible to a real select query syntax and implementing the most simple/common usage. 
 * No syntax validation is done. Final printing can be made by {@link QuerySQLBuilder}
 * 
 * @author Guillaume Mary
 * @see QuerySQLBuilder
 * @see QueryEase
 */
public class Query implements FromAware, WhereAware, HavingAware, OrderByAware, LimitAware<FluentLimitClause>, QueryProvider<Query>,
		QueryStatement, UnionAware {
	
	private final FluentSelectClause select;
	private final Select selectDelegate;
	private final FluentFromClause from;
	private final From fromDelegate;
	private final FluentWhereClause where;
	private final Where whereDelegate;
	private final FluentGroupByClause groupBy;
	private final GroupBy groupByDelegate;
	private final FluentHavingClause having;
	private final Having havingDelegate;
	private final FluentOrderByClause orderBy;
	private final OrderBy orderByDelegate;
	private final FluentLimitClause limit;
	private final Limit limitDelegate;
	
	public Query() {
		this(null);
	}
	
	public Query(Fromable rootTable) {
		this(new Select(), new From(rootTable), new Where(), new GroupBy(), new Having(), new OrderBy(), new Limit()); 
	}
	
	public Query(Select selectDelegate,
				 From fromDelegate,
				 Where whereDelegate,
				 GroupBy groupByDelegate,
				 Having havingDelegate,
				 OrderBy orderByDelegate,
				 Limit limitDelegate) {
		this.selectDelegate = selectDelegate;
		this.select = new MethodReferenceDispatcher()
				.redirect(SelectChain.class, this.selectDelegate, true)
				.redirect((SerializableTriFunction<FluentSelectClause, String, Class, FluentSelectClauseAliasableExpression>)
						FluentSelectClause::add, new BiFunction<String, Class, FluentSelectClauseAliasableExpression>() {
					@Override
					public FluentSelectClauseAliasableExpression apply(String s, Class aClass) {
						AliasableExpression<Select> add = Query.this.selectDelegate.add(s, aClass);
						return new MethodDispatcher()
								.redirect(Aliasable.class, alias -> {
									add.as(alias);
									return null;    // we don't care about the returned object since the proxy is returned
								}, true)
								.fallbackOn(select)
								.build(FluentSelectClauseAliasableExpression.class);
					}
				})
				.redirect(FromAware.class, this)
				.redirect(QueryProvider.class, this)
				.build(FluentSelectClause.class);
		this.fromDelegate = fromDelegate;
		this.from = new MethodDispatcher()
				.redirect(JoinChain.class, this.fromDelegate, true)
				.redirect(WhereAware.class, this)
				.redirect(GroupByAware.class, this)
				.redirect(OrderByAware.class, this)
				.redirect(LimitAware.class, this)
				.redirect(QueryProvider.class, this)
				.build(FluentFromClause.class);
		this.whereDelegate = whereDelegate;
		this.where = new MethodDispatcher()
				.redirect(CriteriaChain.class, this.whereDelegate, true)
				.redirect(Iterable.class, whereDelegate)
				.redirect(GroupByAware.class, this)
				.redirect(OrderByAware.class, this)
				.redirect(LimitAware.class, this)
				.redirect(QueryProvider.class, this)
				.redirect(UnionAware.class, this)
				.build(FluentWhereClause.class);
		this.groupByDelegate = groupByDelegate;
		this.groupBy = new MethodDispatcher()
				.redirect(GroupByChain.class, this.groupByDelegate, true)
				.redirect(HavingAware.class, this)
				.redirect(OrderByAware.class, this)
				.redirect(LimitAware.class, this)
				.redirect(QueryProvider.class, this)
				.redirect(UnionAware.class, this)
				.build(FluentGroupByClause.class);
		this.havingDelegate = havingDelegate;
		this.having = new MethodDispatcher()
				.redirect(CriteriaChain.class, this.havingDelegate, true)
				.redirect(OrderByAware.class, this)
				.redirect(LimitAware.class, this)
				.redirect(QueryProvider.class, this)
				.redirect(UnionAware.class, this)
				.build(FluentHavingClause.class);
		this.orderByDelegate = orderByDelegate;
		this.orderBy = new MethodDispatcher()
				.redirect(OrderByChain.class, this.orderByDelegate, true)
				.redirect(LimitAware.class, this)
				.redirect(QueryProvider.class, this)
				.redirect(UnionAware.class, this)
				.build(FluentOrderByClause.class);
		this.limitDelegate = limitDelegate;
		this.limit = new MethodDispatcher()
				.redirect(LimitChain.class, this.limitDelegate, true)
				.redirect(QueryProvider.class, this)
				.redirect(UnionAware.class, this)
				.build(FluentLimitClause.class);
	}
	
	public FluentSelectClause getSelect() {
		return this.select;
	}
	
	/**
	 * @return a concrete implementation of a select
	 */
	public Select getSelectDelegate() {
		return selectDelegate;
	}
	
	public JoinChain getFrom() {
		return this.from;
	}
	
	public From getFromDelegate() {
		return fromDelegate;
	}
	
	public FluentWhereClause getWhere() {
		return where;
	}
	
	public Where getWhereDelegate() {
		return whereDelegate;
	}
	
	public GroupBy getGroupByDelegate() {
		return groupByDelegate;
	}
	
	public Having getHavingDelegate() {
		return havingDelegate;
	}
	
	public OrderBy getOrderByDelegate() {
		return orderByDelegate;
	}
	
	public Limit getLimitDelegate() {
		return limitDelegate;
	}
	
	@Override
	public KeepOrderSet<Selectable<?>> getColumns() {
		return this.selectDelegate.getColumns();
	}
	
	@Override
	public Map<Selectable<?>, String> getAliases() {
		return this.selectDelegate.getAliases();
	}
	
	public FluentSelectClause select(Iterable<? extends Selectable<?>> selectables) {
		selectables.forEach(this.selectDelegate::add);
		return this.select;
	}
	
	public FluentSelectClause select(Selectable<?> expression, Selectable<?>... expressions) {
		this.selectDelegate.add(expression, expressions);
		return select;
	}
	
	public FluentSelectClauseAliasableExpression select(String expression, Class<?> javaType) {
		AliasableExpression<Select> add = this.selectDelegate.add(expression, javaType);
		return new MethodDispatcher()
				.redirect(Aliasable.class, alias -> {
					add.as(alias);
					return null;    // we don't care about returned object since proxy is returned
				}, true)
				.redirect(FluentSelectClause.class, select)
				.build(FluentSelectClauseAliasableExpression.class);
	}
	
	public FluentSelectClause select(Selectable<?> column, String alias) {
		this.selectDelegate.add(column, alias);
		return select;
	}
	
	public FluentSelectClause select(Selectable<?> col1, String alias1, Selectable<?> col2, String alias2) {
		this.selectDelegate.add(col1, alias1, col2, alias2);
		return select;
	}
	
	public FluentSelectClause select(Selectable<?> col1, String alias1, Selectable<?> col2, String alias2, Selectable<?> col3, String alias3) {
		this.selectDelegate.add(col1, alias1, col2, alias2, col3, alias3);
		return select;
	}
	
	public FluentSelectClause select(Map<? extends Selectable<?>, String> aliasedColumns) {
		this.selectDelegate.add(aliasedColumns);
		return select;
	}
	
	@Override
	public FluentFromClause from(Fromable leftTable, Fromable rightTable, String joinCondition) {
		this.fromDelegate.setRoot(leftTable).innerJoin(rightTable, joinCondition);
		return from;
	}
	
	@Override
	public FluentFromClause from(Fromable leftTable) {
		this.fromDelegate.setRoot(leftTable);
		return from;
	}
	
	@Override
	public FluentFromClause from(QueryProvider<?> query, String alias) {
		this.fromDelegate.setRoot(query.getQuery().asPseudoTable(), alias);
		return from;
	}
	
	@Override
	public FluentFromClause from(Fromable leftTable, String tableAlias) {
		this.fromDelegate.setRoot(leftTable, tableAlias);
		return from;
	}
	
	@Override
	public FluentFromClause from(Fromable leftTable, String leftTableAlias, Fromable rightTable, String rightTableAlias, String joinCondition) {
		this.fromDelegate.setRoot(leftTable).innerJoin(rightTable, rightTableAlias, joinCondition);
		return from;
	}
	
	@Override
	public <I> FluentFromClause from(JoinLink<?, I> leftColumn, JoinLink<?, I> rightColumn) {
		this.fromDelegate.setRoot(leftColumn.getOwner()).innerJoin(leftColumn, rightColumn);
		return from;
	}
	
	@Override
	public <O> FluentWhereClause where(Column<?, O> column, CharSequence condition) {
		this.whereDelegate.and(new ColumnCriterion(column, condition));
		return where;
	}
	
	@Override
	public <O> FluentWhereClause where(Column<?, O> column, ConditionalOperator<? super O, ?> condition) {
		this.whereDelegate.and(new ColumnCriterion(column, condition));
		return where;
	}
	
	@Override
	public FluentWhereClause where(Criteria criteria) {
		this.whereDelegate.and(criteria);
		return where;
	}
	
	@Override
	public FluentWhereClause where(Object... criteria) {
		this.whereDelegate.and(criteria);
		return where;
	}
	
	@Override
	public FluentGroupByClause groupBy(Column column, Column... columns) {
		this.groupByDelegate.add(column, columns);
		return groupBy;
	}
	
	@Override
	public FluentGroupByClause groupBy(String column, String... columns) {
		this.groupByDelegate.add(column, columns);
		return groupBy;
	}
	
	@Override
	public FluentHavingClause having(Column column, String condition) {
		this.havingDelegate.and(column, condition);
		return having;
	}
	
	@Override
	public FluentHavingClause having(Object... columns) {
		this.havingDelegate.and(columns);
		return having;
	}
	
	@Override
	public Query getQuery() {
		return this;
	}
	
	public FluentOrderByClause orderBy() {
		return orderBy;
	}
	
	@Override
	public FluentOrderByClause orderBy(Column column, Order order) {
		this.orderByDelegate.add(column, order);
		return orderBy;
	}
	
	@Override
	public FluentOrderByClause orderBy(Column col1, Order order1, Column col2, Order order2) {
		this.orderByDelegate.add(col1, order1, col2, order2);
		return orderBy;
	}
	
	@Override
	public FluentOrderByClause orderBy(Column col1, Order order1, Column col2, Order order2, Column col3, Order order3) {
		this.orderByDelegate.add(col1, order1, col2, order2, col3, order3);
		return orderBy;
	}
	
	@Override
	public FluentOrderByClause orderBy(String column, Order order) {
		this.orderByDelegate.add(column, order);
		return orderBy;
	}
	
	@Override
	public FluentOrderByClause orderBy(String col1, Order order1, String col2, Order order2) {
		this.orderByDelegate.add(col1, order1, col2, order2);
		return orderBy;
	}
	
	@Override
	public FluentOrderByClause orderBy(String col1, Order order1, String col2, Order order2, String col3, Order order3) {
		this.orderByDelegate.add(col1, order1, col2, order2, col3, order3);
		return orderBy;
	}
	
	@Override
	public FluentOrderByClause orderBy(Column column, Column... columns) {
		this.orderByDelegate.add(column, columns);
		return orderBy;
	}
	
	@Override
	public FluentOrderByClause orderBy(String column, String... columns) {
		this.orderByDelegate.add(column, columns);
		return orderBy;
	}
	
	@Override
	public FluentLimitClause limit(int value) {
		this.limitDelegate.setCount(value);
		return limit;
	}
	
	@Override
	public FluentLimitClause limit(int value, Integer offset) {
		this.limitDelegate.setCount(value, offset);
		return limit;
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
	
	public interface FluentFromClause extends JoinChain<FluentFromClause>, WhereAware, GroupByAware, OrderByAware, LimitAware<FluentLimitClause>, QueryProvider<Query> {
		
	}
	
	public interface FluentWhereClause extends CriteriaChain<FluentWhereClause>, GroupByAware, OrderByAware, LimitAware<FluentLimitClause>, UnionAware, QueryProvider<Query> {
		
	}
	
	public interface FluentGroupByClause extends GroupByChain<FluentGroupByClause>, HavingAware, OrderByAware, LimitAware<FluentLimitClause>, UnionAware, QueryProvider<Query> {
		
	}
	
	public interface FluentHavingClause extends CriteriaChain<FluentHavingClause>, OrderByAware, LimitAware<FluentLimitClause>, UnionAware, QueryProvider<Query> {
		
	}
	
	public interface FluentOrderByClause extends OrderByChain<FluentOrderByClause>, LimitAware<FluentLimitClause>, UnionAware, QueryProvider<Query> {
		
	}
	
	public interface FluentLimitClause extends LimitChain<FluentLimitClause>, UnionAware, QueryProvider<Query> {
		
	}
}
