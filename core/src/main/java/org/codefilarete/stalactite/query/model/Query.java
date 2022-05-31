package org.codefilarete.stalactite.query.model;

import java.util.Map;

import org.codefilarete.stalactite.query.builder.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.OrderByChain.Order;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.reflect.MethodDispatcher;

/**
 * A support for a SQL query, trying to be closest as possible to a real select query syntax and implementing the most simple/common usage. 
 * No syntax validation is done. Final printing can be made by {@link QuerySQLBuilder}
 * 
 * @author Guillaume Mary
 * @see QuerySQLBuilder
 * @see QueryEase
 */
public class Query implements FromAware, WhereAware, HavingAware, OrderByAware, LimitAware, QueryProvider<Query>,
		QueryStatement, UnionAware {
	
	private final FluentSelect select;
	private final Select selectSurrogate;
	private final FluentFrom from;
	private final From fromSurrogate;
	private final FluentWhere where;
	private final Where whereSurrogate;
	private final FluentGroupBy groupBy;
	private final GroupBy groupBySurrogate;
	private final FluentHaving having;
	private final Having havingSurrogate;
	private final OrderBy orderBySurrogate;
	private final FluentOrderBy orderBy;
	private final Limit limitSurrogate;
	private final FluentLimit limit;
	
	public Query() {
		this(null);
	}
	
	public Query(Fromable rootTable) {
		this.selectSurrogate = new Select();
		this.select = new MethodDispatcher()
				.redirect(SelectChain.class, selectSurrogate, true)
				.redirect(FromAware.class, this)
				.redirect(QueryProvider.class, this)
				.build(FluentSelect.class);
		this.fromSurrogate = new From(rootTable);
		this.from = new MethodDispatcher()
				.redirect(JoinChain.class, fromSurrogate, true)
				.redirect(WhereAware.class, this)
				.redirect(GroupByAware.class, this)
				.redirect(OrderByAware.class, this)
				.redirect(LimitAware.class, this)
				.redirect(QueryProvider.class, this)
				.build(FluentFrom.class);
		this.whereSurrogate = new Where();
		this.where = new MethodDispatcher()
				.redirect(CriteriaChain.class, whereSurrogate, true)
				.redirect(Iterable.class, whereSurrogate)
				.redirect(GroupByAware.class, this)
				.redirect(OrderByAware.class, this)
				.redirect(LimitAware.class, this)
				.redirect(QueryProvider.class, this)
				.redirect(UnionAware.class, this)
				.build(FluentWhere.class);
		this.groupBySurrogate = new GroupBy();
		this.groupBy = new MethodDispatcher()
				.redirect(GroupByChain.class, groupBySurrogate, true)
				.redirect(HavingAware.class, this)
				.redirect(OrderByAware.class, this)
				.redirect(LimitAware.class, this)
				.redirect(QueryProvider.class, this)
				.redirect(UnionAware.class, this)
				.build(FluentGroupBy.class);
		this.havingSurrogate = new Having();
		this.having = new MethodDispatcher()
				.redirect(CriteriaChain.class, havingSurrogate, true)
				.redirect(OrderByAware.class, this)
				.redirect(LimitAware.class, this)
				.redirect(QueryProvider.class, this)
				.redirect(UnionAware.class, this)
				.build(FluentHaving.class);
		this.orderBySurrogate = new OrderBy();
		this.orderBy = new MethodDispatcher()
				.redirect(OrderByChain.class, orderBySurrogate, true)
				.redirect(LimitAware.class, this)
				.redirect(QueryProvider.class, this)
				.redirect(UnionAware.class, this)
				.build(FluentOrderBy.class);
		this.limitSurrogate = new Limit();
		this.limit = new MethodDispatcher()
				.redirect(LimitChain.class, limitSurrogate, true)
				.redirect(QueryProvider.class, this)
				.redirect(UnionAware.class, this)
				.build(FluentLimit.class);
		
	}
	
	public FluentSelect getSelect() {
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
	
	public FluentWhere getWhere() {
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
	public KeepOrderSet<Selectable> getColumns() {
		return this.selectSurrogate.getColumns();
	}
	
	public FluentSelect select(Iterable<? extends Selectable> selectables) {
		selectables.forEach(this.selectSurrogate::add);
		return this.select;
	}
	
	public FluentSelect select(Selectable expression, Selectable... expressions) {
		this.selectSurrogate.add(expression, expressions);
		return select;
	}
	
	public FluentSelect select(String expression, String... expressions) {
		this.selectSurrogate.add(expression, expressions);
		return select;
	}
	
	public FluentSelect select(Column column, String alias) {
		this.selectSurrogate.add(column, alias);
		return select;
	}
	
	public FluentSelect select(Column col1, String alias1, Column col2, String alias2) {
		this.selectSurrogate.add(col1, alias1, col2, alias2);
		return select;
	}
	
	public FluentSelect select(Column col1, String alias1, Column col2, String alias2, Column col3, String alias3) {
		this.selectSurrogate.add(col1, alias1, col2, alias2, col3, alias3);
		return select;
	}
	
	public FluentSelect select(Map<Column, String> aliasedColumns) {
		this.selectSurrogate.add(aliasedColumns);
		return select;
	}
	
	@Override
	public FluentFrom from(Fromable leftTable, Fromable rightTable, String joinCondition) {
		this.fromSurrogate.setRoot(leftTable).innerJoin(rightTable, joinCondition);
		return from;
	}
	
	@Override
	public FluentFrom from(Fromable leftTable) {
		this.fromSurrogate.setRoot(leftTable);
		return from;
	}
	
	@Override
	public FluentFrom from(Fromable leftTable, String tableAlias) {
		this.fromSurrogate.setRoot(leftTable, tableAlias);
		return from;
	}
	
	@Override
	public FluentFrom from(Fromable leftTable, String leftTableAlias, Fromable rightTable, String rightTableAlias, String joinCondition) {
		this.fromSurrogate.setRoot(leftTable).innerJoin(rightTable, rightTableAlias, joinCondition);
		return from;
	}
	
	@Override
	public <I> FluentFrom from(JoinLink<I> leftColumn, JoinLink<I> rightColumn) {
		this.fromSurrogate.setRoot(leftColumn.getOwner()).innerJoin(leftColumn, rightColumn);
		return from;
	}
	
	@Override
	public FluentWhere where(Column column, CharSequence condition) {
		this.whereSurrogate.and(new ColumnCriterion(column, condition));
		return where;
	}
	
	@Override
	public FluentWhere where(Column column, AbstractRelationalOperator condition) {
		this.whereSurrogate.and(new ColumnCriterion(column, condition));
		return where;
	}
	
	@Override
	public FluentWhere where(Criteria criteria) {
		this.whereSurrogate.and(criteria);
		return where;
	}
	
	@Override
	public FluentGroupBy groupBy(Column column, Column... columns) {
		this.groupBySurrogate.add(column, columns);
		return groupBy;
	}
	
	@Override
	public FluentGroupBy groupBy(String column, String... columns) {
		this.groupBySurrogate.add(column, columns);
		return groupBy;
	}
	
	@Override
	public FluentHaving having(Column column, String condition) {
		this.havingSurrogate.and(column, condition);
		return having;
	}
	
	@Override
	public FluentHaving having(Object... columns) {
		this.havingSurrogate.and(columns);
		return having;
	}
	
	@Override
	public Query getQuery() {
		return this;
	}
	
	@Override
	public FluentOrderBy orderBy(Column column, Order order) {
		this.orderBySurrogate.add(column, order);
		return orderBy;
	}
	
	@Override
	public FluentOrderBy orderBy(Column col1, Order order1, Column col2, Order order2) {
		this.orderBySurrogate.add(col1, order1, col2, order2);
		return orderBy;
	}
	
	@Override
	public FluentOrderBy orderBy(Column col1, Order order1, Column col2, Order order2, Column col3, Order order3) {
		this.orderBySurrogate.add(col1, order1, col2, order2, col3, order3);
		return orderBy;
	}
	
	@Override
	public FluentOrderBy orderBy(String column, Order order) {
		this.orderBySurrogate.add(column, order);
		return orderBy;
	}
	
	@Override
	public FluentOrderBy orderBy(String col1, Order order1, String col2, Order order2) {
		this.orderBySurrogate.add(col1, order1, col2, order2);
		return orderBy;
	}
	
	@Override
	public FluentOrderBy orderBy(String col1, Order order1, String col2, Order order2, String col3, Order order3) {
		this.orderBySurrogate.add(col1, order1, col2, order2, col3, order3);
		return orderBy;
	}
	
	@Override
	public FluentOrderBy orderBy(Column column, Column... columns) {
		this.orderBySurrogate.add(column, columns);
		return orderBy;
	}
	
	@Override
	public FluentOrderBy orderBy(String column, String... columns) {
		this.orderBySurrogate.add(column, columns);
		return orderBy;
	}
	
	@Override
	public LimitChain limit(int value) {
		return this.limit.setValue(value);
	}
	
	@Override
	public Union unionAll(QueryProvider<Query> query) {
		return new Union(this, query.getQuery());
	}
	
	public interface FluentSelect extends SelectChain<FluentSelect>, FromAware, QueryProvider<Query> {
		
	}
	
	public interface FluentFrom extends JoinChain<FluentFrom>, WhereAware, GroupByAware, OrderByAware, LimitAware, QueryProvider<Query> {
		
	}
	
	public interface FluentWhere extends CriteriaChain<FluentWhere>, GroupByAware, OrderByAware, LimitAware, UnionAware, QueryProvider<Query> {
		
	}
	
	public interface FluentGroupBy extends GroupByChain<FluentGroupBy>, HavingAware, OrderByAware, LimitAware, UnionAware, QueryProvider<Query> {
		
	}
	
	public interface FluentHaving extends CriteriaChain<FluentHaving>, OrderByAware, LimitAware, UnionAware, QueryProvider<Query> {
		
	}
	
	public interface FluentOrderBy extends OrderByChain<FluentOrderBy>, LimitAware, UnionAware, QueryProvider<Query> {
		
	}
	
	public interface FluentLimit extends LimitChain<FluentLimit>, UnionAware, QueryProvider<Query> {
		
	}
	
	public interface FluentUnion extends UnionAware, QueryProvider<Union> {
		
	}
}
