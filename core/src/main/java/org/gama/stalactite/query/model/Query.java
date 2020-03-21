package org.gama.stalactite.query.model;

import java.util.Map;

import org.gama.lang.reflect.MethodDispatcher;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.query.builder.SQLQueryBuilder;
import org.gama.stalactite.query.model.OrderByChain.Order;

/**
 * A support for a SQL query, trying to be closest as possible to a real select query syntax and implementing the most simple/common usage. 
 * No syntax validation is done. Final printing can be made by {@link SQLQueryBuilder}
 * 
 * @author Guillaume Mary
 * @see SQLQueryBuilder
 * @see QueryEase
 */
public class Query implements FromAware, WhereAware, HavingAware, OrderByAware, LimitAware, QueryProvider {
	
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
		this.selectSurrogate = new Select();
		this.select = new MethodDispatcher()
				.redirect(SelectChain.class, selectSurrogate, true)
				.redirect(FromAware.class, this)
				.redirect(QueryProvider.class, this)
				.build(FluentSelect.class);
		this.fromSurrogate = new From();
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
				.build(FluentWhere.class);
		this.groupBySurrogate = new GroupBy();
		this.groupBy = new MethodDispatcher()
				.redirect(GroupByChain.class, groupBySurrogate, true)
				.redirect(HavingAware.class, this)
				.redirect(OrderByAware.class, this)
				.redirect(LimitAware.class, this)
				.redirect(QueryProvider.class, this)
				.build(FluentGroupBy.class);
		this.havingSurrogate = new Having();
		this.having = new MethodDispatcher()
				.redirect(CriteriaChain.class, havingSurrogate, true)
				.redirect(OrderByAware.class, this)
				.redirect(LimitAware.class, this)
				.redirect(QueryProvider.class, this)
				.build(FluentHaving.class);
		this.orderBySurrogate = new OrderBy();
		this.orderBy = new MethodDispatcher()
				.redirect(OrderByChain.class, orderBySurrogate, true)
				.redirect(LimitAware.class, this)
				.redirect(QueryProvider.class, this)
				.build(FluentOrderBy.class);
		this.limitSurrogate = new Limit();
		this.limit = new MethodDispatcher()
				.redirect(LimitChain.class, limitSurrogate, true)
				.redirect(QueryProvider.class, this)
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
	
	public FluentSelect select(Iterable<Object> selectables) {
		return this.select.add(selectables);
	}
	
	public FluentSelect select(Object selectable, Object... selectables) {
		return this.select.add(selectable, selectables);
	}
	
	public FluentSelect select(Column column, String alias) {
		return this.select.add(column, alias);
	}
	
	public FluentSelect select(Column col1, String alias1, Column col2, String alias2) {
		return this.select.add(col1, alias1, col2, alias2);
	}
	
	public FluentSelect select(Column col1, String alias1, Column col2, String alias2, Column col3, String alias3) {
		return this.select.add(col1, alias1, col2, alias2, col3, alias3);
	}
	
	public FluentSelect select(Map<Column, String> aliasedColumns) {
		return this.select.add(aliasedColumns);
	}
	
	@Override
	public FluentFrom from(Table leftTable, Table rightTable, String joinCondition) {
		return this.from.innerJoin(leftTable, rightTable, joinCondition);
	}
	
	@Override
	public FluentFrom from(Table leftTable) {
		return this.from.crossJoin(leftTable);
	}
	
	@Override
	public FluentFrom from(Table leftTable, String tableAlias) {
		return this.from.crossJoin(leftTable, tableAlias);
	}
	
	@Override
	public FluentFrom from(Table leftTable, String leftTableAlias, Table rightTable, String rightTableAlias, String joinCondition) {
		return this.from.innerJoin(leftTable, leftTableAlias, rightTable, rightTableAlias, joinCondition);
	}
	
	@Override
	public FluentFrom from(Column leftColumn, Column rightColumn) {
		return this.from.innerJoin(leftColumn, rightColumn);
	}
	
	@Override
	public FluentFrom from(Column leftColumn, String leftColumnAlias, Column rightColumn, String rightColumnAlias) {
		return this.from.innerJoin(leftColumn, leftColumnAlias, rightColumn, rightColumnAlias);
	}
	
	@Override
	public FluentFrom fromLeftOuter(Column leftColumn, Column rightColumn) {
		return this.from.leftOuterJoin(leftColumn, rightColumn);
	}
	
	@Override
	public FluentFrom fromLeftOuter(Column leftColumn, String leftColumnAlias, Column rightColumn, String rightColumnAlias) {
		return this.from.leftOuterJoin(leftColumn, leftColumnAlias, rightColumn, rightColumnAlias);
	}
	
	@Override
	public FluentFrom fromRightOuter(Column leftColumn, Column rightColumn) {
		return this.from.rightOuterJoin(leftColumn, rightColumn);
	}
	
	@Override
	public FluentFrom fromRightOuter(Column leftColumn, String leftColumnAlias, Column rightColumn, String rightColumnAlias) {
		return this.from.rightOuterJoin(leftColumn, leftColumnAlias, rightColumn, rightColumnAlias);
	}
	
	@Override
	public FluentWhere where(Column column, CharSequence condition) {
		return this.where.and(new ColumnCriterion(column, condition));
	}
	
	@Override
	public FluentWhere where(Column column, AbstractRelationalOperator condition) {
		return this.where.and(new ColumnCriterion(column, condition));
	}
	
	@Override
	public FluentWhere where(Criteria criteria) {
		return this.where.and(criteria);
	}
	
	@Override
	public FluentGroupBy groupBy(Column column, Column... columns) {
		return this.groupBy.add(column, columns);
	}
	
	@Override
	public FluentGroupBy groupBy(String column, String... columns) {
		return this.groupBy.add(column, columns);
	}
	
	@Override
	public FluentHaving having(Column column, String condition) {
		return having.and(column, condition);
	}
	
	@Override
	public FluentHaving having(Object... columns) {
		return having.and(columns);
	}
	
	@Override
	public Query getQuery() {
		return this;
	}
	
	@Override
	public FluentOrderBy orderBy(Column column, Order order) {
		return this.orderBy.add(column, order);
	}
	
	@Override
	public FluentOrderBy orderBy(Column col1, Order order1, Column col2, Order order2) {
		return this.orderBy.add(col1, order1, col2, order2);
	}
	
	@Override
	public FluentOrderBy orderBy(Column col1, Order order1, Column col2, Order order2, Column col3, Order order3) {
		return this.orderBy.add(col1, order1, col2, order2, col3, order3);
	}
	
	@Override
	public FluentOrderBy orderBy(String column, Order order) {
		return this.orderBy.add(column, order);
	}
	
	@Override
	public FluentOrderBy orderBy(String col1, Order order1, String col2, Order order2) {
		return this.orderBy.add(col1, order1, col2, order2);
	}
	
	@Override
	public FluentOrderBy orderBy(String col1, Order order1, String col2, Order order2, String col3, Order order3) {
		return this.orderBy.add(col1, order1, col2, order2, col3, order3);
	}
	
	@Override
	public FluentOrderBy orderBy(Column column, Column... columns) {
		return this.orderBy.add(column, columns);
	}
	
	@Override
	public FluentOrderBy orderBy(String column, String... columns) {
		return this.orderBy.add(column, columns);
	}
	
	@Override
	public LimitChain limit(int value) {
		return this.limit.setValue(value);
	}
	
	public interface FluentSelect extends SelectChain<FluentSelect>, FromAware, QueryProvider {
		
	}
	
	public interface FluentFrom extends JoinChain<FluentFrom>, WhereAware, GroupByAware, OrderByAware, LimitAware, QueryProvider {
		
	}
	
	public interface FluentWhere extends CriteriaChain<FluentWhere>, GroupByAware, OrderByAware, LimitAware, QueryProvider {
		
	}
	
	public interface FluentGroupBy extends GroupByChain<FluentGroupBy>, HavingAware, OrderByAware, LimitAware, QueryProvider {
		
	}
	
	public interface FluentHaving extends CriteriaChain<FluentHaving>, OrderByAware, LimitAware, QueryProvider {
		
	}
	
	public interface FluentOrderBy extends OrderByChain<FluentOrderBy>, LimitAware, QueryProvider {
		
	}
	
	public interface FluentLimit extends LimitChain<FluentLimit>, QueryProvider {
		
	}
}
