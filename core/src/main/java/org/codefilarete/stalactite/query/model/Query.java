package org.codefilarete.stalactite.query.model;

import java.util.Map;

import org.codefilarete.stalactite.query.api.CriteriaChain;
import org.codefilarete.stalactite.query.api.Fromable;
import org.codefilarete.stalactite.query.api.GroupByAware;
import org.codefilarete.stalactite.query.api.GroupByChain;
import org.codefilarete.stalactite.query.api.HavingAware;
import org.codefilarete.stalactite.query.api.JoinChain;
import org.codefilarete.stalactite.query.api.LimitAware;
import org.codefilarete.stalactite.query.api.LimitChain;
import org.codefilarete.stalactite.query.api.OrderByAware;
import org.codefilarete.stalactite.query.api.OrderByChain;
import org.codefilarete.stalactite.query.api.QueryProvider;
import org.codefilarete.stalactite.query.api.QueryStatement;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.query.api.UnionAware;
import org.codefilarete.stalactite.query.api.WhereAware;
import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * A support for a SQL query.
 * Final printing can be made by {@link QuerySQLBuilder}
 * 
 * @author Guillaume Mary
 * @see QuerySQLBuilder
 * @see FluentQueries
 * @see FluentQuery
 */
public class Query implements QueryStatement {
	
	private final Select selectDelegate;
	private final From fromDelegate;
	private final Where whereDelegate;
	private final GroupBy groupByDelegate;
	private final Having havingDelegate;
	private final OrderBy orderByDelegate;
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
		this.fromDelegate = fromDelegate;
		this.whereDelegate = whereDelegate;
		this.groupByDelegate = groupByDelegate;
		this.havingDelegate = havingDelegate;
		this.orderByDelegate = orderByDelegate;
		this.limitDelegate = limitDelegate;
	}
	
	/**
	 * @return a concrete implementation of a select
	 */
	public Select getSelectDelegate() {
		return selectDelegate;
	}
	
	public From getFromDelegate() {
		return fromDelegate;
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
	
	public void select(Iterable<? extends Selectable<?>> selectables) {
		selectables.forEach(this.selectDelegate::add);
	}
	
	public void select(Selectable<?> expression, Selectable<?>... expressions) {
		this.selectDelegate.add(expression, expressions);
	}
	
	public void select(String expression, Class<?> javaType) {
		this.selectDelegate.add(expression, javaType);
	}
	
	public void select(String expression, Class<?> javaType, String alias) {
		this.selectDelegate.add(expression, javaType, alias);
	}
	
	public void select(Selectable<?> column, String alias) {
		this.selectDelegate.add(column, alias);
	}
	
	public void select(Selectable<?> col1, String alias1, Selectable<?> col2, String alias2) {
		this.selectDelegate.add(col1, alias1, col2, alias2);
	}
	
	public void select(Selectable<?> col1, String alias1, Selectable<?> col2, String alias2, Selectable<?> col3, String alias3) {
		this.selectDelegate.add(col1, alias1, col2, alias2, col3, alias3);
	}
	
	public void select(Map<? extends Selectable<?>, String> aliasedColumns) {
		this.selectDelegate.add(aliasedColumns);
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
