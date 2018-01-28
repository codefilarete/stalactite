package org.gama.stalactite.query.builder;

import org.gama.lang.StringAppender;
import org.gama.sql.dml.PreparedSQL;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.query.builder.OperandBuilder.PreparedSQLWrapper;
import org.gama.stalactite.query.builder.OperandBuilder.StringAppenderWrapper;
import org.gama.stalactite.query.model.GroupBy;
import org.gama.stalactite.query.model.Having;
import org.gama.stalactite.query.model.Limit;
import org.gama.stalactite.query.model.OrderBy;
import org.gama.stalactite.query.model.OrderBy.OrderedColumn;
import org.gama.stalactite.query.model.OrderByChain.Order;
import org.gama.stalactite.query.model.SelectQuery;

/**
 * @author Guillaume Mary
 */
public class SelectQueryBuilder implements SQLBuilder, PreparedSQLBuilder {
	
	private final DMLNameProvider dmlNameProvider;
	private final SelectQuery selectQuery;
	private final SelectBuilder selectBuilder;
	private final FromBuilder fromBuilder;
	private final WhereBuilder whereBuilder;
	private final WhereBuilder havingBuilder;
	
	public SelectQueryBuilder(SelectQuery selectQuery) {
		this.dmlNameProvider = new DMLNameProvider(selectQuery.getFromSurrogate().getTableAliases());
		this.selectQuery = selectQuery;
		this.selectBuilder = new SelectBuilder(selectQuery.getSelectSurrogate(), dmlNameProvider);
		this.fromBuilder = new FromBuilder(selectQuery.getFromSurrogate());
		this.whereBuilder = new WhereBuilder(selectQuery.getWhere(), dmlNameProvider);
		this.havingBuilder = new WhereBuilder(selectQuery.getHavingSurrogate(), dmlNameProvider);
	}
	
	@Override
	public String toSQL() {
		StringAppender sql = new StringAppender(500);
		
		sql.cat("select ", selectBuilder.toSQL());
		sql.cat(" from ", fromBuilder.toSQL());
		if (!selectQuery.getWhereSurrogate().getConditions().isEmpty()) {
			sql.cat(" where ", whereBuilder.toSQL());
		}
		
		GroupBy groupBy = selectQuery.getGroupBySurrogate();
		if (!groupBy.getGroups().isEmpty()) {
			cat(groupBy, sql.cat(" group by "));
		}
		
		Having having = selectQuery.getHavingSurrogate();
		if (!having.getConditions().isEmpty()) {
			havingBuilder.toSQL(sql.cat(" having "));
		}
		
		OrderBy orderBy = selectQuery.getOrderBySurrogate();
		if (!orderBy.getColumns().isEmpty()) {
			cat(orderBy, sql.cat(" order by "));
		}
		
		Limit limit = selectQuery.getLimitSurrogate();
		sql.catIf(limit.getValue() != null, " limit ", limit.getValue());
		
		return sql.toString();
	}
	
	@Override
	public PreparedSQL toPreparedSQL(ColumnBinderRegistry parameterBinderRegistry) {
		StringAppender sql = new StringAppender(500);
		
		PreparedSQLWrapper preparedSQLWrapper = new PreparedSQLWrapper(new StringAppenderWrapper(sql, dmlNameProvider), parameterBinderRegistry, dmlNameProvider);
		
		sql.cat("select ", selectBuilder.toSQL());
		sql.cat(" from ", fromBuilder.toSQL());
		if (!selectQuery.getWhereSurrogate().getConditions().isEmpty()) {
			sql.cat(" where ");
			whereBuilder.toPreparedSQL(preparedSQLWrapper);
		}
		
		GroupBy groupBy = selectQuery.getGroupBySurrogate();
		if (!groupBy.getGroups().isEmpty()) {
			cat(groupBy, sql.cat(" group by "));
		}
		
		Having having = selectQuery.getHavingSurrogate();
		if (!having.getConditions().isEmpty()) {
			sql.cat(" having ");
			havingBuilder.toPreparedSQL(preparedSQLWrapper);
		}
		
		OrderBy orderBy = selectQuery.getOrderBySurrogate();
		if (!orderBy.getColumns().isEmpty()) {
			cat(orderBy, sql.cat(" order by "));
		}
		
		Limit limit = selectQuery.getLimitSurrogate();
		if (limit.getValue() != null) {
			sql.cat(" limit ");
			preparedSQLWrapper.catValue(limit.getValue());
		}
		
		PreparedSQL result = new PreparedSQL(sql.toString(), preparedSQLWrapper.getParameterBinders());
		result.setValues(preparedSQLWrapper.getValues());
		
		return result;
	}
	
	private void cat(OrderBy orderBy, StringAppender sql) {
		for (OrderedColumn o : orderBy) {
			Object column = o.getColumn();
			cat(column, sql);
			sql.catIf(o.getOrder() != null, o.getOrder() == Order.ASC ? " asc" : " desc").cat(", ");
		}
		sql.cutTail(2);
	}
	
	private void cat(GroupBy groupBy, StringAppender sql) {
		for (Object o : groupBy) {
			cat(o, sql);
			sql.cat(", ");
		}
		sql.cutTail(2);
	}
	
	private void cat(Object column, StringAppender sql) {
		if (column instanceof String) {
			sql.cat(column);
		} else if (column instanceof Column) {
			sql.cat(dmlNameProvider.getName((Column) column));
		}
	}
}
