package org.gama.stalactite.query.builder;

import javax.annotation.Nonnull;

import org.gama.lang.StringAppender;
import org.gama.sql.dml.PreparedSQL;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.query.builder.OperatorBuilder.PreparedSQLWrapper;
import org.gama.stalactite.query.builder.OperatorBuilder.StringAppenderWrapper;
import org.gama.stalactite.query.model.GroupBy;
import org.gama.stalactite.query.model.Having;
import org.gama.stalactite.query.model.Limit;
import org.gama.stalactite.query.model.OrderBy;
import org.gama.stalactite.query.model.OrderBy.OrderedColumn;
import org.gama.stalactite.query.model.OrderByChain.Order;
import org.gama.stalactite.query.model.Query;
import org.gama.stalactite.query.model.QueryProvider;

/**
 * @author Guillaume Mary
 */
public class QueryBuilder implements SQLBuilder, PreparedSQLBuilder {
	
	private final DMLNameProvider dmlNameProvider;
	private final Query query;
	private final SelectBuilder selectBuilder;
	private final FromBuilder fromBuilder;
	private final WhereBuilder whereBuilder;
	private final WhereBuilder havingBuilder;
	
	/**
	 * Constructor to be combined with result of {@link Query} methods for a short writing, because it avoids calling {@link QueryProvider#getSelectQuery()} 
	 * 
	 * @param query a {@link QueryProvider}
	 */
	public QueryBuilder(@Nonnull QueryProvider query) {
		this(query.getSelectQuery());
	}
	
	/**
	 * Main constructor
	 * 
	 * @param query a {@link Query}
	 */
	public QueryBuilder(@Nonnull Query query) {
		this.dmlNameProvider = new DMLNameProvider(query.getFromSurrogate().getTableAliases());
		this.query = query;
		this.selectBuilder = new SelectBuilder(query.getSelectSurrogate(), dmlNameProvider);
		this.fromBuilder = new FromBuilder(query.getFromSurrogate());
		this.whereBuilder = new WhereBuilder(query.getWhere(), dmlNameProvider);
		this.havingBuilder = new WhereBuilder(query.getHavingSurrogate(), dmlNameProvider);
	}
	
	@Override
	public String toSQL() {
		StringAppender sql = new StringAppender(500);
		
		sql.cat("select ", selectBuilder.toSQL());
		sql.cat(" from ", fromBuilder.toSQL());
		if (!query.getWhereSurrogate().getConditions().isEmpty()) {
			sql.cat(" where ", whereBuilder.toSQL());
		}
		
		GroupBy groupBy = query.getGroupBySurrogate();
		if (!groupBy.getGroups().isEmpty()) {
			cat(groupBy, sql.cat(" group by "));
		}
		
		Having having = query.getHavingSurrogate();
		if (!having.getConditions().isEmpty()) {
			havingBuilder.appendSQL(sql.cat(" having "));
		}
		
		OrderBy orderBy = query.getOrderBySurrogate();
		if (!orderBy.getColumns().isEmpty()) {
			cat(orderBy, sql.cat(" order by "));
		}
		
		Limit limit = query.getLimitSurrogate();
		sql.catIf(limit.getValue() != null, " limit ", limit.getValue());
		
		return sql.toString();
	}
	
	@Override
	public PreparedSQL toPreparedSQL(ColumnBinderRegistry parameterBinderRegistry) {
		StringAppender sql = new StringAppender(500);
		
		PreparedSQLWrapper preparedSQLWrapper = new PreparedSQLWrapper(new StringAppenderWrapper(sql, dmlNameProvider), parameterBinderRegistry, dmlNameProvider);
		
		sql.cat("select ", selectBuilder.toSQL());
		sql.cat(" from ", fromBuilder.toSQL());
		if (!query.getWhereSurrogate().getConditions().isEmpty()) {
			sql.cat(" where ");
			whereBuilder.toPreparedSQL(preparedSQLWrapper);
		}
		
		GroupBy groupBy = query.getGroupBySurrogate();
		if (!groupBy.getGroups().isEmpty()) {
			cat(groupBy, sql.cat(" group by "));
		}
		
		Having having = query.getHavingSurrogate();
		if (!having.getConditions().isEmpty()) {
			sql.cat(" having ");
			havingBuilder.toPreparedSQL(preparedSQLWrapper);
		}
		
		OrderBy orderBy = query.getOrderBySurrogate();
		if (!orderBy.getColumns().isEmpty()) {
			cat(orderBy, sql.cat(" order by "));
		}
		
		Limit limit = query.getLimitSurrogate();
		if (limit.getValue() != null) {
			sql.cat(" limit ");
			preparedSQLWrapper.catValue(null, limit.getValue());
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
