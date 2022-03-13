package org.codefilarete.stalactite.query.builder;

import org.codefilarete.tool.StringAppender;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.persistence.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.stalactite.persistence.sql.ddl.structure.Column;
import org.codefilarete.stalactite.query.builder.OperatorBuilder.PreparedSQLWrapper;
import org.codefilarete.stalactite.query.builder.OperatorBuilder.StringAppenderWrapper;
import org.codefilarete.stalactite.query.model.GroupBy;
import org.codefilarete.stalactite.query.model.Having;
import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.query.model.OrderBy;
import org.codefilarete.stalactite.query.model.OrderBy.OrderedColumn;
import org.codefilarete.stalactite.query.model.OrderByChain.Order;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.QueryProvider;

/**
 * Builder of SQL from {@link Query}.
 * 
 * @author Guillaume Mary
 * @see #toSQL()
 * @see #toPreparedSQL(ColumnBinderRegistry) 
 */
public class SQLQueryBuilder implements SQLBuilder, PreparedSQLBuilder {
	
	private final DMLNameProvider dmlNameProvider;
	private final Query query;
	private final SelectBuilder selectBuilder;
	private final FromBuilder fromBuilder;
	private final WhereBuilder whereBuilder;
	private final WhereBuilder havingBuilder;
	
	/**
	 * Constructor to be combined with result of {@link Query} methods for a short writing, because it avoids calling {@link QueryProvider#getQuery()} 
	 * 
	 * @param query a {@link QueryProvider}
	 */
	public SQLQueryBuilder(QueryProvider query) {
		this(query.getQuery());
	}
	
	/**
	 * Main constructor
	 * 
	 * @param query a {@link Query}
	 */
	public SQLQueryBuilder(Query query) {
		this.dmlNameProvider = new DMLNameProvider(query.getFromSurrogate().getTableAliases()::get);
		this.query = query;
		this.selectBuilder = new SelectBuilder(query.getSelectSurrogate(), dmlNameProvider);
		this.fromBuilder = new FromBuilder(query.getFromSurrogate());
		this.whereBuilder = new WhereBuilder(query.getWhere(), dmlNameProvider);
		this.havingBuilder = new WhereBuilder(query.getHavingSurrogate(), dmlNameProvider);
	}
	
	/**
	 * Creates a String from Query given at construction time.
	 * <strong>SQL contains criteria values which may not be a good idea to be executed because it is exposed to SQL injection. Please don't run
	 * the returned SQL, else you know what you're doing, use it for debugging purpose for instance.</strong>
	 * One may prefer {@link #toPreparedSQL(ColumnBinderRegistry)}
	 * 
	 * @return the SQL represented by Query given at construction time
	 */
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
	
	/**
	 * Creates a {@link PreparedSQL} from Query given at construction time.
	 * 
	 * @return a {@link PreparedSQL} from Query given at construction time
	 */
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
