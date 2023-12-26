package org.codefilarete.stalactite.query.builder;

import java.util.IdentityHashMap;

import org.codefilarete.stalactite.query.builder.FromSQLBuilderFactory.FromSQLBuilder;
import org.codefilarete.stalactite.query.builder.SelectSQLBuilderFactory.SelectSQLBuilder;
import org.codefilarete.stalactite.query.builder.WhereSQLBuilderFactory.WhereSQLBuilder;
import org.codefilarete.stalactite.query.model.ColumnCriterion;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.GroupBy;
import org.codefilarete.stalactite.query.model.Having;
import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.query.model.OrderBy;
import org.codefilarete.stalactite.query.model.OrderBy.OrderedColumn;
import org.codefilarete.stalactite.query.model.OrderByChain.Order;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.StringAppender;

/**
 * Factory for {@link QuerySQLBuilder}.
 * Made to let one override SQL generators and make them take into account special objects like particular {@link Column},
 * {@link org.codefilarete.stalactite.sql.ddl.structure.Table} or {@link org.codefilarete.stalactite.query.model.operator.SQLFunction}
 * and whatsoever, because Stalactite SQL builders only generate SQL for known object type, through "if instanceof"
 * mechanism, which hardly let one extends it, making users depending on project release for new feature / function / operators.
 * 
 * A default one is available through {@link Dialect#getQuerySQLBuilderFactory()} is and changeable through
 * {@link Dialect#setQuerySQLBuilderFactory(QuerySQLBuilderFactory)} which then let one give its own implementation of {@link QuerySQLBuilder}.
 * 
 * @author Guillaume Mary
 */
public class QuerySQLBuilderFactory {
	
	private final ColumnBinderRegistry parameterBinderRegistry;
	private final SelectSQLBuilderFactory selectBuilder;
	private final FromSQLBuilderFactory fromSqlBuilder;
	private final WhereSQLBuilderFactory whereSqlBuilder;
	private final WhereSQLBuilderFactory havingBuilder;
	
	/**
	 * Main constructor
	 *
	 * @param parameterBinderRegistry necessary while using {@link PreparedSQLBuilder#toPreparedSQL()} after calling {@link #queryBuilder(Query)}
	 * @param selectBuilder factory for select clause
	 * @param fromSqlBuilder factory for from clause
	 * @param whereSqlBuilder factory for where clause
	 * @param havingBuilder factory for having clause
	 */
	public QuerySQLBuilderFactory(ColumnBinderRegistry parameterBinderRegistry,
								  SelectSQLBuilderFactory selectBuilder,
								  FromSQLBuilderFactory fromSqlBuilder,
								  WhereSQLBuilderFactory whereSqlBuilder,
								  WhereSQLBuilderFactory havingBuilder) {
		this.parameterBinderRegistry = parameterBinderRegistry;
		this.selectBuilder = selectBuilder;
		this.fromSqlBuilder = fromSqlBuilder;
		this.whereSqlBuilder = whereSqlBuilder;
		this.havingBuilder = havingBuilder;
	}
	
	/**
	 * A default constructor that uses default factories, use mainly for test or default behavior (not related to a database vendor)
	 * 
	 * @param javaTypeToSqlTypeMapping a {@link Query}
	 */
	public QuerySQLBuilderFactory(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping, ColumnBinderRegistry parameterBinderRegistry) {
		this.parameterBinderRegistry = parameterBinderRegistry;
		this.selectBuilder = new SelectSQLBuilderFactory(javaTypeToSqlTypeMapping);
		this.fromSqlBuilder = new FromSQLBuilderFactory();
		this.whereSqlBuilder = new WhereSQLBuilderFactory(javaTypeToSqlTypeMapping, parameterBinderRegistry);
		this.havingBuilder = new WhereSQLBuilderFactory(javaTypeToSqlTypeMapping, parameterBinderRegistry);
	}
	
	public ColumnBinderRegistry getParameterBinderRegistry() {
		return parameterBinderRegistry;
	}
	
	public SelectSQLBuilderFactory getSelectBuilder() {
		return selectBuilder;
	}
	
	public FromSQLBuilderFactory getFromSqlBuilder() {
		return fromSqlBuilder;
	}
	
	public WhereSQLBuilderFactory getWhereSqlBuilder() {
		return whereSqlBuilder;
	}
	
	public WhereSQLBuilderFactory getHavingBuilder() {
		return havingBuilder;
	}
	
	public QuerySQLBuilder queryBuilder(Query query, CriteriaChain<?> where) {
		if (where.iterator().hasNext()) {    // prevents from empty where causing malformed SQL
			query.getWhere().and(where);
		}
		return queryBuilder(query);
	}
	
	public QuerySQLBuilder queryBuilder(Query query, CriteriaChain<?> where, IdentityHashMap<Column, Column> columnClones) {
		if (where.iterator().hasNext()) {    // prevents from empty where causing malformed SQL
			where.forEach(criterion -> {
				if (criterion instanceof ColumnCriterion) {
					ColumnCriterion columnCriterion = ((ColumnCriterion) criterion).copyFor(columnClones.get(((ColumnCriterion) criterion).getColumn()));
					query.getWhere().and(columnCriterion);
				} else {
					query.getWhere().and(criterion);
				}
			});
		}
		return queryBuilder(query);
	}
	
	public QuerySQLBuilder queryBuilder(Query query) {
		DMLNameProvider dmlNameProvider = new DMLNameProvider(query.getFromSurrogate().getTableAliases()::get);
		return new QuerySQLBuilder(query,
				dmlNameProvider,
				selectBuilder.queryBuilder(query.getSelectSurrogate(), dmlNameProvider),
				fromSqlBuilder.fromBuilder(query.getFromSurrogate(), dmlNameProvider, this),
				whereSqlBuilder.whereBuilder(query.getWhereSurrogate(), dmlNameProvider),
				havingBuilder.whereBuilder(query.getHavingSurrogate(), dmlNameProvider),
				parameterBinderRegistry);
	}
	
	/**
	 * Transforms a {@link Query} object to an SQL {@link String}.
	 * Requires some {@link SelectSQLBuilder}, {@link FromSQLBuilder}, {@link WhereSQLBuilder}.
	 *
	 * @see #toSQL()
	 * @see #toPreparedSQL()
	 */
	public static class QuerySQLBuilder implements SQLBuilder, PreparedSQLBuilder {
		
		private final Query query;
		private final DMLNameProvider dmlNameProvider;
		private final SelectSQLBuilder selectSQLBuilder;
		private final FromSQLBuilder fromSqlBuilder;
		private final WhereSQLBuilder whereSqlBuilder;
		private final WhereSQLBuilder havingBuilder;
		private final ColumnBinderRegistry parameterBinderRegistry;
		
		public QuerySQLBuilder(Query query,
							   DMLNameProvider dmlNameProvider,
							   SelectSQLBuilder selectSQLBuilder,
							   FromSQLBuilder fromSqlBuilder,
							   WhereSQLBuilder whereSqlBuilder,
							   WhereSQLBuilder havingBuilder,
							   ColumnBinderRegistry parameterBinderRegistry) {
			this.query = query;
			this.dmlNameProvider = dmlNameProvider;
			this.selectSQLBuilder = selectSQLBuilder;
			this.fromSqlBuilder = fromSqlBuilder;
			this.whereSqlBuilder = whereSqlBuilder;
			this.havingBuilder = havingBuilder;
			this.parameterBinderRegistry = parameterBinderRegistry;
		}
		
		/**
		 * Creates a String from Query given at construction time.
		 * <strong>SQL contains criteria values which may not be a good idea to be executed because it is exposed to SQL injection. Please don't run
		 * the returned SQL, else you know what you're doing, use it for debugging purpose for instance.</strong>
		 * One may prefer {@link #toPreparedSQL()}
		 *
		 * @return the SQL represented by Query given at construction time
		 */
		@Override
		public String toSQL() {
			StringAppender sql = new StringAppender(500);
			
			sql.cat("select ", selectSQLBuilder.toSQL());
			sql.cat(" from ", fromSqlBuilder.toSQL());
			if (!query.getWhereSurrogate().getConditions().isEmpty()) {
				sql.cat(" where ", whereSqlBuilder.toSQL());
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
		public PreparedSQL toPreparedSQL() {
			StringAppender sql = new StringAppender(500);
			
			PreparedSQLWrapper preparedSQLWrapper = new PreparedSQLWrapper(new StringAppenderWrapper(sql, dmlNameProvider), parameterBinderRegistry, dmlNameProvider);
			
			sql.cat("select ", selectSQLBuilder.toSQL());
			sql.cat(" from ", fromSqlBuilder.toSQL());
			if (!query.getWhereSurrogate().getConditions().isEmpty()) {
				sql.cat(" where ");
				whereSqlBuilder.toPreparedSQL(preparedSQLWrapper);
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
}
