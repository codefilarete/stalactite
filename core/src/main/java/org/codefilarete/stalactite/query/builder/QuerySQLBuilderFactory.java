package org.codefilarete.stalactite.query.builder;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.query.builder.FromSQLBuilderFactory.FromSQLBuilder;
import org.codefilarete.stalactite.query.builder.FunctionSQLBuilderFactory.FunctionSQLBuilder;
import org.codefilarete.stalactite.query.builder.SQLAppender.SubSQLAppender;
import org.codefilarete.stalactite.query.builder.SelectSQLBuilderFactory.SelectSQLBuilder;
import org.codefilarete.stalactite.query.builder.WhereSQLBuilderFactory.WhereSQLBuilder;
import org.codefilarete.stalactite.query.model.AbstractCriterion;
import org.codefilarete.stalactite.query.model.Criteria;
import org.codefilarete.stalactite.query.model.GroupBy;
import org.codefilarete.stalactite.query.model.Having;
import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.query.model.OrderBy;
import org.codefilarete.stalactite.query.model.OrderBy.OrderedColumn;
import org.codefilarete.stalactite.query.model.OrderByChain.Order;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.QueryStatement;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.query.model.Union;
import org.codefilarete.stalactite.query.model.ValuedVariable;
import org.codefilarete.stalactite.query.model.operator.SQLFunction;
import org.codefilarete.stalactite.sql.DMLNameProviderFactory;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.JavaTypeToSqlTypeMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.Reflections;

/**
 * Factory for {@link QuerySQLBuilder}.
 * Made to let one override SQL generators and make them take into account special objects like particular {@link Column},
 * {@link org.codefilarete.stalactite.sql.ddl.structure.Table} or {@link org.codefilarete.stalactite.query.model.operator.SQLFunction}
 * and whatsoever, because Stalactite SQL builders only generate SQL for known object type, through "if instanceof"
 * mechanism, which hardly let one extends it, making users depending on project release for new feature / function / operators.
 * 
 * A default one is available through {@link Dialect#getQuerySQLBuilderFactory()}.
 * 
 * @author Guillaume Mary
 */
public class QuerySQLBuilderFactory {
	
	private final ColumnBinderRegistry parameterBinderRegistry;
	private final SelectSQLBuilderFactory selectBuilderFactory;
	private final FromSQLBuilderFactory fromBuilderFactory;
	private final WhereSQLBuilderFactory whereBuilderFactory;
	private final WhereSQLBuilderFactory havingBuilderFactory;
	private final FunctionSQLBuilderFactory functionSQLBuilderFactory;
	private final DMLNameProviderFactory dmlNameProviderFactory;
	
	/**
	 * Main constructor
	 *
	 * @param dmlNameProviderFactory factory for SQL name provider
	 * @param parameterBinderRegistry necessary while using {@link PreparableSQLBuilder#toPreparableSQL()} after calling {@link #queryBuilder(Query)}
	 * @param selectBuilderFactory factory for select clause
	 * @param fromBuilderFactory factory for from clause
	 * @param whereBuilderFactory factory for where clause
	 * @param havingBuilderFactory factory for having clause
	 */
	public QuerySQLBuilderFactory(DMLNameProviderFactory dmlNameProviderFactory,
								  ColumnBinderRegistry parameterBinderRegistry,
								  SelectSQLBuilderFactory selectBuilderFactory,
								  FromSQLBuilderFactory fromBuilderFactory,
								  WhereSQLBuilderFactory whereBuilderFactory,
								  WhereSQLBuilderFactory havingBuilderFactory,
								  FunctionSQLBuilderFactory functionSQLBuilderFactory) {
		this.dmlNameProviderFactory = dmlNameProviderFactory;
		this.parameterBinderRegistry = parameterBinderRegistry;
		this.selectBuilderFactory = selectBuilderFactory;
		this.fromBuilderFactory = fromBuilderFactory;
		this.whereBuilderFactory = whereBuilderFactory;
		this.havingBuilderFactory = havingBuilderFactory;
		this.functionSQLBuilderFactory = functionSQLBuilderFactory;
	}
	
	/**
	 * A default constructor that uses default factories, use mainly for test or default behavior (not related to a database vendor)
	 * 
	 * @param javaTypeToSqlTypeMapping a {@link Query}
	 */
	public QuerySQLBuilderFactory(JavaTypeToSqlTypeMapping javaTypeToSqlTypeMapping, DMLNameProviderFactory dmlNameProviderFactory, ColumnBinderRegistry parameterBinderRegistry) {
		this.dmlNameProviderFactory = dmlNameProviderFactory;
		this.parameterBinderRegistry = parameterBinderRegistry;
		this.selectBuilderFactory = new SelectSQLBuilderFactory(javaTypeToSqlTypeMapping);
		this.fromBuilderFactory = new FromSQLBuilderFactory();
		this.whereBuilderFactory = new WhereSQLBuilderFactory(javaTypeToSqlTypeMapping, parameterBinderRegistry);
		this.havingBuilderFactory = new WhereSQLBuilderFactory(javaTypeToSqlTypeMapping, parameterBinderRegistry);
		this.functionSQLBuilderFactory = new FunctionSQLBuilderFactory(javaTypeToSqlTypeMapping);
	}
	
	public ColumnBinderRegistry getParameterBinderRegistry() {
		return parameterBinderRegistry;
	}
	
	public SelectSQLBuilderFactory getSelectBuilderFactory() {
		return selectBuilderFactory;
	}
	
	public FromSQLBuilderFactory getFromBuilderFactory() {
		return fromBuilderFactory;
	}
	
	public WhereSQLBuilderFactory getWhereBuilderFactory() {
		return whereBuilderFactory;
	}
	
	public WhereSQLBuilderFactory getHavingBuilderFactory() {
		return havingBuilderFactory;
	}
	
	public FunctionSQLBuilderFactory getFunctionSQLBuilderFactory() {
		return functionSQLBuilderFactory;
	}
	
	public DMLNameProviderFactory getDmlNameProviderFactory() {
		return dmlNameProviderFactory;
	}
	
	public QuerySQLBuilder queryBuilder(Query query, Iterable<AbstractCriterion> where) {
		if (where.iterator().hasNext()) {    // prevents from empty where causing malformed SQL
			query.getWhere().and(where);
		}
		return queryBuilder(query);
	}
	
	public QuerySQLBuilder queryBuilder(Query query, Iterable<AbstractCriterion> where, Map<? extends Selectable<?>, ? extends Selectable<?>> columnClones) {
		if (where.iterator().hasNext()) {    // prevents from empty where causing malformed SQL
			Criteria.copy(where, query.getWhere(), columnClones::get);
		}
		return queryBuilder(query);
	}
	
	public QuerySQLBuilder queryBuilder(Query query) {
		DMLNameProvider dmlNameProvider = dmlNameProviderFactory.build(query.getFromDelegate().getTableAliases()::get);
		return new QuerySQLBuilder(query,
				dmlNameProvider,
				selectBuilderFactory.queryBuilder(query.getSelectDelegate(), dmlNameProvider),
				fromBuilderFactory.fromBuilder(query.getFromDelegate(), dmlNameProvider, this),
				whereBuilderFactory.whereBuilder(query.getWhereDelegate(), dmlNameProvider),
				havingBuilderFactory.whereBuilder(query.getHavingDelegate(), dmlNameProvider),
				functionSQLBuilderFactory.functionSQLBuilder(dmlNameProvider),
				parameterBinderRegistry);
	}
	
	public UnionSQLBuilder unionBuilder(Union union) {
		return new UnionSQLBuilder(union, this);
	}
	
	public QueryStatementSQLBuilder queryStatementBuilder(QueryStatement queryStatement) {
		if (queryStatement instanceof Query) {
			return queryBuilder((Query) queryStatement);
		} else if (queryStatement instanceof Union) {
			return unionBuilder((Union) queryStatement);
		} else {
			throw new IllegalArgumentException("Unsupported query statement type: " + Reflections.toString(queryStatement.getClass()));
		}
	}
	
	public interface QueryStatementSQLBuilder extends SQLBuilder, PreparableSQLBuilder {
		
		void appendTo(SQLAppender sqlWrapper);
	}
	
	/**
	 * Transforms a {@link Query} object to an SQL {@link String}.
	 * Requires some {@link SelectSQLBuilder}, {@link FromSQLBuilder}, {@link WhereSQLBuilder}.
	 *
	 * @see #toSQL()
	 * @see #toPreparableSQL()
	 */
	public static class QuerySQLBuilder implements QueryStatementSQLBuilder {
		
		private final Query query;
		private final DMLNameProvider dmlNameProvider;
		private final SelectSQLBuilder selectSQLBuilder;
		private final FromSQLBuilder fromSqlBuilder;
		private final WhereSQLBuilder whereSqlBuilder;
		private final WhereSQLBuilder havingBuilder;
		private final FunctionSQLBuilder functionSQLBuilder;
		private final ColumnBinderRegistry parameterBinderRegistry;
		
		public QuerySQLBuilder(Query query,
							   DMLNameProvider dmlNameProvider,
							   SelectSQLBuilder selectSQLBuilder,
							   FromSQLBuilder fromSqlBuilder,
							   WhereSQLBuilder whereSqlBuilder,
							   WhereSQLBuilder havingBuilder,
							   FunctionSQLBuilder functionSQLBuilder,
							   ColumnBinderRegistry parameterBinderRegistry) {
			this.query = query;
			this.dmlNameProvider = dmlNameProvider;
			this.selectSQLBuilder = selectSQLBuilder;
			this.fromSqlBuilder = fromSqlBuilder;
			this.whereSqlBuilder = whereSqlBuilder;
			this.havingBuilder = havingBuilder;
			this.functionSQLBuilder = functionSQLBuilder;
			this.parameterBinderRegistry = parameterBinderRegistry;
		}
		
		/**
		 * Creates a String from Query given at construction time.
		 * <strong>SQL contains criteria values which may not be a good idea to be executed because it is exposed to SQL injection. Please don't run
		 * the returned SQL, else you know what you're doing, use it for debugging purpose for instance.</strong>
		 * One may prefer {@link #toPreparableSQL()}
		 *
		 * @return the SQL represented by Query given at construction time
		 */
		@Override
		public String toSQL() {
			StringSQLAppender result = new StringSQLAppender(dmlNameProvider);
			appendTo(result);
			return result.getSQL();
		}
			
		/**
		 * Creates a {@link PreparedSQL} from Query given at construction time.
		 *
		 * @return a {@link PreparedSQL} from Query given at construction time
		 */
		@Override
		public ExpandableSQLAppender toPreparableSQL() {
			ExpandableSQLAppender preparedSQLAppender = new ExpandableSQLAppender(parameterBinderRegistry, dmlNameProvider);
			appendTo(preparedSQLAppender);
			return preparedSQLAppender;
		}
		
		@Override
		public void appendTo(SQLAppender sqlWrapper) {
			sqlWrapper.cat("select ", selectSQLBuilder.toSQL());
			sqlWrapper.cat(" from ");
			fromSqlBuilder.appendTo(sqlWrapper);
			if (!query.getWhereDelegate().getConditions().isEmpty()) {
				sqlWrapper.cat(" where ");
				whereSqlBuilder.appendTo(sqlWrapper);
			}
			
			GroupBy groupBy = query.getGroupByDelegate();
			if (!groupBy.getGroups().isEmpty()) {
				cat(groupBy, sqlWrapper.cat(" group by "));
			}
			
			Having having = query.getHavingDelegate();
			if (!having.getConditions().isEmpty()) {
				sqlWrapper.cat(" having ");
				havingBuilder.appendTo(sqlWrapper);
			}
			
			OrderBy orderBy = query.getOrderByDelegate();
			if (!orderBy.getColumns().isEmpty()) {
				sqlWrapper.cat(" order by ");
				cat(orderBy, sqlWrapper);
			}
			
			Limit limit = query.getLimitDelegate();
			if (limit.getCount() != null) {
				sqlWrapper.cat(" limit ");
				sqlWrapper.catValue(new ValuedVariable<>(limit.getCount()));
				if (limit.getOffset() != null) {
					sqlWrapper.cat(" offset ");
					sqlWrapper.catValue(new ValuedVariable<>(limit.getOffset()));
				}
			}
		}
		
		private void cat(OrderBy orderBy, SQLAppender sql) {
			for (OrderedColumn o : orderBy) {
				Object column = o.getColumn();
				cat(column, sql);
				sql.catIf(o.getOrder() != null, o.getOrder() == Order.ASC ? " asc" : " desc").cat(", ");
			}
			sql.removeLastChars(2);
		}
		
		private void cat(GroupBy groupBy, SQLAppender sql) {
			for (Object o : groupBy) {
				cat(o, sql);
				sql.cat(", ");
			}
			sql.removeLastChars(2);
		}
		
		private void cat(Object column, SQLAppender sql) {
			if (column instanceof String) {
				sql.cat((String) column);
			} else if (column instanceof SQLFunction) {
				functionSQLBuilder.cat((SQLFunction) column, sql);
			} else if (column instanceof Selectable) {
				sql.catColumn((Selectable<?>) column);
			}
		}
	}
	
	/**
	 * {@link SQLBuilder} for {@link QueryStatement} in a From clause. Based on the rendering of a union, but extended to {@link QueryStatement}
	 * to be able to print also simple queries in a From.
	 *
	 * @author Guillaume Mary
	 */
	public static class UnionSQLBuilder implements QueryStatementSQLBuilder {
		
		private static final String UNION_ALL_SEPARATOR = " union all ";
		
		private final Union union;
		private final QuerySQLBuilderFactory querySQLBuilderFactory;
		private final DMLNameProviderFactory dmlNameProviderFactory;
		
		public UnionSQLBuilder(Union union, QuerySQLBuilderFactory querySQLBuilderFactory) {
			this.union = union;
			this.querySQLBuilderFactory = querySQLBuilderFactory;
			this.dmlNameProviderFactory = querySQLBuilderFactory.getDmlNameProviderFactory();
		}
		
		@Override
		public CharSequence toSQL() {
			// Note that we don't need (and can't give) aliases here because they are took on Union queries
			StringSQLAppender result = new StringSQLAppender(dmlNameProviderFactory.build(Collections.emptyMap()));
			appendTo(result);
			return result.getSQL();
		}
		
		@Override
		public ExpandableSQLAppender toPreparableSQL() {
			// Note that we don't need (and can't give) aliases here because they are took on Union queries
			ExpandableSQLAppender expandableSQLAppender = new ExpandableSQLAppender(
					querySQLBuilderFactory.getParameterBinderRegistry(),
					dmlNameProviderFactory.build(Collections.emptyMap()));
			appendTo(expandableSQLAppender);
			return expandableSQLAppender;
		}
		
		@Override
		public void appendTo(SQLAppender preparedSQLAppender) {
			Set<Query> queries = union.getQueries();
			queries.forEach(query -> {
				QuerySQLBuilder sqlBuilder = querySQLBuilderFactory.queryBuilder(query);
				SubSQLAppender subAppender = preparedSQLAppender.newSubPart(dmlNameProviderFactory.build(query.getFromDelegate().getTableAliases()::get));
				sqlBuilder.appendTo(subAppender);
				subAppender.close();
				preparedSQLAppender.cat(UNION_ALL_SEPARATOR);
			});
			preparedSQLAppender.removeLastChars(UNION_ALL_SEPARATOR.length());
		}
	}
}
