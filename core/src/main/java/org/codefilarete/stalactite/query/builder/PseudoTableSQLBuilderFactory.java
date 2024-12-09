package org.codefilarete.stalactite.query.builder;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.QueryStatement;
import org.codefilarete.stalactite.sql.statement.binder.PreparedStatementWriter;
import org.codefilarete.tool.StringAppender;
import org.codefilarete.tool.trace.ModifiableInt;

/**
 * Factory of {@link PseudoTableSQLBuilder} 
 * 
 * @author Guillaume Mary
 */
public class PseudoTableSQLBuilderFactory {
	
	private static final String UNION_ALL_SEPARATOR = ") union all (";
	
	public PseudoTableSQLBuilderFactory() {
	}
	
	public PseudoTableSQLBuilder pseudoTableBuilder(QueryStatement union, QuerySQLBuilderFactory querySQLBuilderFactory) {
		return new PseudoTableSQLBuilder(union, querySQLBuilderFactory);
	}
	
	/**
	 * {@link SQLBuilder} for {@link QueryStatement} in a From clause. Based on the rendering of a union, but extended to {@link QueryStatement}
	 * to be able to print also simple queries in a From.
	 * 
	 * @author Guillaume Mary
	 */
	public static class PseudoTableSQLBuilder implements SQLBuilder, PreparableSQLBuilder {
		
		private final QueryStatement queryStatement;
		private final QuerySQLBuilderFactory querySQLBuilderFactory;
		
		public PseudoTableSQLBuilder(QueryStatement queryStatement, QuerySQLBuilderFactory querySQLBuilderFactory) {
			this.queryStatement = queryStatement;
			this.querySQLBuilderFactory = querySQLBuilderFactory;
		}
		
		@Override
		public CharSequence toSQL() {
			StringAppender result = new StringAppender(500);
			toSQL(new StringSQLAppender(result, new DMLNameProvider(queryStatement.getAliases()::get)));
			return result.toString();
		}
		
		public void toSQL(SQLAppender sqlAppender) {
			UnionGenerator sql = new UnionGenerator(sqlAppender);
			queryStatement.getQueries().forEach(query -> {
				sql.cat(query);
				sqlAppender.cat(" union all ");
			});
			sqlAppender.removeLastChars(" union all ".length());
		}
		
		@Override
		public ExpandableSQLAppender toPreparableSQL() {
			ExpandableSQLAppender expandableSQLAppender = new ExpandableSQLAppender(querySQLBuilderFactory.getParameterBinderRegistry(), new DMLNameProvider(queryStatement.getAliases()::get));
			appendTo(expandableSQLAppender);
			
//			PreparedSQL preparedSQL = new PreparedSQL(preparedSQLAppender.getSQL(), preparedSQLAppender.getParameterBinders());
//			preparedSQL.setValues(preparedSQLAppender.getValues());
			return expandableSQLAppender;
		}
		
		public void appendTo(ExpandableSQLAppender preparedSQLAppender) {
			Set<Query> queries = queryStatement.getQueries();
			Map<Integer, PreparedStatementWriter> parameterBinders = new HashMap<>();
			Map<Integer, Object> values = new HashMap<>();
			ModifiableInt parameterIndex = new ModifiableInt(1);
			preparedSQLAppender.cat("(");
			queries.forEach(query -> {
				querySQLBuilderFactory.queryBuilder(query).appendTo(preparedSQLAppender);
				preparedSQLAppender.cat(UNION_ALL_SEPARATOR);
//				preparedSQLAppender.getValues().values().forEach(value -> {
//					// since ids are all
//					values.put(parameterIndex.getValue(), value);
//					// NB: parameter binder is expected to be always the same since we always put ids
//					parameterBinders.put(parameterIndex.getValue(),
//							preparedSQLAppender.getParameterBinder(1 + parameterIndex.getValue() % preparedSql.getValues().size()));
//					parameterIndex.increment();
//				});
			});
			preparedSQLAppender.removeLastChars(UNION_ALL_SEPARATOR.length());
			preparedSQLAppender.cat(")");
		}
		
		private class UnionGenerator {
			
			private final SQLAppender sqlAppender;
			
			private UnionGenerator(SQLAppender sqlAppender) {
				this.sqlAppender = sqlAppender;
			}
			
			private void cat(Query query) {
				QuerySQLBuilder unionBuilder = querySQLBuilderFactory.queryBuilder(query);
				unionBuilder.toSQL(sqlAppender);
			}
		}
	}
}
