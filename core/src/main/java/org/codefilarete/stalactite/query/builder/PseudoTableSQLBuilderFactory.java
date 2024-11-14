package org.codefilarete.stalactite.query.builder;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.QueryStatement;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
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
	public static class PseudoTableSQLBuilder implements SQLBuilder, PreparedSQLBuilder {
		
		private final QueryStatement queryStatement;
		private final QuerySQLBuilderFactory querySQLBuilderFactory;
		
		public PseudoTableSQLBuilder(QueryStatement queryStatement, QuerySQLBuilderFactory querySQLBuilderFactory) {
			this.queryStatement = queryStatement;
			this.querySQLBuilderFactory = querySQLBuilderFactory;
		}
		
		@Override
		public CharSequence toSQL() {
			StringAppender result = new StringAppender(500);
			toSQL(new StringAppenderWrapper(result, new DMLNameProvider(queryStatement.getAliases()::get)));
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
		public PreparedSQL toPreparedSQL() {
			Set<Query> queries = queryStatement.getQueries();
			StringAppender unionSql = new StringAppender();
			Map<Integer, PreparedStatementWriter> parameterBinders = new HashMap<>();
			Map<Integer, Object> values = new HashMap<>();
			ModifiableInt parameterIndex = new ModifiableInt(1);
			queries.forEach(query -> {
				PreparedSQL preparedSql = querySQLBuilderFactory.queryBuilder(query).toPreparedSQL();
				unionSql.cat(preparedSql.getSQL(), UNION_ALL_SEPARATOR);
				preparedSql.getValues().values().forEach(value -> {
					// since ids are all
					values.put(parameterIndex.getValue(), value);
					// NB: parameter binder is expected to be always the same since we always put ids
					parameterBinders.put(parameterIndex.getValue(),
							preparedSql.getParameterBinder(1 + parameterIndex.getValue() % preparedSql.getValues().size()));
					parameterIndex.increment();
				});
			});
			unionSql.cutTail(UNION_ALL_SEPARATOR.length())
					.wrap("(", ")");
			
			PreparedSQL preparedSQL = new PreparedSQL(unionSql.toString(), parameterBinders);
			preparedSQL.setValues(values);
			return preparedSQL;
		}
		
		public PreparedSQL toPreparedSQL(PreparedSQLWrapper preparedSQLWrapper) {
			Set<Query> queries = queryStatement.getQueries();
			Map<Integer, PreparedStatementWriter> parameterBinders = new HashMap<>();
			Map<Integer, Object> values = new HashMap<>();
			ModifiableInt parameterIndex = new ModifiableInt(1);
			preparedSQLWrapper.cat("(");
			queries.forEach(query -> {
				PreparedSQL preparedSql = querySQLBuilderFactory.queryBuilder(query).toPreparedSQL(preparedSQLWrapper);
				preparedSQLWrapper.cat(UNION_ALL_SEPARATOR);
				preparedSql.getValues().values().forEach(value -> {
					// since ids are all
					values.put(parameterIndex.getValue(), value);
					// NB: parameter binder is expected to be always the same since we always put ids
					parameterBinders.put(parameterIndex.getValue(),
							preparedSql.getParameterBinder(1 + parameterIndex.getValue() % preparedSql.getValues().size()));
					parameterIndex.increment();
				});
			});
			preparedSQLWrapper.removeLastChars(UNION_ALL_SEPARATOR.length());
			preparedSQLWrapper.cat(")");
			
			PreparedSQL preparedSQL = new PreparedSQL(preparedSQLWrapper.toString(), parameterBinders);
			preparedSQL.setValues(values);
			return preparedSQL;
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
