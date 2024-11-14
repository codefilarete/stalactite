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
 * @author Guillaume Mary
 */
public class UnionSQLBuilderFactory {
	
	private static final String UNION_ALL_SEPARATOR = ") union all (";
	
	public UnionSQLBuilderFactory() {
	}
	
	public UnionSQLBuilder unionBuilder(QueryStatement union, QuerySQLBuilderFactory querySQLBuilderFactory) {
		return new UnionSQLBuilder(union, querySQLBuilderFactory);
	}
	
	public static class UnionSQLBuilder implements SQLBuilder, PreparedSQLBuilder {
		
		private final QueryStatement union;
		private final QuerySQLBuilderFactory querySQLBuilderFactory;
		
		public UnionSQLBuilder(QueryStatement union, QuerySQLBuilderFactory querySQLBuilderFactory) {
			this.union = union;
			this.querySQLBuilderFactory = querySQLBuilderFactory;
		}
		
		@Override
		public CharSequence toSQL() {
			StringAppender result = new StringAppender(500);
			toSQL(new StringAppenderWrapper(result, new DMLNameProvider(union.getAliases()::get)));
			return result.toString();
		}
		
		public void toSQL(SQLAppender sqlAppender) {
			UnionGenerator sql = new UnionGenerator(sqlAppender);
			union.getQueries().forEach(query -> {
				sql.cat(query);
				sqlAppender.cat(" union all ");
			});
			sqlAppender.removeLastChars(" union all ".length());
		}
		
		@Override
		public PreparedSQL toPreparedSQL() {
			Set<Query> queries = union.getQueries();
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
			Set<Query> queries = union.getQueries();
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
