package org.codefilarete.stalactite.query.builder;

import java.util.Set;

import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.QueryStatement;
import org.codefilarete.tool.StringAppender;

/**
 * Factory of {@link PseudoTableSQLBuilder} 
 * 
 * @author Guillaume Mary
 */
public class PseudoTableSQLBuilderFactory {
	
	private static final String UNION_ALL_SEPARATOR = " union all ";
	
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
			appendTo(new StringSQLAppender(result, new DMLNameProvider(queryStatement.getAliases()::get)));
			return result.toString();
		}
		
		@Override
		public ExpandableSQLAppender toPreparableSQL() {
			ExpandableSQLAppender expandableSQLAppender = new ExpandableSQLAppender(querySQLBuilderFactory.getParameterBinderRegistry(), new DMLNameProvider(queryStatement.getAliases()::get));
			appendTo(expandableSQLAppender);
			return expandableSQLAppender;
		}
		
		public void appendTo(SQLAppender preparedSQLAppender) {
			Set<Query> queries = queryStatement.getQueries();
			queries.forEach(query -> {
				querySQLBuilderFactory.queryBuilder(query).appendTo(preparedSQLAppender);
				preparedSQLAppender.cat(UNION_ALL_SEPARATOR);
			});
			preparedSQLAppender.removeLastChars(UNION_ALL_SEPARATOR.length());
		}
		
		private class UnionGenerator {
			
			private final SQLAppender sqlAppender;
			
			private UnionGenerator(SQLAppender sqlAppender) {
				this.sqlAppender = sqlAppender;
			}
			
			private void cat(Query query) {
				QuerySQLBuilder unionBuilder = querySQLBuilderFactory.queryBuilder(query);
				unionBuilder.appendTo(sqlAppender);
			}
		}
	}
}
