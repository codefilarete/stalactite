package org.codefilarete.stalactite.query.builder;

import java.util.Set;

import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.builder.SQLAppender.SubSQLAppender;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.QueryStatement;
import org.codefilarete.stalactite.sql.DMLNameProviderFactory;

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
		private final DMLNameProviderFactory dmlNameProviderFactory;
		
		public PseudoTableSQLBuilder(QueryStatement queryStatement, QuerySQLBuilderFactory querySQLBuilderFactory) {
			this.queryStatement = queryStatement;
			this.querySQLBuilderFactory = querySQLBuilderFactory;
			this.dmlNameProviderFactory = querySQLBuilderFactory.getDmlNameProviderFactory();
		}
		
		@Override
		public CharSequence toSQL() {
			StringSQLAppender result = new StringSQLAppender(dmlNameProviderFactory.build(queryStatement.getAliases()::get));
			appendTo(result);
			return result.getSQL();
		}
		
		@Override
		public ExpandableSQLAppender toPreparableSQL() {
			ExpandableSQLAppender expandableSQLAppender = new ExpandableSQLAppender(querySQLBuilderFactory.getParameterBinderRegistry(), dmlNameProviderFactory.build(queryStatement.getAliases()::get));
			appendTo(expandableSQLAppender);
			return expandableSQLAppender;
		}
		
		public void appendTo(SQLAppender preparedSQLAppender) {
			Set<Query> queries = queryStatement.getQueries();
			queries.forEach(query -> {
				QuerySQLBuilder sqlBuilder = querySQLBuilderFactory.queryBuilder(query);
				SubSQLAppender subAppender = preparedSQLAppender.newSubPart(dmlNameProviderFactory.build(query.getFromSurrogate().getTableAliases()::get));
				sqlBuilder.appendTo(subAppender);
				subAppender.close();
				preparedSQLAppender.cat(UNION_ALL_SEPARATOR);
			});
			preparedSQLAppender.removeLastChars(UNION_ALL_SEPARATOR.length());
		}
	}
}
