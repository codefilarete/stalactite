package org.codefilarete.stalactite.query.builder;

import org.codefilarete.stalactite.query.builder.QuerySQLBuilderFactory.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Union;
import org.codefilarete.tool.StringAppender;

/**
 * @author Guillaume Mary
 */
public class UnionSQLBuilderFactory {
	
	public UnionSQLBuilderFactory() {
	}
	
	public UnionSQLBuilder unionBuilder(Union union, QuerySQLBuilderFactory querySQLBuilderFactory) {
		return new UnionSQLBuilder(union, querySQLBuilderFactory);
	}
	
	public static class UnionSQLBuilder implements SQLBuilder {
		
		private final Union union;
		private final QuerySQLBuilderFactory querySQLBuilderFactory;
		
		public UnionSQLBuilder(Union union, QuerySQLBuilderFactory querySQLBuilderFactory) {
			this.union = union;
			this.querySQLBuilderFactory = querySQLBuilderFactory;
		}
		
		@Override
		public CharSequence toSQL() {
			StringAppender sql = new UnionGenerator();
			return sql.ccat(union.getQueries(), " union all ").toString();
		}
		
		private class UnionGenerator extends StringAppender {
			
			@Override
			public StringAppender cat(Object o) {
				if (o instanceof Query) {
					return cat((Query) o);
				} else {
					return super.cat(o);
				}
			}
			
			private StringAppender cat(Query query) {
				QuerySQLBuilder unionBuilder = querySQLBuilderFactory.queryBuilder(query);
				return cat(unionBuilder.toSQL());
			}
		}
	}
}
