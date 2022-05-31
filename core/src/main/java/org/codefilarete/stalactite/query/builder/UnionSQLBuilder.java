package org.codefilarete.stalactite.query.builder;

import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Union;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.StringAppender;

/**
 * @author Guillaume Mary
 */
public class UnionSQLBuilder implements SQLBuilder, PreparedSQLBuilder {
	
	private final Union union;
	
	public UnionSQLBuilder(Union union) {
		this.union = union;
	}
	
	@Override
	public PreparedSQL toPreparedSQL(ColumnBinderRegistry parameterBinderRegistry) {
		return null;
	}
	
	@Override
	public CharSequence toSQL() {
		StringAppender sql = new UnionGenerator();
		return sql.ccat(union.getQueries(), " union all ").toString();
	}
	
	private static class UnionGenerator extends StringAppender {
		
		@Override
		public StringAppender cat(Object o) {
			if (o instanceof Query) {
				return cat((Query) o);
			} else {
				return super.cat(o);
			}
		}
		
		private StringAppender cat(Query query) {
			QuerySQLBuilder unionBuilder = new QuerySQLBuilder(query);
			return cat(unionBuilder.toSQL());
		}
	} 
}
