package org.codefilarete.stalactite.query.builder;

import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Union;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.statement.PreparedSQL;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.StringAppender;

/**
 * @author Guillaume Mary
 */
public class UnionSQLBuilder implements SQLBuilder, PreparedSQLBuilder {
	
	private final Union union;
	private final Dialect dialect;
	
	public UnionSQLBuilder(Union union, Dialect dialect) {
		this.union = union;
		this.dialect = dialect;
	}
	
	@Override
	public PreparedSQL toPreparedSQL(ColumnBinderRegistry parameterBinderRegistry) {
		return null;
	}
	
	@Override
	public CharSequence toSQL() {
		StringAppender sql = new UnionGenerator(this.dialect);
		return sql.ccat(union.getQueries(), " union all ").toString();
	}
	
	private static class UnionGenerator extends StringAppender {
		
		private final Dialect dialect;
		
		public UnionGenerator(Dialect dialect) {
			this.dialect = dialect;
		}
		
		@Override
		public StringAppender cat(Object o) {
			if (o instanceof Query) {
				return cat((Query) o);
			} else {
				return super.cat(o);
			}
		}
		
		private StringAppender cat(Query query) {
			QuerySQLBuilder unionBuilder = new QuerySQLBuilder(query, dialect);
			return cat(unionBuilder.toSQL());
		}
	} 
}
