package org.codefilarete.stalactite.query;

import java.util.List;

import org.codefilarete.stalactite.query.builder.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.sql.Dialect;

/**
 * @author Guillaume Mary
 */
public interface EntitySelectExecutor<C> {
	
	List<C> loadGraph(CriteriaChain where);
	
	static QuerySQLBuilder createQueryBuilder(CriteriaChain where, Query query, Dialect dialect) {
		QuerySQLBuilder sqlQueryBuilder = new QuerySQLBuilder(query, dialect);
		if (where.iterator().hasNext()) {    // prevents from empty where causing malformed SQL
			query.getWhere().and(where);
		}
		return sqlQueryBuilder;
	}
}
