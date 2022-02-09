package org.codefilarete.stalactite.persistence.query;

import java.util.List;

import org.codefilarete.stalactite.query.builder.SQLQueryBuilder;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.Query;

/**
 * @author Guillaume Mary
 */
public interface EntitySelectExecutor<C> {
	
	List<C> loadGraph(CriteriaChain where);
	
	static SQLQueryBuilder createQueryBuilder(CriteriaChain where, Query query) {
		SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder(query);
		if (where.iterator().hasNext()) {    // prevents from empty where causing malformed SQL
			query.getWhere().and(where);
		}
		return sqlQueryBuilder;
	}
}
