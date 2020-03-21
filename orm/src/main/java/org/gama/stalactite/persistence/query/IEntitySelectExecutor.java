package org.gama.stalactite.persistence.query;

import java.util.List;

import org.gama.stalactite.query.builder.SQLQueryBuilder;
import org.gama.stalactite.query.model.CriteriaChain;
import org.gama.stalactite.query.model.Query;

/**
 * @author Guillaume Mary
 */
public interface IEntitySelectExecutor<C> {
	
	List<C> loadSelection(CriteriaChain where);
	
	List<C> loadGraph(CriteriaChain where);
	
	static SQLQueryBuilder createQueryBuilder(CriteriaChain where, Query query) {
		SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder(query);
		if (where.iterator().hasNext()) {    // prevents from empty where causing malformed SQL
			query.getWhere().and(where);
		}
		return sqlQueryBuilder;
	}
}
