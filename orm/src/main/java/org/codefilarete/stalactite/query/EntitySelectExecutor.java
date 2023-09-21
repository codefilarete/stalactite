package org.codefilarete.stalactite.query;

import java.util.Set;

import org.codefilarete.stalactite.query.builder.QuerySQLBuilder;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.sql.Dialect;

/**
 * @author Guillaume Mary
 */
public interface EntitySelectExecutor<C> {
	
	Set<C> loadGraph(CriteriaChain where);
}
