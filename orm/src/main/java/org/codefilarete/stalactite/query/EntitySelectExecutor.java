package org.codefilarete.stalactite.query;

import java.util.Set;

import org.codefilarete.stalactite.query.model.CriteriaChain;

/**
 * @author Guillaume Mary
 */
public interface EntitySelectExecutor<C> {
	
	Set<C> loadGraph(CriteriaChain where);
}
