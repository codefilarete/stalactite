package org.codefilarete.stalactite.query;

import java.util.Set;

import org.codefilarete.stalactite.query.model.CriteriaChain;

/**
 * Contract that defines expected methods to load an entity graph conditionally on some properties criteria coming
 * from {@link CriteriaChain}.
 * 
 * @author Guillaume Mary
 */
public interface EntitySelector<C, I> {
	
	Set<C> select(CriteriaChain where);
	
}
