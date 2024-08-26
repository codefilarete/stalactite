package org.codefilarete.stalactite.query;

import org.codefilarete.stalactite.query.model.CriteriaChain;

/**
 * Interface to describe necessary inputs of {@link EntitySelector#select(ConfiguredEntityCriteria)}.
 * 
 * @author Guillaume Mary
 */
public interface ConfiguredEntityCriteria {
	
	boolean hasCollectionCriteria();
	
	CriteriaChain getCriteria();
	
}
