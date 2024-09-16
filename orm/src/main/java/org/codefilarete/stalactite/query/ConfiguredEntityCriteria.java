package org.codefilarete.stalactite.query;

import java.util.function.Consumer;

import org.codefilarete.stalactite.query.model.CriteriaChain;

/**
 * Interface to describe necessary inputs of {@link EntitySelector#select(ConfiguredEntityCriteria, Consumer, Consumer)}.
 * 
 * @author Guillaume Mary
 */
public interface ConfiguredEntityCriteria {
	
	boolean hasCollectionCriteria();
	
	CriteriaChain<?> getCriteria();
	
}
