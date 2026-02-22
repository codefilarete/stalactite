package org.codefilarete.stalactite.query;

import java.util.Map;

import org.codefilarete.stalactite.query.api.CriteriaChain;
import org.codefilarete.stalactite.query.model.Limit;
import org.codefilarete.stalactite.query.model.OrderBy;

/**
 * Interface to describe the necessary inputs of {@link EntityFinder#select(ConfiguredEntityCriteria, Map, OrderBy, Limit)}.
 * 
 * @author Guillaume Mary
 */
public interface ConfiguredEntityCriteria {
	
	boolean hasCollectionCriteria();
	
	CriteriaChain<?> getCriteria();
	
}
