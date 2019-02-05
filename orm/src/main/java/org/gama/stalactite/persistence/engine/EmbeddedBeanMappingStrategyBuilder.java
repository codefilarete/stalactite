package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.mapping.EmbeddedBeanMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface EmbeddedBeanMappingStrategyBuilder<C> {
	
	<T extends Table> EmbeddedBeanMappingStrategy<C, T> build(Dialect dialect, T targetTable);
	
}
