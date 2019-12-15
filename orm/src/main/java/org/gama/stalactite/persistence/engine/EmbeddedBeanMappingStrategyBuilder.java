package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.mapping.EmbeddedBeanMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Builder of an {@link EmbeddedBeanMappingStrategy}
 * 
 * @author Guillaume Mary
 * @see #build(Dialect)
 * @see #build(Dialect, Table)
 */
public interface EmbeddedBeanMappingStrategyBuilder<C> extends EmbeddableMappingConfigurationProvider<C> {
	
	EmbeddedBeanMappingStrategy<C, Table> build(Dialect dialect);
	
	<T extends Table> EmbeddedBeanMappingStrategy<C, T> build(Dialect dialect, T targetTable);
	
}
