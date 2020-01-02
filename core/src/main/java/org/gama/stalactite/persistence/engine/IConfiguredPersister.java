package org.gama.stalactite.persistence.engine;

import java.util.Collection;

import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface IConfiguredPersister<C, I> {
	
	IEntityMappingStrategy<C, I, ?> getMappingStrategy();
	
	Collection<Table> giveImpliedTables();
	
	PersisterListener<C, I> getPersisterListener();
}
