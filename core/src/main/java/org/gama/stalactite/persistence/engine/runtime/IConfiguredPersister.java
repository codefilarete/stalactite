package org.gama.stalactite.persistence.engine.runtime;

import java.util.Collection;

import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface IConfiguredPersister<C, I> {
	
	<T extends Table> IEntityMappingStrategy<C, I, T> getMappingStrategy();
	
	Collection<Table> giveImpliedTables();
	
	PersisterListener<C, I> getPersisterListener();
}
