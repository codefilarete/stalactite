package org.gama.stalactite.persistence.engine.runtime;

import java.util.Collection;

import org.gama.stalactite.persistence.engine.EntityPersister;
import org.gama.stalactite.persistence.engine.listening.PersisterListenerCollection;
import org.gama.stalactite.persistence.mapping.EntityMappingStrategy;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface ConfiguredPersister<C, I> extends EntityPersister<C, I> {
	
	<T extends Table> EntityMappingStrategy<C, I, T> getMappingStrategy();
	
	Collection<Table> giveImpliedTables();
	
	PersisterListenerCollection<C, I> getPersisterListener();
}
