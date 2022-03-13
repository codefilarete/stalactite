package org.codefilarete.stalactite.persistence.engine.runtime;

import java.util.Collection;

import org.codefilarete.stalactite.persistence.engine.EntityPersister;
import org.codefilarete.stalactite.persistence.engine.listener.PersisterListenerCollection;
import org.codefilarete.stalactite.persistence.mapping.EntityMappingStrategy;
import org.codefilarete.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface ConfiguredPersister<C, I> extends EntityPersister<C, I> {
	
	<T extends Table> EntityMappingStrategy<C, I, T> getMappingStrategy();
	
	Collection<Table> giveImpliedTables();
	
	PersisterListenerCollection<C, I> getPersisterListener();
}
