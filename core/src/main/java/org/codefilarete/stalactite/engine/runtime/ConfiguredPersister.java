package org.codefilarete.stalactite.engine.runtime;

import java.util.Collection;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.listener.PersisterListenerCollection;
import org.codefilarete.stalactite.mapping.EntityMappingStrategy;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface ConfiguredPersister<C, I> extends EntityPersister<C, I> {
	
	<T extends Table> EntityMappingStrategy<C, I, T> getMappingStrategy();
	
	Collection<Table> giveImpliedTables();
	
	PersisterListenerCollection<C, I> getPersisterListener();
}
