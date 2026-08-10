package org.codefilarete.stalactite.engine.runtime;

import java.util.Map;

import org.codefilarete.stalactite.engine.EntityWriteExecutor;

public interface PolymorphicWriter<C, I, SUBENTITY extends C>
		extends PolymorphicPersister<C>, EntityWriteExecutor<C, I> {
	
	Map<Class<SUBENTITY>, ? extends EntityWriteExecutor<SUBENTITY, I>> getSubEntitiesPersisters();
}
