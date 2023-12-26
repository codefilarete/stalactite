package org.codefilarete.stalactite.engine;

import org.codefilarete.stalactite.engine.runtime.BeanPersister;

/**
 * Builder of an {@link BeanPersister}
 * 
 * @author Guillaume Mary
 * @see #build(PersistenceContext)
 */
public interface PersisterBuilder<C, I>  {
	
	EntityPersister<C, I> build(PersistenceContext persistenceContext);
	
}
