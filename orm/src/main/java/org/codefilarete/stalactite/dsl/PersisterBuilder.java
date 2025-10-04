package org.codefilarete.stalactite.dsl;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.PersistenceContext;
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
