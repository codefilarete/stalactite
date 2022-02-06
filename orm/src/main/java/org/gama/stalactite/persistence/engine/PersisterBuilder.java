package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.engine.runtime.Persister;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Builder of an {@link Persister}
 * 
 * @author Guillaume Mary
 * @see #build(PersistenceContext)
 * @see #build(PersistenceContext, Table)
 */
public interface PersisterBuilder<C, I>  {
	
	EntityPersister<C, I> build(PersistenceContext persistenceContext);
	
	EntityPersister<C, I> build(PersistenceContext persistenceContext, Table table);
}
