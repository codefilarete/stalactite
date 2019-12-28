package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.structure.Table;

/**
 * Builder of an {@link Persister}
 * 
 * @author Guillaume Mary
 * @see #build(PersistenceContext)
 * @see #build(PersistenceContext, Table)
 */
public interface PersisterBuilder<C, I>  {
	
	IPersister<C, I> build(PersistenceContext persistenceContext);
	
	<T extends Table> IPersister<C, I> build(PersistenceContext persistenceContext, T table);
}
