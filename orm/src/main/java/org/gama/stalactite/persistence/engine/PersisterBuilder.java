package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.structure.Table;

/**
 * Builder of an {@link Persister}
 * 
 * @author Guillaume Mary
 * @see #build(PersistenceContext)
 * @see #build(PersistenceContext, Table)
 */
public interface PersisterBuilder<C, I> {
	
	EntityMappingConfiguration<C, I> getConfiguration();
	
	Persister<C, I, Table> build(PersistenceContext persistenceContext);
	
	<T extends Table> Persister<C, I, T> build(PersistenceContext persistenceContext, T table);
}
