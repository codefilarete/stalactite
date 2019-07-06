package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Builder of an {@link Persister}
 * 
 * @author Guillaume Mary
 * @see #build(PersistenceContext)
 * @see #build(PersistenceContext, Table)
 */
public interface PersisterBuilder<C, I> extends EntityMappingConfigurationProvider<C, I> {
	
	JoinedTablesPersister<C, I, Table> build(PersistenceContext persistenceContext);
	
	<T extends Table> JoinedTablesPersister<C, I, T> build(PersistenceContext persistenceContext, T table);
}
