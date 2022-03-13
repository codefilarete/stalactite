package org.codefilarete.stalactite.engine;

import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.PersistenceContext;
import org.codefilarete.stalactite.engine.runtime.Persister;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

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
