package org.codefilarete.stalactite.engine;

import org.codefilarete.stalactite.engine.runtime.BeanPersister;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Builder of an {@link BeanPersister}
 * 
 * @author Guillaume Mary
 * @see #build(PersistenceContext)
 * @see #build(PersistenceContext, Table)
 */
public interface PersisterBuilder<C, I>  {
	
	EntityPersister<C, I> build(PersistenceContext persistenceContext);
	
	EntityPersister<C, I> build(PersistenceContext persistenceContext, Table table);
}
