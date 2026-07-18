package org.codefilarete.stalactite.engine;

import org.codefilarete.stalactite.engine.listener.EntityWriteListener;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Equivalent to {@link EntityPersister} but for write operations only.
 * Made for responsibility design reason: reading entity aggregate is much more different than writing it.
 * Reading requires SQL joins, while write operations are more simple (no join, only insert/update/delete in cascade).
 * Thus, the implementations of those are quite different and don't do the same logic.
 * By having this interface we can compose a final {@link EntityPersister} from multiple {@link EntityWriteExecutor}
 * and {@link EntityReadExecutor} implementations.
 * 
 * @param <C>
 * @param <I>
 */
public interface EntityWriteExecutor<C, I> extends InsertExecutor<C>, UpdateExecutor<C>, DeleteExecutor<C, I>, EntityWriteListener<C> {
	
	<T extends Table<T>> EntityMapping<C, I, T> getMapping();
	
	I getId(C entity);
//	
//	default I getId(C entity) {
//		return getMapping().getId(entity);
//	}
}
