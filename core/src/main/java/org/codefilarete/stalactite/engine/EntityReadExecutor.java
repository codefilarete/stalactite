package org.codefilarete.stalactite.engine;

import org.codefilarete.stalactite.engine.listener.EntityReadListener;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Persister contract focusing on read operations only.
 * Made for responsibility design reason: reading entity aggregate is much more different than writing it.
 * Reading requires SQL joins, while write operations are more simple (no join, only insert/update/delete in cascade).
 * Thus, the implementations of those are quite different and don't do the same logic.
 * Moreover, by having this interface we can compose some other high-level persister, for example
 * {@link EntityReadWriteExecutor} or {@link EntityPersister}.
 * 
 * @param <C> entity type to write
 * @param <I> entity identifier type
 */
public interface EntityReadExecutor<C, I> extends EntityReadListener<C, I>, SelectExecutor<C, I> {
	
	default Class<C> getClassToPersist() {
		return getMapping().getClassToPersist();
	}
	
	<T extends Table<T>> EntityMapping<C, I, T> getMapping();
	
	default <T extends Table<T>> T getMainTable() {
		return (T) getMapping().getTargetTable();
	}
	
}
