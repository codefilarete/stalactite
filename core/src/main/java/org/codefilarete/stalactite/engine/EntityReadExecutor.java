package org.codefilarete.stalactite.engine;

import org.codefilarete.stalactite.engine.listener.EntityReadListener;

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
}
