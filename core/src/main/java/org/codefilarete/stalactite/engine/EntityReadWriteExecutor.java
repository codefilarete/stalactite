package org.codefilarete.stalactite.engine;

/**
 * The contract for an all-in-one entity persister with basic capabilities:
 * - write the entity (without cascading the relations if any)
 * - load the entity from the database (without its relations)
 * 
 * Thus, this interface is a mashup of {@link EntityWriteExecutor} and {@link EntityReadExecutor}, which allows to
 * propose also the {@link PersistExecutor#persist(Object[])} ability since this method requires to load the entity
 * from the database to compute the difference between the memory state and the persisted state in update case.
 * 
 * @param <C>
 * @param <I>
 */
public interface EntityReadWriteExecutor<C, I> extends EntityWriteExecutor<C, I>, EntityReadExecutor<C, I>, PersistExecutor<C> {
}
