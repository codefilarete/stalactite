package org.codefilarete.stalactite.engine;

/**
 * @author Guillaume Mary
 */
public interface DeleteExecutor<C, I> {
	
	/**
	 * Will delete given instances.
	 * This method will take optimistic lock (versioned entity) into account.
	 *
	 * @param entities entities to be deleted
	 */
	void delete(Iterable<? extends C> entities);
	
	/**
	 * Will delete given instances only by their identifier.
	 * This method will not take optimistic lock (versioned entity) into account, so it will delete database rows "roughly".
	 *
	 * @param entities entities to be deleted
	 */
	void deleteById(Iterable<? extends C> entities);
	
	/**
	 * Will delete entities only from their identifier.
	 * This method will not take optimistic lock (versioned entity) into account, so it will delete database rows "roughly".
	 *
	 * Can't be named "deleteById" due to generics type erasure that generates same signature as {@link #deleteById(Iterable)}
	 *
	 * @param ids entities identifiers
	 * @return deleted row count
	 */
	//int deleteFromId(Iterable<I> ids);
}
