package org.codefilarete.stalactite.persistence.engine;

/**
 * @author Guillaume Mary
 */
public interface DeleteExecutor<C, I> {
	
	/**
	 * Will given instances.
	 * This method will take optimisic lock (versioned entity) into account.
	 *
	 * @param entities entites to be deleted
	 */
	void delete(Iterable<C> entities);
	
	/**
	 * Will delete instances only by their identifier.
	 * This method will not take optimisic lock (versioned entity) into account, so it will delete database rows "roughly".
	 *
	 * @param entities entites to be deleted
	 */
	void deleteById(Iterable<C> entities);
	
	/**
	 * Will delete entities only from their identifier.
	 * This method will not take optimisic lock (versioned entity) into account, so it will delete database rows "roughly".
	 *
	 * Can't be named "deleteById" due to generics type erasure that generates same signature as {@link #deleteById(Iterable)}
	 *
	 * @param ids entities identifiers
	 * @return deleted row count
	 */
	//int deleteFromId(Iterable<I> ids);
}
