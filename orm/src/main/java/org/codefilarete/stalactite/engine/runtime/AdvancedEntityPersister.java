package org.codefilarete.stalactite.engine.runtime;

/**
 * In the following, "public" means "for the very end-user".
 * 
 * Persisters implementing this interface are expected to provide some non-public behaviors, as such they expose some internal classes that are also
 * not expected to be used publicly. Thus, this interface is much more for internal use, even if some public persisters implement it, it is not
 * expected that they can be cast to this interface type.
 * 
 * @author Guillaume Mary
 */
public interface AdvancedEntityPersister<C, I> extends ConfiguredPersister<C, I> {
	
	/**
	 * Creates a new {@link EntityQueryCriteriaSupport} that can be used for further querying.
	 * 
	 * @return a new {@link EntityQueryCriteriaSupport}
	 */
	EntityQueryCriteriaSupport<C, I> newCriteriaSupport();
	
}