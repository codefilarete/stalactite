package org.codefilarete.stalactite.engine.runtime;

import java.util.function.Consumer;

import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.query.EntityQueryCriteriaSupport;
import org.codefilarete.stalactite.query.EntityFinder;
import org.codefilarete.stalactite.query.model.Select;

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
	
	ProjectionQueryCriteriaSupport<C, I> newProjectionCriteriaSupport(Consumer<Select> selectAdapter);
	
	EntityJoinTree<C, I> getEntityJoinTree();
	
	EntityFinder<C, I> getEntityFinder();
	
}