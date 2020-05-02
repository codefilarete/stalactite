package org.gama.stalactite.persistence.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.gama.lang.Experimental;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.PairIterator;

/**
 * @author Guillaume Mary
 */
public interface IEntityConfiguredPersister<C, I> extends IEntityPersister<C, I>, IConfiguredPersister<C, I> {
	
	/**
	 * Updates given entity in database.
	 * It selects the existing data in database, then compares it with given entity in memory, and then updates database if necessary (nothing
	 * if no change were made).
	 * 
	 * @param entity the entity to be updated
	 * @return the number of updated rows : 1 if entity needed to be updated, 0 if not
	 */
	@Experimental
	default int update(C entity) {
		return update(entity, select(getMappingStrategy().getId(entity)), true);
	}
	
	/**
	 * Updates given entities in database.
	 * It selects the existing data in database, then compares it with given entities in memory, and then updates database if necessary (nothing
	 * if no change were made).
	 * 
	 * @param entities the entities to be updated
	 * @return the number of updated rows : 0 if no entities were updated, maximum is number of given entities 
	 */
	@Experimental
	default int update(Iterable<C> entities) {
		List<I> ids = Iterables.collect(entities, entity -> getMappingStrategy().getId(entity), ArrayList::new);
		return update(() -> new PairIterator<>(entities, select(ids)), true);
	}
	
	@Experimental
	default int update(I id, Consumer<C> entityConsumer) {
		C unmodified = select(id);
		C modified = select(id);
		entityConsumer.accept(modified);
		return update(modified, unmodified, true);
	}
}
