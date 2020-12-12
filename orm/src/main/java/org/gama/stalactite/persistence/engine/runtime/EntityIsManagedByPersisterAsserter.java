package org.gama.stalactite.persistence.engine.runtime;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.gama.lang.Duo;
import org.gama.lang.Reflections;
import org.gama.lang.collection.Iterables;

/**
 * Persister which checks that given instances can be persisted by itself, throws exception if that's not the case.
 * Done in particular for inheritance cases where subtypes may have not been mapped but their entities are accepted (due to usual inheritance), this
 * would have resulted in partial persistence : only common (upper) part was took into account without warning user.
 * 
 * Designed as a wrapper of an underlying surrogate which really persists instances. Made as such for single responsibility consideration.
 * 
 * @author Guillaume Mary
 */
public class EntityIsManagedByPersisterAsserter<C, I> extends PersisterWrapper<C, I> {
	
	private final Consumer<C> asserter;
	
	
	public EntityIsManagedByPersisterAsserter(IEntityConfiguredJoinedTablesPersister<C, I> surrogate) {
		super(surrogate);
		if (getDeepestSurrogate() instanceof PolymorphicPersister) {
			Set<Class<? extends C>> supportedEntityTypes = ((PolymorphicPersister<C>) getDeepestSurrogate()).getSupportedEntityTypes();
			asserter = entity -> {
				if (!supportedEntityTypes.contains(entity.getClass())) {
					throw newAssertException(entity);
				}
			};
		} else {
			asserter = entity -> {
				if (!getClassToPersist().equals(entity.getClass()))
					throw newAssertException(entity);
			};
		}
	}
	
	private UnsupportedOperationException newAssertException(C entity) {
		return new UnsupportedOperationException("Persister of " + Reflections.toString(getClassToPersist())
						+ " is not configured to persist " + Reflections.toString(entity.getClass()));
	}
	
	@VisibleForTesting
	void assertPersisterManagesEntities(Iterable<? extends C> entity) {
		entity.forEach(this::assertPersisterManagesEntity);
	}
	
	@VisibleForTesting
	void assertPersisterManagesEntity(C entity) {
		asserter.accept(entity);
	}
	
	/* Please note that some methods were not overriden for calling assertion because there super implementation invokes some methods that do it,
	 * hence by such a way we avoid multiple calls to assertion. Meanwhile a Unit Test checks all of them. 
	 * Here they are:
	 * - persist(Iterable<? extends C> entities)
	 * - insert(C entity)
	 * - update(C modified, C unmodified, boolean allColumnsStatement)
	 * - update(C entity)
	 * - update(Iterable<C> entities)
	 * - updateById(C entity)
	 * - delete(C entity)
	 * - deleteById(C entity)
	 */
	
	@Override
	public int persist(C entity) {
		assertPersisterManagesEntity(entity);
		return super.persist(entity);
	}
	
	@Override
	public int insert(Iterable<? extends C> entities) {
		assertPersisterManagesEntities(entities);
		return super.insert(entities);
	}
	
	@Override
	public int update(Iterable<? extends Duo<? extends C, ? extends C>> differencesIterable, boolean allColumnsStatement) {
		// we clear asserter input from null because it doesn't support it, and it may happen in update cases of nullified one-to-one relation
		List<C> nonNullEntities = Iterables.stream(differencesIterable).map(Duo::getLeft).filter(Objects::nonNull).collect(Collectors.toList());
		assertPersisterManagesEntities(nonNullEntities);
		return super.update(differencesIterable, allColumnsStatement);
	}
	
	@Override
	public int updateById(Iterable<C> entities) {
		assertPersisterManagesEntities(entities);
		return super.updateById(entities);
	}
	
	@Override
	public int delete(Iterable<C> entities) {
		assertPersisterManagesEntities(entities);
		return super.delete(entities);
	}
	
	@Override
	public int deleteById(Iterable<C> entities) {
		assertPersisterManagesEntities(entities);
		return super.deleteById(entities);
	}
}
