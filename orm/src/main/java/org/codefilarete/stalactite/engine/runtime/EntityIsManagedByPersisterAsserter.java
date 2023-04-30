package org.codefilarete.stalactite.engine.runtime;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Iterables;

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
	
	
	public EntityIsManagedByPersisterAsserter(EntityConfiguredJoinedTablesPersister<C, I> surrogate) {
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
	
	/* Please note that some methods were not overridden for calling assertion because there super implementation invokes some methods that do it,
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
	public void persist(C entity) {
		assertPersisterManagesEntity(entity);
		super.persist(entity);
	}
	
	@Override
	public void insert(Iterable<? extends C> entities) {
		assertPersisterManagesEntities(entities);
		super.insert(entities);
	}
	
	@Override
	public void update(Iterable<? extends Duo<C, C>> differencesIterable, boolean allColumnsStatement) {
		// we clear asserter input from null because it doesn't support it, and it may happen in update cases of nullified one-to-one relation
		List<C> nonNullEntities = Iterables.stream(differencesIterable).map(Duo::getLeft).filter(Objects::nonNull).collect(Collectors.toList());
		assertPersisterManagesEntities(nonNullEntities);
		super.update(differencesIterable, allColumnsStatement);
	}
	
	@Override
	public void updateById(Iterable<? extends C> entities) {
		assertPersisterManagesEntities(entities);
		super.updateById(entities);
	}
	
	@Override
	public void delete(Iterable<? extends C> entities) {
		// we clear asserter input from null because it doesn't support it, and it may happen in update cases of nullified one-to-one relation
		List<C> nonNullEntities = Iterables.stream(entities).filter(Objects::nonNull).collect(Collectors.toList());
		assertPersisterManagesEntities(nonNullEntities);
		super.delete(nonNullEntities);
	}
	
	@Override
	public void deleteById(Iterable<? extends C> entities) {
		assertPersisterManagesEntities(entities);
		super.deleteById(entities);
	}
}
