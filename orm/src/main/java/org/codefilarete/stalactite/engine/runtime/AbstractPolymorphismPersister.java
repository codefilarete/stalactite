package org.codefilarete.stalactite.engine.runtime;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.codefilarete.stalactite.engine.PersistExecutor;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.query.EntityFinder;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.sql.result.Accumulators;

/**
 * Parent class of polymorphic persisters, made to share common code.
 * 
 * @param <C>
 * @param <I>
 * @author Guillaume Mary
 */
public abstract class AbstractPolymorphismPersister<C, I>
		extends PersisterListenerWrapper<C, I>
		implements ConfiguredRelationalPersister<C, I>, PolymorphicPersister<C>, AdvancedEntityPersister<C, I> {
	
	protected final Map<Class<C>, ConfiguredRelationalPersister<C, I>> subEntitiesPersisters;
	protected final ConfiguredRelationalPersister<C, I> mainPersister;
	protected final EntityCriteriaSupport<C> criteriaSupport;
	protected final EntityFinder<C, I> entityFinder;
	protected final PersistExecutor<C> persistExecutor;
	
	protected AbstractPolymorphismPersister(ConfiguredRelationalPersister<C, I> mainPersister,
											Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> subEntitiesPersisters,
											EntityFinder<C, I> entityFinder) {
		this.mainPersister = mainPersister;
		this.subEntitiesPersisters = (Map<Class<C>, ConfiguredRelationalPersister<C, I>>) subEntitiesPersisters;
		this.criteriaSupport = new EntityCriteriaSupport<>(mainPersister.getMapping());
		this.entityFinder = entityFinder;
		if (mainPersister.getMapping().getIdMapping().getIdentifierInsertionManager() instanceof AlreadyAssignedIdentifierManager) {
			this.persistExecutor = new AlreadyAssignedIdentifierPersistExecutor<>(this);
		} else {
			this.persistExecutor = new DefaultPersistExecutor<>(this);
		}
	}
	
	@Override
	public ExecutableEntityQueryCriteria<C, ?> selectWhere() {
		return newCriteriaSupport().wrapIntoExecutable();
	}
	
	@Override
	public EntityQueryCriteriaSupport<C, I> newCriteriaSupport() {
		return new EntityQueryCriteriaSupport<>(criteriaSupport, entityFinder, getPersisterListener());
	}
	
	@Override
	public ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<Select> selectAdapter) {
		ProjectionQueryCriteriaSupport<C, I> projectionSupport = new ProjectionQueryCriteriaSupport<>(criteriaSupport, entityFinder, selectAdapter);
		return projectionSupport.wrapIntoExecutable();
	}
	
	@Override
	public Set<C> selectAll() {
		return selectWhere().execute(Accumulators.toSet());
	}
	
	@Override
	public boolean isNew(C entity) {
		return mainPersister.isNew(entity);
	}
	
	@Override
	public Class<C> getClassToPersist() {
		return mainPersister.getClassToPersist();
	}
	
	@Override
	public EntityJoinTree<C, I> getEntityJoinTree() {
		return mainPersister.getEntityJoinTree();
	}
	
	@Override
	public I getId(C entity) {
		return mainPersister.getId(entity);
	}
	
	@Override
	protected void doPersist(Iterable<? extends C> entities) {
		persistExecutor.persist(entities);
	}
}
