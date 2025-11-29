package org.codefilarete.stalactite.engine.runtime;

import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.stalactite.engine.PersistExecutor;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.query.EntityQueryCriteriaSupport;
import org.codefilarete.stalactite.mapping.AccessorWrapperIdAccessor;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.query.EntityFinder;
import org.codefilarete.stalactite.query.model.Operators;
import org.codefilarete.stalactite.query.model.Select;
import org.codefilarete.stalactite.query.model.operator.TupleIn;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulators;
import org.codefilarete.tool.collection.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	
	public static AbstractPolymorphismPersister<?, ?> lookupForPolymorphicPersister(ConfiguredRelationalPersister<?, ?> targetPersister) {
		if (targetPersister instanceof AbstractPolymorphismPersister) {
			return (AbstractPolymorphismPersister<?, ?>) targetPersister;
		} else if (targetPersister instanceof PersisterWrapper) {
			ConfiguredRelationalPersister<?, ?> deepestDelegate = ((PersisterWrapper<?, ?>) targetPersister).getDeepestDelegate();
			if (deepestDelegate instanceof AbstractPolymorphismPersister) {
				return (AbstractPolymorphismPersister<?, ?>) deepestDelegate;
			} else {
				return null;
			}
		} else {
			return null;
		}
	}
	
	protected final Logger LOGGER = LoggerFactory.getLogger(this.getClass());
	
	protected final Map<Class<C>, ConfiguredRelationalPersister<C, I>> subEntitiesPersisters;
	protected final ConfiguredRelationalPersister<C, I> mainPersister;
	protected final EntityFinder<C, I> entityFinder;
	protected final PersistExecutor<C> persistExecutor;
	
	protected AbstractPolymorphismPersister(ConfiguredRelationalPersister<C, I> mainPersister,
											Map<? extends Class<C>, ? extends ConfiguredRelationalPersister<C, I>> subEntitiesPersisters,
											EntityFinder<C, I> entityFinder) {
		this.mainPersister = mainPersister;
		this.subEntitiesPersisters = (Map<Class<C>, ConfiguredRelationalPersister<C, I>>) subEntitiesPersisters;
		this.entityFinder = entityFinder;
		this.persistExecutor = new DefaultPersistExecutor<>(this);
	}
	
	@Override
	public EntityFinder<C, I> getEntityFinder() {
		return entityFinder;
	}
	
	public Map<Class<C>, ConfiguredRelationalPersister<C, I>> getSubEntitiesPersisters() {
		return subEntitiesPersisters;
	}

	public abstract <LEFTTABLE extends Table<LEFTTABLE>, SUBTABLE extends Table<SUBTABLE>, JOINTYPE>
	void propagateMappedAssociationToSubTables(Key<SUBTABLE, JOINTYPE> foreignKey, PrimaryKey<LEFTTABLE, JOINTYPE> leftPrimaryKey, BiFunction<Key<SUBTABLE, JOINTYPE>, PrimaryKey<LEFTTABLE, JOINTYPE>, String> foreignKeyNamingFunction);
	
	@Override
	public Set<C> doSelect(Iterable<I> ids) {
		LOGGER.debug("selecting entities {}", ids);
		// Note that executor emits select listener events
		IdMapping<C, I> idMapping = mainPersister.getMapping().getIdMapping();
		AccessorWrapperIdAccessor<C, I> idAccessor = (AccessorWrapperIdAccessor<C, I>) idMapping.getIdAccessor();
		if (idMapping.getIdentifierAssembler() instanceof ComposedIdentifierAssembler) {
			// && dialect.supportTupleIn
			Map<? extends Column<?, ?>, ?> columnValues = ((ComposedIdentifierAssembler<I, ?>) idMapping.getIdentifierAssembler()).getColumnValues(ids);
			TupleIn tupleIn = TupleIn.transformBeanColumnValuesToTupleInValues((int) Iterables.size(ids), columnValues);
			EntityQueryCriteriaSupport<C, I> newCriteriaSupport = newCriteriaSupport();
			newCriteriaSupport.getEntityCriteriaSupport().getCriteria().and(tupleIn);
			return newCriteriaSupport.wrapIntoExecutable().execute(Accumulators.toSet());
		} else {
			return selectWhere().and(new AccessorChain<>(idAccessor.getIdAccessor()), Operators.in(ids)).execute(Accumulators.toSet());
		}
	}
	
	@Override
	public ExecutableEntityQueryCriteria<C, ?> selectWhere() {
		return newCriteriaSupport().wrapIntoExecutable();
	}
	
	@Override
	public EntityQueryCriteriaSupport<C, I> newCriteriaSupport() {
		return entityFinder.newCriteriaSupport();
	}
	
	public ProjectionQueryCriteriaSupport<C, I> newProjectionCriteriaSupport(Consumer<Select> selectAdapter) {
		return new ProjectionQueryCriteriaSupport<>(entityFinder, newCriteriaSupport().getEntityCriteriaSupport(), selectAdapter);
	}
	
	@Override
	public ExecutableProjectionQuery<C, ?> selectProjectionWhere(Consumer<Select> selectAdapter) {
		ProjectionQueryCriteriaSupport<C, I> projectionSupport = new ProjectionQueryCriteriaSupport<>(entityFinder, selectAdapter);
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
