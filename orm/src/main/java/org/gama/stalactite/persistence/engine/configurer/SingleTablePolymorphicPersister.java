package org.gama.stalactite.persistence.engine.configurer;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Duo;
import org.gama.lang.Nullable;
import org.gama.lang.Reflections;
import org.gama.lang.StringAppender;
import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.function.SerializableTriConsumer;
import org.gama.lang.trace.ModifiableInt;
import org.gama.reflection.MethodReferenceDispatcher;
import org.gama.stalactite.persistence.engine.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.ExecutableQuery;
import org.gama.stalactite.persistence.engine.IEntityConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.IInsertExecutor;
import org.gama.stalactite.persistence.engine.ISelectExecutor;
import org.gama.stalactite.persistence.engine.IUpdateExecutor;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.gama.stalactite.persistence.engine.SingleTablePolymorphismEntitySelectExecutor;
import org.gama.stalactite.persistence.engine.SingleTablePolymorphismSelectExecutor;
import org.gama.stalactite.persistence.engine.cascade.AbstractJoin.JoinType;
import org.gama.stalactite.persistence.engine.cascade.IJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister.CriteriaProvider;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister.RelationalExecutableEntityQuery;
import org.gama.stalactite.persistence.engine.cascade.StrategyJoinsRowTransformer.EntityInflater;
import org.gama.stalactite.persistence.engine.listening.DeleteByIdListener;
import org.gama.stalactite.persistence.engine.listening.DeleteListener;
import org.gama.stalactite.persistence.engine.listening.IPersisterListener;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener;
import org.gama.stalactite.persistence.mapping.AbstractTransformer;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.mapping.IdMappingStrategy;
import org.gama.stalactite.persistence.query.EntityCriteriaSupport;
import org.gama.stalactite.persistence.query.RelationalEntityCriteria;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.AbstractRelationalOperator;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.result.Row;

/**
 * @author Guillaume Mary
 */
public class SingleTablePolymorphicPersister<C, I, T extends Table<T>, D> implements IEntityConfiguredJoinedTablesPersister<C, I> {
	
	private static final ThreadLocal<Set<RelationIds<Object /* E */, Object /* target */, Object /* target identifier */ >>> DIFFERED_ENTITY_LOADER = new ThreadLocal<>();
	
	private final SingleTablePolymorphismSelectExecutor<C, I, ?, Object> selectExecutor;
	private final Map<Class<? extends C>, JoinedTablesPersister<C, I, ?>> subEntitiesPersisters;
	private final JoinedTablesPersister<C, I, T> mainPersister;
	private final Column<T, D> discriminatorColumn;
	private final SingleTablePolymorphism<C, I, Object> polymorphismPolicy;
	private final SingleTablePolymorphismEntitySelectExecutor<C, I, T, D> entitySelectExecutor;
	private final EntityCriteriaSupport<C> criteriaSupport;
	private final Map<Class<? extends C>, IInsertExecutor<C>> subclassInsertExecutors;
	private final Map<Class<? extends C>, IUpdateExecutor<C>> subclassUpdateExecutors;
	
	public SingleTablePolymorphicPersister(JoinedTablesPersister<C, I, T> mainPersister,
															 Map<Class<? extends C>, JoinedTablesPersister<C, I, ?>> subEntitiesPersisters,
															 ConnectionProvider connectionProvider,
															 Dialect dialect,
															 Column<T, D> discriminatorColumn,
															 SingleTablePolymorphism<C, I, D> polymorphismPolicy) {
		this.mainPersister = mainPersister;
		this.discriminatorColumn = discriminatorColumn;
		this.polymorphismPolicy = (SingleTablePolymorphism<C, I, Object>) polymorphismPolicy;
		this.subclassInsertExecutors = Iterables.map(subEntitiesPersisters.entrySet(), Entry::getKey, e -> e.getValue().getInsertExecutor());
		this.subclassUpdateExecutors = Iterables.map(subEntitiesPersisters.entrySet(), Entry::getKey, e -> e.getValue().getUpdateExecutor());
		
		this.subEntitiesPersisters = subEntitiesPersisters;
		this.subEntitiesPersisters.values().forEach(subclassPersister ->
				subclassPersister.getMappingStrategy().addSilentColumnInserter((Column) discriminatorColumn,
						c -> polymorphismPolicy.getDiscriminatorValue((Class<? extends C>) c.getClass()))
		);
		
		subEntitiesPersisters.forEach((type, persister) ->
				mainPersister.copyJoinsRootTo(persister.getJoinedStrategiesSelect(), JoinedStrategiesSelect.FIRST_STRATEGY_NAME)
		);
		
		this.selectExecutor = new SingleTablePolymorphismSelectExecutor(
				subEntitiesPersisters,
				discriminatorColumn,
				polymorphismPolicy,
				mainPersister.getMainTable(),
				connectionProvider,
				dialect);
		
		this.entitySelectExecutor = new SingleTablePolymorphismEntitySelectExecutor(
				subEntitiesPersisters,
				discriminatorColumn,
				polymorphismPolicy,
				mainPersister.getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect(),
				connectionProvider,
				dialect);
		
		this.criteriaSupport = new EntityCriteriaSupport<>(mainPersister.getMappingStrategy());
	}
	
	@Override
	public Collection<Table> giveImpliedTables() {
		return mainPersister.giveImpliedTables();
	}
	
	@Override
	public PersisterListener<C, I> getPersisterListener() {
		return mainPersister.getPersisterListener();
	}
	
	@Override
	public int insert(Iterable<? extends C> entities) {
		Map<Class, Set<C>> entitiesPerType = new HashMap<>();
		for (C entity : entities) {
			entitiesPerType.computeIfAbsent(entity.getClass(), cClass -> new HashSet<>()).add(entity);
		}
		
		// We "warn" user if he didn't give some configured instances (such as main type entities, only sub types are expected)
		Set<Class> entitiesTypes = new HashSet<>(entitiesPerType.keySet());
		entitiesTypes.removeAll(subclassInsertExecutors.keySet());
		if (!entitiesTypes.isEmpty()) {
			StringAppender classNameAppender = new StringAppender() {
				@Override
				public StringAppender cat(Object s) {
					if (s instanceof Class) {
						return super.cat(Reflections.toString((Class) s));
					} else {
						return super.cat(s);
					}
				}
			};
			classNameAppender.ccat(entitiesTypes, ", ");
			throw new IllegalArgumentException("Some entities can't be inserted because their mapping is undefined : " + classNameAppender);
		}
		
		ModifiableInt insertCount = new ModifiableInt();
		subclassInsertExecutors.forEach((subclass, insertExecutor) -> {
			Set<C> subtypeEntities = entitiesPerType.get(subclass);
			if (subtypeEntities != null) {
				insertCount.increment(insertExecutor.insert(subtypeEntities));
			}
		});
		
		return insertCount.getValue();
	}
	
	@Override
	public int updateById(Iterable<C> entities) {
		Map<Class, Set<C>> entitiesPerType = new HashMap<>();
		for (C entity : entities) {
			entitiesPerType.computeIfAbsent(entity.getClass(), cClass -> new HashSet<>()).add(entity);
		}
		ModifiableInt updateCount = new ModifiableInt();
		subclassUpdateExecutors.forEach((subclass, updateExecutor) -> {
			Set<C> entitiesToUpdate = entitiesPerType.get(subclass);
			if (entitiesToUpdate != null) {
				updateCount.increment(updateExecutor.updateById(entitiesToUpdate));
			}
		});
		
		return updateCount.getValue();
	}
	
	@Override
	public int update(Iterable<? extends Duo<? extends C, ? extends C>> differencesIterable, boolean allColumnsStatement) {
		Map<Class, Set<Duo<? extends C, ? extends C>>> entitiesPerType = new HashMap<>();
		differencesIterable.forEach(payload -> {
			C entity = Objects.preventNull(payload.getLeft(), payload.getRight());
			entitiesPerType.computeIfAbsent(entity.getClass(), k -> new HashSet<>()).add(payload);
		});
		ModifiableInt updateCount = new ModifiableInt();
		subclassUpdateExecutors.forEach((subclass, updateExecutor) -> {
			Set<Duo<? extends C, ? extends C>> entitiesToUpdate = entitiesPerType.get(subclass);
			if (entitiesToUpdate != null) {
				updateCount.increment(updateExecutor.update(entitiesToUpdate, allColumnsStatement));
			}
		});
		
		return updateCount.getValue();
	}
	
	@Override
	public List<C> select(Iterable<I> ids) {
		return selectExecutor.select(ids);
	}
	
	@Override
	public int delete(Iterable<C> entities) {
		// deleting throught main entity is suffiscient because subentities tables is also main entity one
		// NB: we use deleteExecutor not to trigger listener, because they should be triggered by wrapper, else we would try to delete twice
		// related beans for instance
		return mainPersister.getDeleteExecutor().delete(entities);
	}
	
	@Override
	public int deleteById(Iterable<C> entities) {
		// NB: we use deleteExecutor not to trigger listener, because they should be triggered by wrapper, else we would try to delete twice
		// related beans for instance
		return mainPersister.getDeleteExecutor().deleteById(entities);
	}
	
	@Override
	public int persist(Iterable<C> entities) {
		Map<Class, Set<C>> entitiesPerType = new HashMap<>();
		for (C entity : entities) {
			entitiesPerType.computeIfAbsent(entity.getClass(), cClass -> new HashSet<>()).add(entity);
		}
		ModifiableInt insertCount = new ModifiableInt();
		subEntitiesPersisters.forEach((subclass, persister) -> {
			Set<C> subtypeEntities = entitiesPerType.get(subclass);
			if (subtypeEntities != null) {
				insertCount.increment(persister.persist(subtypeEntities));
			}
		});
		
		return insertCount.getValue();
	}
	
	@Override
	public <O> ExecutableEntityQuery<C> selectWhere(SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator) {
		EntityCriteriaSupport<C> localCriteriaSupport = newWhere();
		localCriteriaSupport.and(getter, operator);
		return wrapIntoExecutable(localCriteriaSupport);
	}
	
	private RelationalExecutableEntityQuery<C> wrapIntoExecutable(EntityCriteriaSupport<C> localCriteriaSupport) {
		MethodReferenceDispatcher methodDispatcher = new MethodReferenceDispatcher();
		return methodDispatcher
				.redirect((SerializableFunction<ExecutableQuery, List<C>>) ExecutableQuery::execute,
						() -> entitySelectExecutor.loadGraph(localCriteriaSupport.getCriteria()))
				.redirect(CriteriaProvider::getCriteria, localCriteriaSupport::getCriteria)
				.redirect(RelationalEntityCriteria.class, localCriteriaSupport, true)
				.build((Class<RelationalExecutableEntityQuery<C>>) (Class) RelationalExecutableEntityQuery.class);
	}
	
	private EntityCriteriaSupport<C> newWhere() {
		// we must clone the underlying support, else it would be modified for all subsequent invokations and criteria will aggregate
		return new EntityCriteriaSupport<>(criteriaSupport);
	}
	
	
	@Override
	public <O> ExecutableEntityQuery<C> selectWhere(SerializableBiConsumer<C, O> setter, AbstractRelationalOperator<O> operator) {
		EntityCriteriaSupport<C> localCriteriaSupport = newWhere();
		localCriteriaSupport.and(setter, operator);
		return wrapIntoExecutable(localCriteriaSupport);
	}
	
	@Override
	public List<C> selectAll() {
		return entitySelectExecutor.loadGraph(newWhere().getCriteria());
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
	public void addInsertListener(InsertListener insertListener) {
//		mainPersister.addInsertListener(insertListener);
		subEntitiesPersisters.values().forEach(p -> p.addInsertListener(insertListener));
	}
	
	@Override
	public void addUpdateListener(UpdateListener updateListener) {
//		mainPersister.addUpdateListener(updateListener);
		subEntitiesPersisters.values().forEach(p -> p.addUpdateListener(updateListener));
	}
	
	@Override
	public void addSelectListener(SelectListener selectListener) {
//		mainPersister.addSelectListener(selectListener);
		subEntitiesPersisters.values().forEach(p -> p.addSelectListener(selectListener));
	}
	
	@Override
	public void addDeleteListener(DeleteListener deleteListener) {
//		mainPersister.addDeleteListener(deleteListener);
		subEntitiesPersisters.values().forEach(p -> p.addDeleteListener(deleteListener));
	}
	
	@Override
	public void addDeleteByIdListener(DeleteByIdListener deleteListener) {
//		mainPersister.addDeleteByIdListener(deleteListener);
		subEntitiesPersisters.values().forEach(p -> p.addDeleteByIdListener(deleteListener));
	}
	
	/**
	 * Overriden to capture {@link IEntityMappingStrategy#addSilentColumnInserter(Column, Function)} and
	 * {@link IEntityMappingStrategy#addSilentColumnUpdater(Column, Function)} (see {@link org.gama.stalactite.persistence.engine.CascadeManyConfigurer})
	 * Made to dispatch those methods subclass strategies since their persisters are in charge of managing their entities (not the parent one).
	 *
	 * Design question : one may think that's not a good design to override a getter, caller should invoke an intention-clear method on
	 * ourselves (Persister) but the case is to add a silent Column insert/update which is not the goal of the Persister to know implementation
	 * detail : they are to manage cascades and coordinate their mapping strategies. {@link IEntityMappingStrategy} are in charge of knowing
	 * {@link Column} actions.
	 *
	 * @param <T> table type
	 * @return an enhanced version of our main persister mapping strategy which dispatches silent column insert/update to sub-entities ones
	 */
	@Override
	public <T extends Table> IEntityMappingStrategy<C, I, T> getMappingStrategy() {
		// TODO: This is not the cleanest implementation because we use MethodReferenceDispatcher which is kind of overkill : use a dispatching
		//  interface
		MethodReferenceDispatcher methodReferenceDispatcher = new MethodReferenceDispatcher();
		IEntityMappingStrategy<C, I, T> result = methodReferenceDispatcher
				.redirect((SerializableTriConsumer<IEntityMappingStrategy<C, I, T>, Column<T, Object>, Function<C, Object>>)
								IEntityMappingStrategy::addSilentColumnInserter,
						(c, f) -> subEntitiesPersisters.values().forEach(p -> p.getMappingStrategy().addSilentColumnInserter((Column) c, f)))
				.redirect((SerializableTriConsumer<IEntityMappingStrategy<C, I, T>, Column<T, Object>, Function<C, Object>>)
								IEntityMappingStrategy::addSilentColumnUpdater,
						(c, f) -> subEntitiesPersisters.values().forEach(p -> p.getMappingStrategy().addSilentColumnUpdater((Column) c, f)))
				.fallbackOn(mainPersister.getMappingStrategy())
				.build((Class<IEntityMappingStrategy<C, I, T>>) (Class) IEntityMappingStrategy.class);
		return result;
	}
	
	@Override
	public <SRC, T1 extends Table, T2 extends Table> void joinAsOne(IJoinedTablesPersister<SRC, I> sourcePersister,
																	Column<T1, I> leftColumn,
																	Column<T2, I> rightColumn,
																	BeanRelationFixer<SRC, C> beanRelationFixer,
																	boolean optional) {
		
		// TODO: simplify query : it joins on target table as many as subentities which can be reduced to one join if FirstPhaseOneToOneLoader
		//  can compute disciminatorValue 
		subEntitiesPersisters.forEach((c, subPersister) -> {
			Column primaryKey = (Column ) Iterables.first(subPersister.getMainTable().getPrimaryKey().getColumns());
			sourcePersister.getJoinedStrategiesSelect().addMergeJoin(JoinedStrategiesSelect.FIRST_STRATEGY_NAME,
					new FirstPhaseOneToOneLoader(subPersister.getMappingStrategy().getIdMappingStrategy(), primaryKey, selectExecutor,
							mainPersister.getClassToPersist(), discriminatorColumn, polymorphismPolicy.getDiscriminatorValue(c)),
					(Set) Arrays.asHashSet(leftColumn, rightColumn, discriminatorColumn),
					leftColumn, rightColumn, optional ? JoinType.OUTER : JoinType.INNER);
		});
		
		
		// adding second phase loader
		((IPersisterListener) sourcePersister).addSelectListener(new SecondPhaseOneToOneLoader<>(beanRelationFixer));
	}
	
	@Override
	public <SRC, T1 extends Table, T2 extends Table> void joinAsMany(IJoinedTablesPersister<SRC, I> sourcePersister,
																	 Column<T1, I> leftColumn,
																	 Column<T2, I> rightColumn,
																	 BeanRelationFixer<SRC, C> beanRelationFixer,
																	 String joinName, boolean optional) {
		
		sourcePersister.getJoinedStrategiesSelect().addPassiveJoin(joinName,
				leftColumn,
				rightColumn,
				JoinType.OUTER,
				(Set) Collections.emptySet());
		
		// TODO: simplify query : it joins on target table as many as subentities which can be reduced to one join if FirstPhaseOneToOneLoader
		//  can compute disciminatorValue 
		subEntitiesPersisters.forEach((c, subPersister) -> {
			Column subclassPrimaryKey = (Column ) Iterables.first(subPersister.getMainTable().getPrimaryKey().getColumns());
			sourcePersister.getJoinedStrategiesSelect().addMergeJoin(JoinedStrategiesSelect.FIRST_STRATEGY_NAME,
					new FirstPhaseOneToOneLoader(subPersister.getMappingStrategy().getIdMappingStrategy(), subclassPrimaryKey, selectExecutor,
							mainPersister.getClassToPersist(), discriminatorColumn, polymorphismPolicy.getDiscriminatorValue(c)),
					(Set) Arrays.asHashSet(rightColumn, subclassPrimaryKey, discriminatorColumn),
					leftColumn, subclassPrimaryKey, JoinType.OUTER);
		});
		
		
		// adding second phase loader
		((IPersisterListener) sourcePersister).addSelectListener(new SecondPhaseOneToOneLoader<>(beanRelationFixer));	
	}
	
	@Override
	public JoinedStrategiesSelect<C, I, ?> getJoinedStrategiesSelect() {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public <E, ID, TT extends Table> void copyJoinsRootTo(JoinedStrategiesSelect<E, ID, TT> joinedStrategiesSelect, String joinName) {
		throw new UnsupportedOperationException();
	}
	
	private static class FirstPhaseOneToOneLoader<E, ID, D> implements EntityInflater<E, ID> {
		
		private final Column<Table, ID> primaryKeyColumn;
		private final IdMappingStrategy<E, ID> idMappingStrategy;
		private final ISelectExecutor<E, ID> selectExecutor;
		private final Class<E> mainType;
		private final Column<Table, D> discriminatorColumn;
		private final Object discriminatorValue;
		
		public FirstPhaseOneToOneLoader(IdMappingStrategy<E, ID> subEntityIdMappingStrategy,
										Column<Table, ID> primaryKeyColumn,
										ISelectExecutor<E, ID> selectExecutor,
										Class<E> mainType,
										Column<Table, D> discriminatorColumn,
										Object discriminatorValue) {
			this.primaryKeyColumn = primaryKeyColumn;
			this.idMappingStrategy = subEntityIdMappingStrategy;
			this.selectExecutor = selectExecutor;
			this.mainType = mainType;
			this.discriminatorColumn = discriminatorColumn;
			this.discriminatorValue = discriminatorValue;
		}
		
		@Override
		public Class<E> getEntityType() {
			return mainType;
		}
		
		@Override
		public ID giveIdentifier(Row row, ColumnedRow columnedRow) {
			return idMappingStrategy.getIdentifierAssembler().assemble(row, columnedRow);
		}
		
		@Override
		public AbstractTransformer<E> copyTransformerWithAliases(ColumnedRow columnedRow) {
			return new AbstractTransformer<E>(null, columnedRow) {
				
				// this is not invoked
				@Override
				public AbstractTransformer<E> copyWithAliases(ColumnedRow columnedRow) {
					throw new UnsupportedOperationException("this is not expected to be copied, row transformation algorithm as changed,"
							+ " please fix it or fix this method");
				}
				
				/**
				 * Overriden to store data into {@link SingleTablePolymorphicPersister#DIFFERED_ENTITY_LOADER}, no property filling here.
				 */
				@Override
				public void applyRowToBean(Row row, E bean) {
					Set<RelationIds<Object, E, ID>> existingSet =  (Set) DIFFERED_ENTITY_LOADER.get();
					D dtype = (D) columnedRow.getValue(discriminatorColumn, row);
					if (discriminatorValue.equals(dtype)) {
						existingSet.add(new RelationIds<>(selectExecutor,
								idMappingStrategy.getIdAccessor()::getId, bean, (ID) getColumnedRow().getValue(primaryKeyColumn, row)));
					}
				}
			};
		}
	}
	
	static class SecondPhaseOneToOneLoader<SRC, TRGT, ID> implements SelectListener<SRC, ID> {
		
		private final BeanRelationFixer<SRC, TRGT> beanRelationFixer;
		
		public SecondPhaseOneToOneLoader(BeanRelationFixer<SRC, TRGT> beanRelationFixer) {
			this.beanRelationFixer = beanRelationFixer;
		}
		
		@Override
		public void beforeSelect(Iterable<ID> ids) {
			Set<RelationIds<Object, Object, Object>> existingSet = DIFFERED_ENTITY_LOADER.get();
			if (existingSet == null) {
				existingSet = new HashSet<>();
				DIFFERED_ENTITY_LOADER.set(existingSet);
			}
		}
		
		@Override
		public void afterSelect(Iterable<? extends SRC> result) {
			selectTargetEntities(result);
			DIFFERED_ENTITY_LOADER.remove();
		}
		
		/**
		 * Mainly created to clarify types with TRGTID as parameter
		 * @param sourceEntities main entities, those that have the relation
		 * @param <TRGTID> target identifier type
		 */
		private <TRGTID> void selectTargetEntities(Iterable<? extends SRC> sourceEntities) {
			Map<ISelectExecutor<TRGT, TRGTID>, Set<TRGTID>> selectsToExecute = new HashMap<>();
			Map<ISelectExecutor<TRGT, TRGTID>, Function<TRGT, TRGTID>> idAccessors = new HashMap<>();
			Map<SRC, Set<TRGTID>> targetIdPerSource = new HashMap<>();
			Set<RelationIds<SRC, TRGT, TRGTID>> relationIds = (Set) DIFFERED_ENTITY_LOADER.get();
			// we remove null targetIds (Target Ids may be null if relation is nullified) because
			// - selecting entities with null id is non-sensence
			// - it prevents from generating SQL "in ()" which is invalid
			// - it prevents from NullPointerException when applying target to source
			relationIds.stream().filter(r -> r.getTargetId() != null).forEach(r -> {
				idAccessors.putIfAbsent(r.getSelectExecutor(), r.getIdAccessor());
				targetIdPerSource.computeIfAbsent(r.getSource(), k -> new HashSet<>()).add(r.getTargetId());
				selectsToExecute.computeIfAbsent(r.getSelectExecutor(), k -> new HashSet<>()).add(r.getTargetId());
			});
			
			// we load target entities from their ids, and map them per their loader
			Map<ISelectExecutor, List<TRGT>> targetsPerSelector = new HashMap<>();
			selectsToExecute.forEach((selectExecutor, ids) -> {
				targetsPerSelector.put(selectExecutor, selectExecutor.select(ids));
			});
			// then we apply them onto their source entities, to remember which target applies to which source, we use target id
			Map<TRGTID, TRGT> targetPerId = new HashMap<>();
			targetsPerSelector.forEach((selector, loadedTargets) -> targetPerId.putAll(Iterables.map(loadedTargets, idAccessors.get(selector))));
			sourceEntities.forEach(src -> Nullable.nullable(targetIdPerSource.get(src))	// source may not have targetIds if relation if null
					.invoke(s -> s.forEach(targetId -> beanRelationFixer.apply(src, targetPerId.get(targetId)))));
		}
		
		@Override
		public void onError(Iterable<ID> ids, RuntimeException exception) {
			DIFFERED_ENTITY_LOADER.remove();
		}
	}
	
}
