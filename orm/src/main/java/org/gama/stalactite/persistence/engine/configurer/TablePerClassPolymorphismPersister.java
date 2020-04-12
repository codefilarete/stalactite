package org.gama.stalactite.persistence.engine.configurer;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Duo;
import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.function.Functions;
import org.gama.lang.function.SerializableTriConsumer;
import org.gama.lang.trace.ModifiableInt;
import org.gama.reflection.MethodReferenceDispatcher;
import org.gama.stalactite.persistence.engine.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.ExecutableQuery;
import org.gama.stalactite.persistence.engine.IDeleteExecutor;
import org.gama.stalactite.persistence.engine.IEntityConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.IInsertExecutor;
import org.gama.stalactite.persistence.engine.ISelectExecutor;
import org.gama.stalactite.persistence.engine.IUpdateExecutor;
import org.gama.stalactite.persistence.engine.TablePerClassPolymorphicEntitySelectExecutor;
import org.gama.stalactite.persistence.engine.TablePerClassPolymorphicSelectExecutor;
import org.gama.stalactite.persistence.engine.cascade.AbstractJoin.JoinType;
import org.gama.stalactite.persistence.engine.cascade.IJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister.CriteriaProvider;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister.RelationalExecutableEntityQuery;
import org.gama.stalactite.persistence.engine.listening.DeleteByIdListener;
import org.gama.stalactite.persistence.engine.listening.DeleteListener;
import org.gama.stalactite.persistence.engine.listening.IPersisterListener;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.query.EntityCriteriaSupport;
import org.gama.stalactite.persistence.query.RelationalEntityCriteria;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.AbstractRelationalOperator;
import org.gama.stalactite.sql.ConnectionProvider;

/**
 * @author Guillaume Mary
 */
public class TablePerClassPolymorphismPersister<C, I, T extends Table<T>> implements IEntityConfiguredJoinedTablesPersister<C, I> {
	
	private static final ThreadLocal<Set<RelationIds<Object /* E */, Object /* target */, Object /* target identifier */ >>> DIFFERED_ENTITY_LOADER = new ThreadLocal<>();
	
	private final EntityCriteriaSupport<C> criteriaSupport;
	private final TablePerClassPolymorphicEntitySelectExecutor<C, I, T> entitySelectExecutor;
	private final TablePerClassPolymorphicSelectExecutor<C, I, T> selectExecutor;
	private final Map<Class<? extends C>, IInsertExecutor<C>> subclassInsertExecutors;
	private final Map<Class<? extends C>, IUpdateExecutor<C>> subclassUpdateExecutors;
	private final Map<Class<? extends C>, IDeleteExecutor<C, I>> subclassDeleteExecutors;
	@javax.annotation.Nonnull
	private final JoinedTablesPersister<C, I, T> mainPersister;
	private final Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> subEntitiesPersisters;
	
	public TablePerClassPolymorphismPersister(JoinedTablesPersister<C, I, T> mainPersister,
											  Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> subEntitiesPersisters,
											  ConnectionProvider connectionProvider,
											  Dialect dialect) {
		this.mainPersister = mainPersister;
		Set<Entry<Class<? extends C>, JoinedTablesPersister<C, I, T>>> entries = subEntitiesPersisters.entrySet();
		this.subclassInsertExecutors = Iterables.map(entries, Entry::getKey, e -> e.getValue().getInsertExecutor());
		this.subclassUpdateExecutors = Iterables.map(entries, Entry::getKey, e -> e.getValue().getUpdateExecutor());
		this.subclassDeleteExecutors = Iterables.map(entries, Entry::getKey, e -> e.getValue().getDeleteExecutor());
		
		Map<Class, Table> tablePerSubEntity = Iterables.map((Set) entries,
				Entry::getKey,
				Functions.<Entry<Class, JoinedTablesPersister>, JoinedTablesPersister, Table>chain(Entry::getValue, JoinedTablesPersister::getMainTable));
		
		
		this.subEntitiesPersisters = subEntitiesPersisters;
		this.subEntitiesPersisters.forEach((type, persister) ->
				mainPersister.getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect().getJoinsRoot().projectTo(
				persister.getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect(),
				JoinedStrategiesSelect.ROOT_STRATEGY_NAME
		));
		
		Map<Class<? extends C>, ISelectExecutor<C, I>> subEntitiesSelectors = Iterables.map(subEntitiesPersisters.entrySet(),
				Entry::getKey,
				Functions.chain(Entry::getValue, JoinedTablesPersister::getSelectExecutor));
		this.selectExecutor = new TablePerClassPolymorphicSelectExecutor<>(
				tablePerSubEntity,
				subEntitiesSelectors,
				mainPersister.getMainTable(), connectionProvider, dialect.getColumnBinderRegistry());
		
		this.entitySelectExecutor = new TablePerClassPolymorphicEntitySelectExecutor<>(tablePerSubEntity, subEntitiesPersisters,
				mainPersister.getMainTable(), connectionProvider, dialect.getColumnBinderRegistry());
		
		this.criteriaSupport = new EntityCriteriaSupport<>(mainPersister.getMappingStrategy());
	}
	
	@Override
	public Collection<Table> giveImpliedTables() {
		// in table-per-class main persister table does not participate in database schema : only sub entities persisters do
		return this.subEntitiesPersisters.values().stream().map(JoinedTablesPersister::getMappingStrategy).map(IEntityMappingStrategy::getTargetTable).collect(Collectors.toList());
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
		Map<Class, Set<C>> entitiesPerType = new HashMap<>();
		for (C entity : entities) {
			entitiesPerType.computeIfAbsent(entity.getClass(), cClass -> new HashSet<>()).add(entity);
		}
		ModifiableInt deleteCount = new ModifiableInt();
		subclassDeleteExecutors.forEach((subclass, deleteExecutor) -> {
			Set<C> subtypeEntities = entitiesPerType.get(subclass);
			if (subtypeEntities != null) {
				deleteCount.increment(deleteExecutor.delete(subtypeEntities));
			}
		});
		return deleteCount.getValue();
	}
	
	@Override
	public int deleteById(Iterable<C> entities) {
		Map<Class, Set<C>> entitiesPerType = new HashMap<>();
		for (C entity : entities) {
			entitiesPerType.computeIfAbsent(entity.getClass(), cClass -> new HashSet<>()).add(entity);
		}
		ModifiableInt deleteCount = new ModifiableInt();
		subclassDeleteExecutors.forEach((subclass, deleteExecutor) -> {
			Set<C> subtypeEntities = entitiesPerType.get(subclass);
			if (subtypeEntities != null) {
				deleteCount.increment(deleteExecutor.deleteById(subtypeEntities));
			}
		});
		return deleteCount.getValue();
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
		subEntitiesPersisters.values().forEach(p -> p.addInsertListener(insertListener));
	}
	
	@Override
	public void addUpdateListener(UpdateListener updateListener) {
		subEntitiesPersisters.values().forEach(p -> p.addUpdateListener(updateListener));
	}
	
	@Override
	public void addSelectListener(SelectListener selectListener) {
		subEntitiesPersisters.values().forEach(p -> p.addSelectListener(selectListener));
	}
	
	@Override
	public void addDeleteListener(DeleteListener deleteListener) {
		subEntitiesPersisters.values().forEach(p -> p.addDeleteListener(deleteListener));
	}
	
	@Override
	public void addDeleteByIdListener(DeleteByIdListener deleteListener) {
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
						(c, f) -> subEntitiesPersisters.values().forEach(p -> {
							Column projectedColumn = p.getMainTable().addColumn(c.getName(), c.getJavaType(), c.getSize());
							p.getMappingStrategy().addSilentColumnInserter(projectedColumn, f);
						}))
				.redirect((SerializableTriConsumer<IEntityMappingStrategy<C, I, T>, Column<T, Object>, Function<C, Object>>)
								IEntityMappingStrategy::addSilentColumnUpdater,
						(c, f) -> subEntitiesPersisters.values().forEach(p -> {
							Column projectedColumn = p.getMainTable().addColumn(c.getName(), c.getJavaType(), c.getSize());
							p.getMappingStrategy().addSilentColumnUpdater(projectedColumn, f);
						}))
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
		String createdJoinNodeName = sourcePersister.getJoinedStrategiesSelect().addRelationJoin(JoinedStrategiesSelect.ROOT_STRATEGY_NAME,
				(IEntityMappingStrategy) this.getMappingStrategy(),
				leftColumn,
				rightColumn,
				optional ? JoinType.OUTER : JoinType.INNER,
				beanRelationFixer);
		
		copyJoinsRootTo(sourcePersister.getJoinedStrategiesSelect(), createdJoinNodeName);
	}
	
	@Override
	public <SRC, T1 extends Table, T2 extends Table, J> void joinAsMany(IJoinedTablesPersister<SRC, J> sourcePersister,
																	 Column<T1, J> leftColumn, Column<T2, J> rightColumn,
																	 BeanRelationFixer<SRC, C> beanRelationFixer, String joinName,
																	 boolean optional) {
		// TODO: simplify query : it joins on target table as many as subentities which can be reduced to one join if FirstPhaseOneToOneLoader
		//  can compute disciminatorValue 
		Column<T, Object> mainTablePK = Iterables.first(mainPersister.getMainTable().getPrimaryKey().getColumns());
		Map<JoinedTablesPersister, Column> joinColumnPerSubPersister = new HashMap<>();
		if (rightColumn.equals(mainTablePK)) {
			// join is made on primary key => case is association table
			subEntitiesPersisters.forEach((c, subPersister) -> {
				Column<T, Object> column = Iterables.first(subPersister.getMainTable().getPrimaryKey().getColumns());
				joinColumnPerSubPersister.put(subPersister, column);
			});
		} else {
			// join is made on a foreign key => case of relation owned by reverse side
			subEntitiesPersisters.forEach((c, subPersister) -> {
				Column<T, J> column = subPersister.getMainTable().addColumn(rightColumn.getName(), rightColumn.getJavaType());
				joinColumnPerSubPersister.put(subPersister, column);
			});
		}
		
		subEntitiesPersisters.forEach((c, subPersister) -> {
			Column subclassPrimaryKey = Iterables.first(subPersister.getMainTable().getPrimaryKey().getColumns());
			sourcePersister.getJoinedStrategiesSelect().addMergeJoin(joinName,
					new FirstPhaseOneToOneLoader(subPersister.getMappingStrategy().getIdMappingStrategy(), subclassPrimaryKey, selectExecutor,
							mainPersister.getClassToPersist(), DIFFERED_ENTITY_LOADER),
					(Set) Arrays.asHashSet(subclassPrimaryKey),
					leftColumn, joinColumnPerSubPersister.get(subPersister), JoinType.OUTER);
		});
		
		// adding second phase loader
		((IPersisterListener) sourcePersister).addSelectListener(new SecondPhaseOneToOneLoader<>(beanRelationFixer, DIFFERED_ENTITY_LOADER));
	}
	
	@Override
	public JoinedStrategiesSelect<C, I, ?> getJoinedStrategiesSelect() {
		return mainPersister.getJoinedStrategiesSelect();
	}
	
	@Override
	public <E, ID, T extends Table> void copyJoinsRootTo(JoinedStrategiesSelect<E, ID, T> joinedStrategiesSelect, String joinName) {
		getJoinedStrategiesSelect().getJoinsRoot().copyTo(joinedStrategiesSelect, joinName);
	}
	
}
