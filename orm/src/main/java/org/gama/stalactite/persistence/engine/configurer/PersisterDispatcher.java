package org.gama.stalactite.persistence.engine.configurer;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Duo;
import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Collections;
import org.gama.lang.collection.Iterables;
import org.gama.lang.exception.NotImplementedException;
import org.gama.lang.function.Functions;
import org.gama.lang.trace.ModifiableInt;
import org.gama.reflection.MethodReferenceDispatcher;
import org.gama.stalactite.persistence.engine.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.ExecutableQuery;
import org.gama.stalactite.persistence.engine.IConfiguredPersister;
import org.gama.stalactite.persistence.engine.IDeleteExecutor;
import org.gama.stalactite.persistence.engine.IEntityConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.IEntityConfiguredPersister;
import org.gama.stalactite.persistence.engine.IInsertExecutor;
import org.gama.stalactite.persistence.engine.ISelectExecutor;
import org.gama.stalactite.persistence.engine.IUpdateExecutor;
import org.gama.stalactite.persistence.engine.JoinedTablesPolymorphismEntitySelectExecutor;
import org.gama.stalactite.persistence.engine.JoinedTablesPolymorphismSelectExecutor;
import org.gama.stalactite.persistence.engine.cascade.IJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister.CriteriaProvider;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister.RelationalExecutableEntityQuery;
import org.gama.stalactite.persistence.engine.listening.DeleteByIdListener;
import org.gama.stalactite.persistence.engine.listening.DeleteListener;
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
 * Class that wraps some other persisters and transfers its invokations to them.
 * Used for polymorphism to dispatch method calls to sub-entities persisters.
 * 
 * @author Guillaume Mary
 */
class PersisterDispatcher<C, I> implements IEntityConfiguredJoinedTablesPersister<C, I> {
	
	private final PersisterListenerWrapper<C, I> persisterListenerWrapper;
	private final Map<Class<? extends C>, JoinedTablesPersister<C, I, ?>> subEntitiesPersisters;
	
	public PersisterDispatcher(JoinedTablesPersister<C, I, ?> mainPersister,
							   Map<Class<? extends C>, JoinedTablesPersister<C, I, ?>> subEntitiesPersisters,
							   ConnectionProvider connectionProvider,
							   Dialect dialect) {
		
		this.subEntitiesPersisters = subEntitiesPersisters;
		Set<Entry<Class<? extends C>, JoinedTablesPersister<C, I, ?>>> entries = this.subEntitiesPersisters.entrySet();
		Map<Class<? extends C>, IInsertExecutor<C>> subclassInsertExecutors =
				Iterables.map(entries, Entry::getKey, e -> e.getValue().getInsertExecutor());
		Map<Class<? extends C>, IUpdateExecutor<C>> subclassUpdateExecutors =
				Iterables.map(entries, Entry::getKey, e -> e.getValue().getUpdateExecutor());
		Map<Class<? extends C>, ISelectExecutor<C, I>> subclassSelectExecutors =
				Iterables.map(entries, Entry::getKey, e -> e.getValue().getSelectExecutor());
		Map<Class<? extends C>, IDeleteExecutor<C, I>> subclassDeleteExecutors =
				Iterables.map(entries, Entry::getKey, e -> e.getValue().getDeleteExecutor());
			
			
			
		subEntitiesPersisters.forEach((type, persister) -> 
			mainPersister.getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect().getJoinsRoot().copyTo(
					persister.getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect(),
					JoinedStrategiesSelect.FIRST_STRATEGY_NAME
			)
		);
		
		JoinedTablesPolymorphismSelectExecutor<C, I, ?> selectExecutor = new JoinedTablesPolymorphismSelectExecutor<>(
				Iterables.map(subEntitiesPersisters.entrySet(),
						Entry::getKey, Functions.chain(Entry<Class<? extends C>, JoinedTablesPersister<C, I, ?>>::getValue, JoinedTablesPersister::getMainTable)),
				subclassSelectExecutors,
				mainPersister.getMainTable(), connectionProvider, dialect);
		
		JoinedTablesPolymorphismEntitySelectExecutor<C, I, ?> entitySelectExecutor =
				new JoinedTablesPolymorphismEntitySelectExecutor(subEntitiesPersisters, subEntitiesPersisters, mainPersister.getMainTable(),
						mainPersister.getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect(), connectionProvider, dialect);
		
		EntityCriteriaSupport<C> criteriaSupport = new EntityCriteriaSupport<>(mainPersister.getMappingStrategy());
		
		List<Table> subTables = Iterables.collectToList(subEntitiesPersisters.values(), JoinedTablesPersister::getMainTable);
		
		this.persisterListenerWrapper = new PersisterListenerWrapper<>(new IEntityConfiguredPersister<C, I>() {
			
			@Override
			public Collection<Table> giveImpliedTables() {
				return Collections.cat(mainPersister.giveImpliedTables(), subTables);
			}
			
			@Override
			public PersisterListener<C, I> getPersisterListener() {
				return mainPersister.getPersisterListener();
			}
			
			@Override
			public int insert(Iterable<? extends C> entities) {
				mainPersister.insert(entities);
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
				// NB: we use deleteExecutor not to trigger listener, because they should be triggered by wrapper, else we would try to delete twice
				// related beans for instance
				return mainPersister.getDeleteExecutor().delete(entities);
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
			
			@Override
			public IEntityMappingStrategy<C, I, ?> getMappingStrategy() {
				return mainPersister.getMappingStrategy();
			}
		});
	}
	
	@Override
	public <U, J, Z> String addPersister(String ownerStrategyName, IConfiguredPersister<U, J> persister, BeanRelationFixer<Z, U> beanRelationFixer,
										 Column leftJoinColumn, Column rightJoinColumn, boolean isOuterJoin) {
		throw new NotImplementedException("Waiting for use case");
	}
	
	@Override
	public void addPersisterJoins(String joinName, IJoinedTablesPersister<?, ?> sourcePersister) {
		this.subEntitiesPersisters.values().forEach(p -> p.addPersisterJoins(joinName, sourcePersister));
	}
	
	@Override
	public <I1, T extends Table, C1> void copyJoinsRootTo(JoinedStrategiesSelect<C1, I1, T> joinedStrategiesSelect, String joinName) {
		this.subEntitiesPersisters.values().forEach(p -> p.copyJoinsRootTo(joinedStrategiesSelect, joinName));
	}
	
	@Override
	public JoinedStrategiesSelect<C, I, ?> getJoinedStrategiesSelect() {
		throw new NotImplementedException("Waiting for use case");
	}
	
	@Override
	public IEntityMappingStrategy<C, I, ?> getMappingStrategy() {
		return persisterListenerWrapper.getMappingStrategy();
	}
	
	@Override
	public Collection<Table> giveImpliedTables() {
		return persisterListenerWrapper.giveImpliedTables();
	}
	
	@Override
	public PersisterListener<C, I> getPersisterListener() {
		return persisterListenerWrapper.getPersisterListener();
	}
	
	@Override
	public int persist(Iterable<C> entities) {
		return persisterListenerWrapper.persist(entities);
	}
	
	@Override
	public <O> ExecutableEntityQuery<C> selectWhere(SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator) {
		return persisterListenerWrapper.selectWhere(getter, operator);
	}
	
	@Override
	public <O> ExecutableEntityQuery<C> selectWhere(SerializableBiConsumer<C, O> setter, AbstractRelationalOperator<O> operator) {
		return persisterListenerWrapper.selectWhere(setter, operator);
	}
	
	@Override
	public List<C> selectAll() {
		return persisterListenerWrapper.selectAll();
	}
	
	@Override
	public boolean isNew(C entity) {
		return persisterListenerWrapper.isNew(entity);
	}
	
	@Override
	public Class<C> getClassToPersist() {
		return persisterListenerWrapper.getClassToPersist();
	}
	
	@Override
	public int delete(Iterable<C> entities) {
		return persisterListenerWrapper.delete(entities);
	}
	
	@Override
	public int deleteById(Iterable<C> entities) {
		return persisterListenerWrapper.deleteById(entities);
	}
	
	@Override
	public int insert(Iterable<? extends C> entities) {
		return persisterListenerWrapper.insert(entities);
	}
	
	@Override
	public List<C> select(Iterable<I> ids) {
		return persisterListenerWrapper.select(ids);
	}
	
	@Override
	public int updateById(Iterable<C> entities) {
		return persisterListenerWrapper.updateById(entities);
	}
	
	@Override
	public int update(Iterable<? extends Duo<? extends C, ? extends C>> differencesIterable, boolean allColumnsStatement) {
		return persisterListenerWrapper.update(differencesIterable, allColumnsStatement);
	}
	
	@Override
	public void addInsertListener(InsertListener<C> insertListener) {
		persisterListenerWrapper.addInsertListener(insertListener);
	}
	
	@Override
	public void addUpdateListener(UpdateListener<C> updateListener) {
		persisterListenerWrapper.addUpdateListener(updateListener);
	}
	
	@Override
	public void addSelectListener(SelectListener<C, I> selectListener) {
		persisterListenerWrapper.addSelectListener(selectListener);
	}
	
	@Override
	public void addDeleteListener(DeleteListener<C> deleteListener) {
		persisterListenerWrapper.addDeleteListener(deleteListener);
	}
	
	@Override
	public void addDeleteByIdListener(DeleteByIdListener<C> deleteListener) {
		persisterListenerWrapper.addDeleteByIdListener(deleteListener);
	}
}
