package org.gama.stalactite.persistence.engine.configurer;

import java.util.ArrayList;
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
import org.gama.lang.function.Functions;
import org.gama.lang.trace.ModifiableInt;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.MethodReferenceDispatcher;
import org.gama.stalactite.persistence.engine.ExecutableQuery;
import org.gama.stalactite.persistence.engine.IDeleteExecutor;
import org.gama.stalactite.persistence.engine.IInsertExecutor;
import org.gama.stalactite.persistence.engine.IPersister;
import org.gama.stalactite.persistence.engine.ISelectExecutor;
import org.gama.stalactite.persistence.engine.IUpdateExecutor;
import org.gama.stalactite.persistence.engine.JoinedTablesPolymorphismEntitySelectExecutor;
import org.gama.stalactite.persistence.engine.JoinedTablesPolymorphismSelectExecutor;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.JoinedTablesPolymorphism;
import org.gama.stalactite.persistence.engine.SubEntityMappingConfiguration;
import org.gama.stalactite.persistence.engine.TableNamingStrategy;
import org.gama.stalactite.persistence.engine.UpdateExecutor;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister.CriteriaProvider;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister.RelationalExecutableEntityQuery;
import org.gama.stalactite.persistence.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.Identification;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.MappingPerTable.Mapping;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.PolymorphismBuilder;
import org.gama.stalactite.persistence.engine.listening.DeleteByIdListener;
import org.gama.stalactite.persistence.engine.listening.DeleteListener;
import org.gama.stalactite.persistence.engine.listening.InsertListener;
import org.gama.stalactite.persistence.engine.listening.SelectListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener;
import org.gama.stalactite.persistence.engine.listening.UpdateListener.UpdatePayload;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.query.EntityCriteriaSupport;
import org.gama.stalactite.persistence.query.RelationalEntityCriteria;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.AbstractRelationalOperator;
import org.gama.stalactite.sql.ConnectionProvider;

import static org.gama.lang.Nullable.nullable;

/**
 * @author Guillaume Mary
 */
abstract class JoinedTablesPolymorphismBuilder<C, I, T extends Table> implements PolymorphismBuilder<C, I, T> {
	
	private final JoinedTablesPolymorphism<C, I> polymorphismPolicy;
	private final JoinedTablesPersister<C, I, T> mainPersister;
	private final ColumnBinderRegistry columnBinderRegistry;
	private final ColumnNameProvider columnNameProvider;
	private final Identification identification;
	private final TableNamingStrategy tableNamingStrategy;
	private final Set<Table> tables = new HashSet<>();
	private Map<Class<? extends C>, ISelectExecutor<C, I>> subEntitiesSelectors;
	private Map<Class<? extends C>, IUpdateExecutor<C>> subclassUpdateExecutors;
	
	JoinedTablesPolymorphismBuilder(JoinedTablesPolymorphism<C, I> polymorphismPolicy,
									Identification identification,
									JoinedTablesPersister<C, I, T> mainPersister,
									Mapping mainMapping,
									ColumnBinderRegistry columnBinderRegistry,
									ColumnNameProvider columnNameProvider,
									TableNamingStrategy tableNamingStrategy) {
		this.polymorphismPolicy = polymorphismPolicy;
		this.identification = identification;
		this.mainPersister = mainPersister;
		this.columnBinderRegistry = columnBinderRegistry;
		this.columnNameProvider = columnNameProvider;
		this.tableNamingStrategy = tableNamingStrategy;
	}
	
	private Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> buildSubEntitiesPersisters(PersistenceContext persistenceContext) {
		Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> persisterPerSubclass = new HashMap<>();
		
		BeanMappingBuilder beanMappingBuilder = new BeanMappingBuilder();
		subEntitiesSelectors = new HashMap<>();
		
		
		subclassUpdateExecutors = new HashMap<>();
		for (SubEntityMappingConfiguration<? extends C, I> subConfiguration : polymorphismPolicy.getSubClasses()) {
			Table subTable = nullable(polymorphismPolicy.giveTable(subConfiguration))
					.getOr(() -> new Table(tableNamingStrategy.giveName(subConfiguration.getEntityType())));
			tables.add(subTable);
			Map<IReversibleAccessor, Column> subEntityPropertiesMapping = beanMappingBuilder.build(subConfiguration.getPropertiesMapping(), subTable,
					this.columnBinderRegistry, this.columnNameProvider);
			addPrimarykey(identification, subTable);
			addForeignKey(identification, subTable);
			Mapping subEntityMapping = new Mapping(subConfiguration, subTable, subEntityPropertiesMapping, false);
			addIdentificationToMapping(identification, subEntityMapping);
			ClassMappingStrategy<? extends C, I, Table> classMappingStrategy = PersisterBuilderImpl.createClassMappingStrategy(
					false,
					subTable,
					subEntityPropertiesMapping,
					identification,
					// TODO: no generated keys handler for now, should be taken on main strategy or brought by identification
					null,
					subConfiguration.getPropertiesMapping().getBeanType());
			
			JoinedTablesPersister subclassPersister = new JoinedTablesPersister(persistenceContext, classMappingStrategy);
			persisterPerSubclass.put(subConfiguration.getEntityType(), subclassPersister);
			
			// Adding join with parent table to select
			Column subEntityPrimaryKey = (Column) Iterables.first(subTable.getPrimaryKey().getColumns());
			Column entityPrimaryKey = (Column) Iterables.first(this.mainPersister.getMainTable().getPrimaryKey().getColumns());
			subclassPersister.getJoinedStrategiesSelectExecutor().addComplementaryJoin(JoinedStrategiesSelect.FIRST_STRATEGY_NAME,
					this.mainPersister.getMappingStrategy(), subEntityPrimaryKey, entityPrimaryKey);
			subEntitiesSelectors.put(subConfiguration.getEntityType(), subclassPersister.getSelectExecutor());
			
			
			subclassUpdateExecutors.put(subConfiguration.getEntityType(), new IUpdateExecutor<C>() {
				@Override
				public int updateById(Iterable<C> entities) {
					// TODO
					return 0;
				}
				
				@Override
				public int update(Iterable<? extends Duo<? extends C, ? extends C>> differencesIterable, boolean allColumnsStatement) {
					List<UpdatePayload<C, Table>> updatePayloads = new ArrayList<>();
					differencesIterable.forEach(duo ->
							updatePayloads.add(new UpdatePayload<>(duo, (Map) mainPersister.getMappingStrategy().getUpdateValues(duo.getLeft(),
							duo.getRight(), allColumnsStatement)))
					);
					int rowCount;
					if (Iterables.isEmpty(updatePayloads)) {
						// nothing to update => we return immediatly without any call to listeners
						rowCount = 0;
					} else {
						rowCount = ((UpdateExecutor) mainPersister.getUpdateExecutor()).updateDifferences(updatePayloads, allColumnsStatement);
					}
					
					updatePayloads.clear();
					differencesIterable.forEach(duo ->
						updatePayloads.add(new UpdatePayload<>(duo, subclassPersister.getMappingStrategy().getUpdateValues(duo.getLeft(),
								duo.getRight(), allColumnsStatement)))
					);
					if (!Iterables.isEmpty(updatePayloads)) {
						int subEntityRowCount = subclassPersister.getUpdateExecutor().updateDifferences(updatePayloads, allColumnsStatement);
						// RowCount is either 0 or number of rows updated. In the first case result might be number of rows updated by sub entities.
						// In second case we don't need to change it because sub entities updated row count will also be either 0 or total entities count  
						if (rowCount == 0) {
							rowCount = subEntityRowCount;
						}
					}
					
					return rowCount;
				}
			});
			
			
		}
		
		return persisterPerSubclass;
	}
	
	abstract void addPrimarykey(Identification identification, Table table);
	
	abstract void addForeignKey(Identification identification, Table table);
	
	abstract void addIdentificationToMapping(Identification identification, Mapping mapping);
	
	@Override
	public IPersister<C, I> build(PersistenceContext persistenceContext) {
		Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> joinedTablesPersisters = buildSubEntitiesPersisters(persistenceContext);
		// NB: persisters are not registered into PersistenceContext because it may break implicit polymorphism principle (persisters are then
		// available by PersistenceContext.getPersister(..)) and it is one sure that they are perfect ones (all their features should be tested)
		// joinedTablesPersisters.values().forEach(persistenceContext::addPersister);
		return wrap(mainPersister, joinedTablesPersisters, persistenceContext.getConnectionProvider(), persistenceContext.getDialect());
	}
	
	private IPersister<C, I> wrap(JoinedTablesPersister<C, I, T> mainPersister,
								  Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> subEntitiesPersisters,
								  ConnectionProvider connectionProvider,
								  Dialect dialect) {
		Set<Entry<Class<? extends C>, JoinedTablesPersister<C, I, T>>> entries = subEntitiesPersisters.entrySet();
		Map<Class<? extends C>, IInsertExecutor<C>> subclassInsertExecutors =
				Iterables.map(entries, Entry::getKey, e -> e.getValue().getInsertExecutor());
		Map<Class<? extends C>, IDeleteExecutor<C, I>> subclassDeleteExecutors =
				Iterables.map(entries, Entry::getKey, e -> e.getValue().getDeleteExecutor());
		
		
		JoinedTablesPolymorphismSelectExecutor<C, I, T> selectExecutor = new JoinedTablesPolymorphismSelectExecutor<>(
				Iterables.map(subEntitiesPersisters.entrySet(),
						Entry::getKey, Functions.chain(Entry<Class<? extends C>, JoinedTablesPersister<C, I, T>>::getValue, JoinedTablesPersister::getMainTable)),
				subEntitiesSelectors,
				mainPersister.getMainTable(), connectionProvider, dialect);
		
		JoinedTablesPolymorphismEntitySelectExecutor<C, I, T> entitySelectExecutor =
				new JoinedTablesPolymorphismEntitySelectExecutor<>(subEntitiesPersisters, subEntitiesPersisters, mainPersister.getMainTable(),
						mainPersister.getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect(), connectionProvider, dialect);
		
		EntityCriteriaSupport<C> criteriaSupport = new EntityCriteriaSupport<>(mainPersister.getMappingStrategy());
		
		return new PersisterListenerWrapper<>(new IPersister<C, I>() {
			
			@Override
			public Collection<Table> giveImpliedTables() {
				return Collections.cat(mainPersister.giveImpliedTables(), JoinedTablesPolymorphismBuilder.this.tables);
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
				
				return mainPersister.delete(entities);
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
				
				return mainPersister.deleteById(entities);
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
			public <T extends Table<T>> IEntityMappingStrategy<C, I, T> getMappingStrategy() {
				return (IEntityMappingStrategy<C, I, T>) mainPersister.getMappingStrategy();
			}
		});
	}
}
