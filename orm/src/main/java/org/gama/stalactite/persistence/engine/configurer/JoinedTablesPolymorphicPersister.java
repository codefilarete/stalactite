package org.gama.stalactite.persistence.engine.configurer;

import java.util.Collection;
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
import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Collections;
import org.gama.lang.collection.Iterables;
import org.gama.lang.exception.NotImplementedException;
import org.gama.lang.trace.ModifiableInt;
import org.gama.reflection.MethodReferenceDispatcher;
import org.gama.stalactite.persistence.engine.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.ExecutableQuery;
import org.gama.stalactite.persistence.engine.IConfiguredPersister;
import org.gama.stalactite.persistence.engine.IDeleteExecutor;
import org.gama.stalactite.persistence.engine.IEntityConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.IInsertExecutor;
import org.gama.stalactite.persistence.engine.ISelectExecutor;
import org.gama.stalactite.persistence.engine.IUpdateExecutor;
import org.gama.stalactite.persistence.engine.JoinedTablesPolymorphismEntitySelectExecutor;
import org.gama.stalactite.persistence.engine.JoinedTablesPolymorphismSelectExecutor;
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
 * Class that wraps some other persisters and transfers its invokations to them.
 * Used for polymorphism to dispatch method calls to sub-entities persisters.
 * 
 * @author Guillaume Mary
 */
public class JoinedTablesPolymorphicPersister<C, I> implements IEntityConfiguredJoinedTablesPersister<C, I>, PolymorphicPersister<C, I> {
	
	private static final ThreadLocal<Set<RelationIds<Object /* E */, Object /* target */, Object /* target identifier */ >>> DIFFERED_ENTITY_LOADER = new ThreadLocal<>();
	
	private final PersisterListenerWrapper<C, I> persisterListenerWrapper;
	private final Map<Class<? extends C>, ? extends IEntityConfiguredJoinedTablesPersister<C, I>> subEntitiesPersisters;
	/** The wrapper around wub entities loaders, for 2-phases load  */
	private final JoinedTablesPolymorphismSelectExecutor<C, I, ?> mainSelectExecutor;
	private final Class<C> parentClass;
	private final Map<Class<? extends C>, IInsertExecutor<C>> subclassInsertExecutors;
	private final Map<Class<? extends C>, IUpdateExecutor<C>> subclassUpdateExecutors;
	private final Map<Class<? extends C>, ISelectExecutor<C, I>> subclassSelectExecutors;
	private final Map<Class<? extends C>, IDeleteExecutor<C, I>> subclassDeleteExecutors;
	private final Map<Class<? extends C>, IdMappingStrategy<C, I>> subclassIdMappingStrategies;
	private final Map<Class<? extends C>, Table> tablePerSubEntity;
	private final Column<?, I> mainTablePrimaryKey;
	
	public JoinedTablesPolymorphicPersister(JoinedTablesPersister<C, I, ?> parentPersister,
											Map<Class<? extends C>, JoinedTablesPersister<C, I, ?>> subEntitiesPersisters,
											Map<Class<? extends C>, IUpdateExecutor<C>> subclassUpdateExecutorsOverride,
											ConnectionProvider connectionProvider,
											Dialect dialect) {
		this.parentClass = parentPersister.getClassToPersist();
		this.mainTablePrimaryKey = (Column) Iterables.first(parentPersister.getMappingStrategy().getTargetTable().getPrimaryKey().getColumns());
		
		this.subEntitiesPersisters = subEntitiesPersisters;
		this.subclassUpdateExecutors = subclassUpdateExecutorsOverride;
		Set<Entry<Class<? extends C>, JoinedTablesPersister<C, I, ?>>> entries = subEntitiesPersisters.entrySet();
		this.subclassInsertExecutors = Iterables.map(entries, Entry::getKey, e -> e.getValue().getInsertExecutor());
		this.subclassDeleteExecutors = Iterables.map(entries, Entry::getKey, e -> e.getValue().getDeleteExecutor());
		this.subclassSelectExecutors = Iterables.map(entries, Entry::getKey, e -> e.getValue().getSelectExecutor());
		this.subclassIdMappingStrategies = Iterables.map(entries, Entry::getKey, e -> e.getValue().getMappingStrategy().getIdMappingStrategy());
		
		// sub entities persisters will be used to select sub entities but at this point they lacks subgraph loading, so we add it (from their parent)
		subEntitiesPersisters.forEach((type, persister) -> 
			parentPersister.copyJoinsRootTo(persister.getJoinedStrategiesSelect(), JoinedStrategiesSelect.FIRST_STRATEGY_NAME)
		);
		
		this.tablePerSubEntity = Iterables.map(this.subEntitiesPersisters.entrySet(),
				Entry::getKey,
				entry -> entry.getValue().getMappingStrategy().getTargetTable());
		this.mainSelectExecutor = new JoinedTablesPolymorphismSelectExecutor<>(
				tablePerSubEntity,
				this.subclassSelectExecutors,
				parentPersister.getMainTable(), connectionProvider, dialect);
		
		JoinedTablesPolymorphismEntitySelectExecutor<C, I, ?> entitySelectExecutor =
				new JoinedTablesPolymorphismEntitySelectExecutor(subEntitiesPersisters, subEntitiesPersisters, parentPersister.getMainTable(),
						parentPersister.getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect(), connectionProvider, dialect);
		
		EntityCriteriaSupport<C> criteriaSupport = new EntityCriteriaSupport<>(parentPersister.getMappingStrategy());
		
		List<Table> subTables = Iterables.collectToList(subEntitiesPersisters.values(), p -> p.getMappingStrategy().getTargetTable());
		
		this.persisterListenerWrapper = new PersisterListenerWrapper<>(new IEntityConfiguredJoinedTablesPersister<C, I>() {
			
			@Override
			public <U, J, Z> String addPersister(String ownerStrategyName, IConfiguredPersister<U, J> persister, BeanRelationFixer<Z, U> beanRelationFixer, Column leftJoinColumn, Column rightJoinColumn, boolean isOuterJoin) {
				throw new UnsupportedOperationException();
			}
			
			@Override
			public <SRC> void joinAsOne(IJoinedTablesPersister<SRC, I> sourcePersister, Column leftColumn, Column rightColumn,
										BeanRelationFixer<SRC, C> beanRelationFixer, boolean nullable) {
				throw new UnsupportedOperationException();
			}
			
			@Override
			public JoinedStrategiesSelect<C, I, ?> getJoinedStrategiesSelect() {
				return parentPersister.getJoinedStrategiesSelect();
			}
			
			@Override
			public <E, ID, T extends Table> void copyJoinsRootTo(JoinedStrategiesSelect<E, ID, T> joinedStrategiesSelect, String joinName) {
				getJoinedStrategiesSelect().getJoinsRoot().copyTo(joinedStrategiesSelect, joinName);
			}
			
			@Override
			public Collection<Table> giveImpliedTables() {
				return Collections.cat(parentPersister.giveImpliedTables(), subTables);
			}
			
			@Override
			public PersisterListener<C, I> getPersisterListener() {
				return parentPersister.getPersisterListener();
			}
			
			@Override
			public int insert(Iterable<? extends C> entities) {
				parentPersister.insert(entities);
				Map<Class, Set<C>> entitiesPerType = new HashMap<>();
				for (C entity : entities) {
					entitiesPerType.computeIfAbsent(entity.getClass(), cClass -> new HashSet<>()).add(entity);
				}
				ModifiableInt insertCount = new ModifiableInt();
				JoinedTablesPolymorphicPersister.this.subclassInsertExecutors.forEach((subclass, insertExecutor) -> {
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
				JoinedTablesPolymorphicPersister.this.subclassUpdateExecutors.forEach((subclass, updateExecutor) -> {
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
				JoinedTablesPolymorphicPersister.this.subclassUpdateExecutors.forEach((subclass, updateExecutor) -> {
					Set<Duo<? extends C, ? extends C>> entitiesToUpdate = entitiesPerType.get(subclass);
					if (entitiesToUpdate != null) {
						updateCount.increment(updateExecutor.update(entitiesToUpdate, allColumnsStatement));
					}
				});
				
				return updateCount.getValue();
			}
			
			@Override
			public List<C> select(Iterable<I> ids) {
				return mainSelectExecutor.select(ids);
			}
			
			@Override
			public int delete(Iterable<C> entities) {
				Map<Class, Set<C>> entitiesPerType = new HashMap<>();
				for (C entity : entities) {
					entitiesPerType.computeIfAbsent(entity.getClass(), cClass -> new HashSet<>()).add(entity);
				}
				ModifiableInt deleteCount = new ModifiableInt();
				JoinedTablesPolymorphicPersister.this.subclassDeleteExecutors.forEach((subclass, deleteExecutor) -> {
					Set<C> subtypeEntities = entitiesPerType.get(subclass);
					if (subtypeEntities != null) {
						deleteCount.increment(deleteExecutor.delete(subtypeEntities));
					}
				});
				// NB: we use deleteExecutor not to trigger listener, because they should be triggered by wrapper, else we would try to delete twice
				// related beans for instance
				return parentPersister.getDeleteExecutor().delete(entities);
			}
			
			@Override
			public int deleteById(Iterable<C> entities) {
				Map<Class, Set<C>> entitiesPerType = new HashMap<>();
				for (C entity : entities) {
					entitiesPerType.computeIfAbsent(entity.getClass(), cClass -> new HashSet<>()).add(entity);
				}
				ModifiableInt deleteCount = new ModifiableInt();
				JoinedTablesPolymorphicPersister.this.subclassDeleteExecutors.forEach((subclass, deleteExecutor) -> {
					Set<C> subtypeEntities = entitiesPerType.get(subclass);
					if (subtypeEntities != null) {
						deleteCount.increment(deleteExecutor.deleteById(subtypeEntities));
					}
				});
				// NB: we use deleteExecutor not to trigger listener, because they should be triggered by wrapper, else we would try to delete twice
				// related beans for instance
				return parentPersister.getDeleteExecutor().deleteById(entities);
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
				return parentPersister.isNew(entity);
			}
			
			@Override
			public Class<C> getClassToPersist() {
				return parentClass;
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
				return parentPersister.getMappingStrategy();
			}
		});
	}
	
	@Override
	public <U, J, Z> String addPersister(String ownerStrategyName, IConfiguredPersister<U, J> persister, BeanRelationFixer<Z, U> beanRelationFixer,
										 Column leftJoinColumn, Column rightJoinColumn, boolean isOuterJoin) {
		throw new NotImplementedException("Waiting for use case");
	}
	
	/**
	 * Implementation made for one-to-one use case
	 * 
	 * @param sourcePersister source that needs this instance joins
	 * @param leftColumn left part of the join, expected to be one of source table 
	 * @param rightColumn right part of the join, expected to be one of current instance table
	 * @param beanRelationFixer setter that fix relation ofthis instance onto source persister instance
	 * @param nullable true for optional relation, makes an outer join, else should create a inner join
	 * @param <SRC>
	 */
	@Override
	public <SRC> void joinAsOne(IJoinedTablesPersister<SRC, I> sourcePersister,
								Column leftColumn, Column rightColumn, BeanRelationFixer<SRC, C> beanRelationFixer, boolean nullable) {
		
		// because subgraph loading is made in 2 phases (load ids, then entities in a second SQL request done by load listener) we add a passive join
		// (we don't need to create bean nor fulfill properties in first phase) 
		// NB: here rightColumn is parent class primary key or reverse column that owns property (depending how one-to-one relation is mapped) 
		String mainTableJoinName = sourcePersister.getJoinedStrategiesSelect().addPassiveJoin(JoinedStrategiesSelect.FIRST_STRATEGY_NAME,
				leftColumn, rightColumn, nullable ? JoinType.OUTER : JoinType.INNER, (Set<Column<Table, Object>>) (Set) Arrays.asSet(rightColumn));
		Column primaryKey = (Column ) Iterables.first(getMappingStrategy().getTargetTable().getPrimaryKey().getColumns());
		this.subclassIdMappingStrategies.forEach((c, idMappingStrategy) -> {
			Column subclassPrimaryKey = (Column) Iterables.first(this.tablePerSubEntity.get(c).getPrimaryKey().getColumns());
			sourcePersister.getJoinedStrategiesSelect().addMergeJoin(mainTableJoinName,
					new FirstPhaseOneToOneLoader<C, I>(idMappingStrategy, subclassPrimaryKey, mainSelectExecutor, parentClass),
					(Set) java.util.Collections.singleton(subclassPrimaryKey),
					primaryKey,
					subclassPrimaryKey,
					// since we don't know what kind of sub entity is present we must do an OUTER join between common truk and all sub tables
					JoinType.OUTER);
		});
		
		// adding second phase loader
		((IPersisterListener) sourcePersister).addSelectListener(new SecondPhaseOneToOneLoader<>(beanRelationFixer));
	}
	
	@Override
	public <SRC> void joinAsMany(IJoinedTablesPersister<SRC, I> sourcePersister,
								 Column sourcePrimaryKey, Column relationOwner, BeanRelationFixer<SRC, C> beanRelationFixer, String joinName) {
		
		
		// Subgraph loading is made in 2 phases (load ids, then entities in a second SQL request done by load listener)
		this.subclassIdMappingStrategies.forEach((c, idMappingStrategy) -> {
			Column subclassPrimaryKey = (Column) Iterables.first(this.tablePerSubEntity.get(c).getPrimaryKey().getColumns());
			sourcePersister.getJoinedStrategiesSelect().addMergeJoin(joinName,
					new FirstPhaseOneToOneLoader<C, I>(idMappingStrategy, subclassPrimaryKey, mainSelectExecutor, parentClass),
					(Set) Arrays.asSet(subclassPrimaryKey),
					mainTablePrimaryKey,
					subclassPrimaryKey,
					// since we don't know what kind of sub entity is present we must do an OUTER join between common truk and all sub tables
					JoinType.OUTER);
		});
		
		// adding second phase loader
		((IPersisterListener) sourcePersister).addSelectListener(new SecondPhaseOneToOneLoader<>(beanRelationFixer));
	}
	
	// for one-to-many cases
	@Override
	public <E, ID, T extends Table> void copyJoinsRootTo(JoinedStrategiesSelect<E, ID, T> joinedStrategiesSelect, String joinName) {
		// nothing to do here, called by one-to-many engines, which actually call joinWithMany()
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
	
	public static class RelationIds<SRC, TRGT, TRGTID> {
		private final ISelectExecutor<TRGT, TRGTID> selectExecutor;
		private final Function<TRGT, TRGTID> idAccessor;
		private final SRC source;
		private final TRGTID targetId;
		
		
		private RelationIds(ISelectExecutor<TRGT, TRGTID> selectExecutor, Function<TRGT, TRGTID> idAccessor, SRC source, TRGTID targetId) {
			this.selectExecutor = selectExecutor;
			this.idAccessor = idAccessor;
			this.source = source;
			this.targetId = targetId;
		}
		
		public ISelectExecutor<TRGT, TRGTID> getSelectExecutor() {
			return selectExecutor;
		}
		
		public Function<TRGT, TRGTID> getIdAccessor() {
			return idAccessor;
		}
		
		public SRC getSource() {
			return source;
		}
		
		public TRGTID getTargetId() {
			return targetId;
		}
	}
	
	static class FirstPhaseOneToOneLoader<E, ID> implements EntityInflater<E, ID> {
		
		private final Column<Table, ID> primaryKeyColumn;
		private final IdMappingStrategy<E, ID> idMappingStrategy;
		private final ISelectExecutor<E, ID> selectExecutor;
		private final Class<E> mainType;
		
		public FirstPhaseOneToOneLoader(IdMappingStrategy<E, ID> subEntityIdMappingStrategy,
										Column<Table, ID> primaryKeyColumn,
										ISelectExecutor<E, ID> selectExecutor,
										Class<E> mainType) {
			this.primaryKeyColumn = primaryKeyColumn;
			this.idMappingStrategy = subEntityIdMappingStrategy;
			this.selectExecutor = selectExecutor;
			this.mainType = mainType;
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
				
				@Override
				public void applyRowToBean(Row row, E bean) {
					Set<RelationIds<Object, E, ID>> existingSet =  (Set) DIFFERED_ENTITY_LOADER.get();
					existingSet.add(new RelationIds<>(selectExecutor,
							idMappingStrategy.getIdAccessor()::getId, bean, (ID) getColumnedRow().getValue(primaryKeyColumn, row)));
				}
			};
		}
	}
	
	private static class SecondPhaseOneToOneLoader<SRC, TRGT, ID> implements SelectListener<SRC, ID> {
		
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
			Set<RelationIds<SRC, TRGT, TRGTID>> relationIds = (Set) JoinedTablesPolymorphicPersister.DIFFERED_ENTITY_LOADER.get();
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
