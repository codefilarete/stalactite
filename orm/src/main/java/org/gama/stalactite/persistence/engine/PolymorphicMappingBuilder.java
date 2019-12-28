package org.gama.stalactite.persistence.engine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import org.gama.lang.Duo;
import org.gama.lang.Nullable;
import org.gama.lang.Reflections;
import org.gama.lang.Retryer;
import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.trace.ModifiableInt;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.reflection.MethodReferenceDispatcher;
import org.gama.stalactite.persistence.engine.EmbeddableMappingConfiguration.Linkage;
import org.gama.stalactite.persistence.engine.EntityMappingConfiguration.InheritanceConfiguration;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.JoinedTablesPolymorphism;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.TablePerClassPolymorphism;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.StrategyJoins;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelectExecutor;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.mapping.IMappingStrategy.UpwhereColumn;
import org.gama.stalactite.persistence.query.IEntitySelectExecutor;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.dml.SQLOperation.SQLOperationListener;

/**
 * @author Guillaume Mary
 */
public class PolymorphicMappingBuilder<C, I> extends AbstractEntityMappingBuilder<C, I> {
	
	public PolymorphicMappingBuilder(EntityMappingConfiguration<C, I> entityMappingConfiguration, MethodReferenceCapturer methodSpy) {
		super(entityMappingConfiguration, methodSpy);
	}
	
	@Override
	protected <T extends Table> JoinedTablesPersister<C, I, T> doBuild(PersistenceContext persistenceContext, T targetTable) {
		PolymorphismPolicy polymorphismPolicy = configurationSupport.getPolymorphismPolicy();
		if (polymorphismPolicy instanceof SingleTablePolymorphism) {
			return buildPolymorphism((SingleTablePolymorphism<C, I, ?>) polymorphismPolicy, persistenceContext, targetTable);
		} else if (polymorphismPolicy instanceof JoinedTablesPolymorphism) {
			return buildPolymorphism((JoinedTablesPolymorphism<C, I>) polymorphismPolicy, persistenceContext, targetTable);
		} else if (polymorphismPolicy instanceof TablePerClassPolymorphism) {
			return buildPolymorphism((TablePerClassPolymorphism<C, I>) polymorphismPolicy, persistenceContext, targetTable);
		} else {
			return null;
		}
	}
	
	protected <T extends Table, D> JoinedTablesPersister<C, I, T> buildPolymorphism(
			SingleTablePolymorphism<C, I, D> polymorphismPolicy,
			PersistenceContext persistenceContext,
			T targetTable) {
		
		// grouping all mapped properties into a mapping strategy so they can fit into the same table
		List<Linkage> linkages = new ArrayList<>(configurationSupport.getPropertiesMapping().getPropertiesMapping());
		EmbeddableMappingConfiguration allInheritedPropertiesConfiguration = new MethodReferenceDispatcher()
				.redirect(EmbeddableMappingConfiguration<ColumnNamingStrategy>::getPropertiesMapping, () -> linkages)
				.fallbackOn(configurationSupport.getPropertiesMapping())
				.build(EmbeddableMappingConfiguration.class);
		
		Column discriminatorColumn = targetTable.addColumn(polymorphismPolicy.getDiscriminatorColumn(),
				polymorphismPolicy.getDiscrimintorType());
		discriminatorColumn.setNullable(false);
		
		// we give parent configuration to child one (because it hasn't it) by assembling a new configuration mainly made of subclass one
		// and inheritance elements by "overriding" properties given by sub configuration
		EntityMappingConfiguration<C, I> subClassEffectiveConfiguration = new MethodReferenceDispatcher()
				// TODO: check if redirecting inheritance wouldn't be better (would allow to get relations too)
				.redirect(EntityMappingConfiguration<C, I>::getPropertiesMapping, () -> allInheritedPropertiesConfiguration)
				.fallbackOn(configurationSupport)
				.build((Class<EntityMappingConfiguration<C, I>>) (Class) EntityMappingConfiguration.class);
		
		
		EntityMappingBuilder<C, I> polymorphicMappingBuilder = new EntityMappingBuilder<C, I>(subClassEffectiveConfiguration, methodSpy) {
			@Override
			protected <T extends Table> JoinedTablesPersister<C, I, T> newJoinedTablesPersister(PersistenceContext persistenceContext,
																								IEntityMappingStrategy<C, I, T> mainMappingStrategy) {
				
				Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> persisterPerSubclass = new HashMap<>();
				mainMappingStrategy.addSilentColumnToSelect(discriminatorColumn);
				
				for (SubEntityMappingConfiguration<? extends C, I> subConfiguration : polymorphismPolicy.getSubClasses()) {
					EntityMappingConfiguration<C, I> subEntityEffectiveConfiguration = composeEntityMappingConfiguration(
							subConfiguration, configurationSupport, subClassEffectiveConfiguration);
					
					EntityMappingBuilder<C, I> subclassMappingBuilder = new EntityMappingBuilder<C, I>(subEntityEffectiveConfiguration, methodSpy) {
						
						@Override
						protected void registerPersister(JoinedTablesPersister persister, PersistenceContext persistenceContext) {
							// does nothing because we don't want subclasses to be part of persistence context, else their persister would be available
							// through PersistenceContext#getPersister(Class) which is not expected from a implicit polymorphism (as opposed to an explicit one)
						}
					};
					JoinedTablesPersister subclassPersister = subclassMappingBuilder.build(persistenceContext, targetTable);
					persisterPerSubclass.put(subConfiguration.getEntityType(), subclassPersister);
				}
				
				return new JoinedTablesPersister<C, I, T>(persistenceContext, mainMappingStrategy) {
					
					/* No need to override delete executor instanciation because all subclasses are stored in the same table than the one
					 * given to constructor, and since there's no need to distinguish subclass instances (no field computation), they'll deleted correctly
					 */
					
					/**
					 * Overriden to prevent a {@link ClassCastException} on {@link #getJoinedStrategiesSelectExecutor()}
					 * which is not {@link JoinedTablesPersister} but a {@link SingleTablePolymorphismSelectExecutor} for this instance
					 * (see {@link #newSelectExecutor(IEntityMappingStrategy, ConnectionProvider, Dialect)})
					 * @return a singleton set of the only table
					 */
					@Override
					public Set<Table> giveImpliedTables() {
						return Collections.unmodifiableSet(Arrays.asHashSet(getMainTable()));
					}
					
					@Override
					protected InsertExecutor<C, I, T> newInsertExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy, ConnectionProvider connectionProvider,
															   DMLGenerator dmlGenerator, Retryer writeOperationRetryer, int jdbcBatchSize,
															   int inOperatorMaxSize) {
						
						Map<Class<? extends C>, InsertExecutor<C, I, T>> subclassInsertExecutors =
								Iterables.map(persisterPerSubclass.entrySet(), Entry::getKey, e -> e.getValue().getInsertExecutor());
						
						persisterPerSubclass.values().forEach(subclassPersister -> 
								subclassPersister.getMappingStrategy().addSilentColumnInserter(discriminatorColumn,
										c -> polymorphismPolicy.getDiscriminatorValue((Class<? extends C>) c.getClass()))
						);
						
						
						return new PolymorphicInsertExecutor<>(mappingStrategy, connectionProvider, dmlGenerator,
								writeOperationRetryer, jdbcBatchSize, inOperatorMaxSize,
								subclassInsertExecutors);
					}
					
					@Override
					protected UpdateExecutor<C, I, T> newUpdateExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy,
																			ConnectionProvider connectionProvider, DMLGenerator dmlGenerator,
																			Retryer writeOperationRetryer, int jdbcBatchSize,
																			int inOperatorMaxSize) {
						Map<Class<? extends C>, UpdateExecutor<C, I, T>> subclassUpdateExecutors =
								Iterables.map(persisterPerSubclass.entrySet(), Entry::getKey, e -> e.getValue().getUpdateExecutor());
						
						return new PolymorphicUpdateExecutor<>(mappingStrategy, connectionProvider, dmlGenerator,
								writeOperationRetryer, jdbcBatchSize, inOperatorMaxSize,
								subclassUpdateExecutors);
					}
					
					@Override
					protected ISelectExecutor<C, I> newSelectExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy,
																	  ConnectionProvider connectionProvider,
																	  Dialect dialect) {
						return new SingleTablePolymorphismSelectExecutor<>(persisterPerSubclass, discriminatorColumn, polymorphismPolicy,
								getMainTable(), getConnectionProvider(), dialect);
					}
					
					@Override
					protected IEntitySelectExecutor<C> newEntitySelectExecutor(Dialect dialect) {
						// we create a local persister because we can't call getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect()
						// because it requires to be a JoinedStrategiesSelectExecutor which is not, but a SingleTablePolymorphismSelectExecutor
						JoinedStrategiesSelectExecutor<C, I, T> defaultJoinedStrategiesSelectExecutor =
								new JoinedStrategiesSelectExecutor<>(getMappingStrategy(), dialect, getConnectionProvider());
						
						return new SingleTablePolymorphismEntitySelectExecutor<>(persisterPerSubclass, discriminatorColumn, polymorphismPolicy,
								defaultJoinedStrategiesSelectExecutor.getJoinedStrategiesSelect(), getConnectionProvider(), dialect);
					}
					
					@Override
					public <U, J, Z> String addPersister(String ownerStrategyName, Persister<U, J, ?> persister,
														 BeanRelationFixer<Z, U> beanRelationFixer, Column leftJoinColumn, Column rightJoinColumn,
														 boolean isOuterJoin) {
						return getSingleTablePolymorphismSelectExecutor().addRelation(ownerStrategyName, persister.getMappingStrategy(), beanRelationFixer,
								leftJoinColumn, rightJoinColumn, isOuterJoin);
					}

					private SingleTablePolymorphismSelectExecutor<C, I, T, ?> getSingleTablePolymorphismSelectExecutor() {
						return (SingleTablePolymorphismSelectExecutor<C, I, T, ?>) super.getSelectExecutor();
					}
					
					@Override
					public void addPersisterJoins(String joinName, JoinedTablesPersister<?, I, ?> sourcePersister) {
						StrategyJoins sourceJoinsSubgraphRoot = sourcePersister.getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect().getJoinsRoot();
						getSingleTablePolymorphismSelectExecutor().getPersisterPerSubclass().values().forEach(select -> {
							sourceJoinsSubgraphRoot.copyTo(select.getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect(), joinName);
						});
					}
					
				};
			}
			
		};
		
		
		return polymorphicMappingBuilder.build(persistenceContext, targetTable);
	}
	
	
	protected <T extends Table> JoinedTablesPersister<C, I, T> buildPolymorphism(
			JoinedTablesPolymorphism<C, I> polymorphismPolicy,
			PersistenceContext persistenceContext,
			T targetTable) {

		EntityMappingBuilder<C, I> polymorphicMappingBuilder = new EntityMappingBuilder<C, I>(configurationSupport, methodSpy) {
			@Override
			protected <T extends Table> JoinedTablesPersister<C, I, T> newJoinedTablesPersister(PersistenceContext persistenceContext,
																								IEntityMappingStrategy<C, I, T> mainMappingStrategy) {
				
				
				
				// subclass persister : contains only subclass properties
				Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> persisterPerSubclass = new HashMap<>();
				
				for (SubEntityMappingConfiguration<? extends C, I> subConfiguration : polymorphismPolicy.getSubClasses()) {
					EntityMappingConfiguration<C, I> subEntityConfiguration = composeEntityMappingConfiguration(
							subConfiguration, configurationSupport, null);
					
					EntityMappingBuilder<C, I> subclassMappingBuilder = new EntityMappingBuilder<C, I>(subEntityConfiguration, methodSpy) {

						@Override
						protected void registerPersister(JoinedTablesPersister persister, PersistenceContext persistenceContext) {
							// does nothing because we don't want subclasses to be part of persistence context, else their persister would be available
							// through PersistenceContext#getPersister(Class) which is not expected from a implicit polymorphism (as opposed to an explicit one)
						}
					};
					Table subEntityTargetTable = new Table(configurationSupport.getTableNamingStrategy().giveName(subEntityConfiguration.getEntityType()));
					JoinedTablesPersister subclassPersister = subclassMappingBuilder.build(persistenceContext, subEntityTargetTable);
					
					persisterPerSubclass.put(subConfiguration.getEntityType(), subclassPersister);
					Column subEntityPrimaryKey = (Column) Iterables.first(subEntityTargetTable.getPrimaryKey().getColumns());
					Column entityPrimaryKey = (Column) Iterables.first(targetTable.getPrimaryKey().getColumns());
					subEntityTargetTable.addForeignKey(configurationSupport.getForeignKeyNamingStrategy().giveName(subEntityPrimaryKey, entityPrimaryKey),
							new ArrayList<>(subEntityTargetTable.getPrimaryKey().getColumns()), new ArrayList<>(targetTable.getPrimaryKey().getColumns()));
				}
				
				// subclass persister 2 : contains subclass properties only
				Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> persisterPerSubclass2 = new HashMap<>();
				
				for (SubEntityMappingConfiguration<? extends C, I> subConfiguration : polymorphismPolicy.getSubClasses()) {
					EntityMappingConfiguration<C, I> subEntityConfiguration = composeEntityMappingConfiguration(
							subConfiguration, configurationSupport, null);
					
					EntityMappingConfiguration<C, I> parentEffectiveConfiguration = new MethodReferenceDispatcher()
							.redirect(EntityMappingConfiguration<C, I>::getPropertiesMapping, () -> {
								// overriden to fix bean type to prevent to be parent class because it is unecessary and conflicts with
								// uppest parent join persister that will be registered further
								// also done to instantiate rigth type : subentity one
								return new MethodReferenceDispatcher()
										.redirect(EmbeddableMappingConfiguration<C>::getBeanType, () -> subEntityConfiguration.getEntityType())
										.fallbackOn(configurationSupport.getPropertiesMapping())
										.build((Class<EmbeddableMappingConfiguration<C>>) (Class) EmbeddableMappingConfiguration.class);
							})
							.redirect(EntityMappingConfiguration<C, I>::getEntityFactory, () -> (Function<Column, Object> f) -> Reflections.newInstance(subEntityConfiguration.getEntityType()))
							.fallbackOn(configurationSupport)
							.build((Class<EntityMappingConfiguration<C, I>>) (Class) EntityMappingConfiguration.class);
					
					
					T subentityTargetTable =
							persisterPerSubclass.get(subEntityConfiguration.getEntityType()).getMappingStrategy().getTargetTable();
					
					EntityMappingBuilder<C, I> joinedTablesEntityMappingBuilder = new EntityMappingBuilder<C, I>(parentEffectiveConfiguration, methodSpy) {
						@Override
						protected void registerPersister(JoinedTablesPersister persister, PersistenceContext persistenceContext) {
							// does nothing because we don't want subclasses to be part of persistence context, else their persister would be available
							// through PersistenceContext#getPersister(Class) which is not expected from a implicit polymorphism (as opposed to an explicit one)
						}
					};
					JoinedTablesPersister<C, I, T> pseudoParentPersister = (JoinedTablesPersister<C, I, T>) joinedTablesEntityMappingBuilder.doBuild(persistenceContext, targetTable);
					
					
					EntityMappingBuilder<C, I> subEntityJoinedTablesEntityMappingBuilder = new EntityMappingBuilder<C, I>(subEntityConfiguration, methodSpy) {
						
						@Override
						protected void registerPersister(JoinedTablesPersister persister, PersistenceContext persistenceContext) {
							// does nothing because we don't want subclasses to be part of persistence context, else their persister would be available
							// through PersistenceContext#getPersister(Class) which is not expected from a implicit polymorphism (as opposed to an explicit one)
						}
					};
					
					JoinedTablesPersister<C, I, T> subEntityJoinedTablesPersister = subEntityJoinedTablesEntityMappingBuilder.doBuild(persistenceContext, subentityTargetTable);
					// Adding join with parent table to select
					Column subEntityPrimaryKey = (Column) Iterables.first(subentityTargetTable.getPrimaryKey().getColumns());
					Column entityPrimaryKey = (Column) Iterables.first(targetTable.getPrimaryKey().getColumns());
					pseudoParentPersister.getJoinedStrategiesSelectExecutor().addComplementaryJoin(JoinedStrategiesSelect.FIRST_STRATEGY_NAME,
							subEntityJoinedTablesPersister.getMappingStrategy(), entityPrimaryKey, subEntityPrimaryKey);
					
					persisterPerSubclass2.put(subEntityConfiguration.getEntityType(), pseudoParentPersister);
				}
				
				JoinedTablesPersister<C, I, T> result = new JoinedTablesPersister<C, I, T>(persistenceContext, mainMappingStrategy) {
					private JoinedStrategiesSelectExecutor<C, I, T> defaultJoinedStrategiesSelectExecutor;
					
					/**
					 * Overriden to add subEntities tables but also to prevent a {@link ClassCastException} on {@link #getJoinedStrategiesSelectExecutor()}
					 * which is not {@link JoinedTablesPersister} but a {@link SingleTablePolymorphismSelectExecutor} for this instance
					 * (see {@link #newSelectExecutor(IEntityMappingStrategy, ConnectionProvider, Dialect)})
					 * @return a singleton set of the only table
					 */
					@Override
					public Set<Table> giveImpliedTables() {
						Set<Table> tables = defaultJoinedStrategiesSelectExecutor.getJoinedStrategiesSelect().giveTables();
						persisterPerSubclass.values().forEach(persister -> tables.addAll(persister.giveImpliedTables()));
						return tables;
					}
					
					@Override
					protected InsertExecutor<C, I, T> newInsertExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy,
																		ConnectionProvider connectionProvider,
																		DMLGenerator dmlGenerator, Retryer writeOperationRetryer, int jdbcBatchSize,
																		int inOperatorMaxSize) {
						
						Map<Class<? extends C>, InsertExecutor<C, I, T>> subclassesInsertExecutors =
								Iterables.map(persisterPerSubclass.entrySet(), Entry::getKey, e -> e.getValue().getInsertExecutor());
						
						InsertExecutor<C, I, T> parentClassInsertExecutor = super.newInsertExecutor(mappingStrategy, connectionProvider,
								dmlGenerator,
								writeOperationRetryer, jdbcBatchSize, inOperatorMaxSize);
						
						return new SingleTablePolymorphicInsertExecutor<>(mappingStrategy, connectionProvider, dmlGenerator, writeOperationRetryer,
								jdbcBatchSize, inOperatorMaxSize, subclassesInsertExecutors, parentClassInsertExecutor);
					}
					
					@Override
					protected UpdateExecutor<C, I, T> newUpdateExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy,
																		ConnectionProvider connectionProvider, DMLGenerator dmlGenerator,
																		Retryer writeOperationRetryer, int jdbcBatchSize,
																		int inOperatorMaxSize) {
						Map<Class<? extends C>, UpdateExecutor<C, I, T>> subclassUpdateExecutors =
								Iterables.map(persisterPerSubclass.entrySet(), Entry::getKey, e -> e.getValue().getUpdateExecutor());
						
						PolymorphicUpdateExecutor<C, I, T> a = new PolymorphicUpdateExecutor<>(mappingStrategy,
								connectionProvider, dmlGenerator,
								writeOperationRetryer, jdbcBatchSize, inOperatorMaxSize,
								subclassUpdateExecutors);
						
						Map<Class<? extends C>, UpdateExecutor<C, I, T>> subclassUpdateExecutors2 =
								Iterables.map(persisterPerSubclass2.entrySet(), Entry::getKey, e -> e.getValue().getUpdateExecutor());
						
						return new PolymorphicUpdateExecutor<C, I, T>(mappingStrategy,
								connectionProvider, dmlGenerator,
								writeOperationRetryer, jdbcBatchSize, inOperatorMaxSize,
								subclassUpdateExecutors2) {
							@Override
							public int update(Iterable differencesIterable, boolean allColumnsStatement) {
								// TODO: this count is a real problem because we should return on 1 per modified entity even if it was modified
								// in its upper and lower part
								int i = super.update(differencesIterable, allColumnsStatement);
								i+=a.update(differencesIterable, allColumnsStatement);
								return i;
							}
						};
					}
					
					@Override
					protected ISelectExecutor<C, I> newSelectExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy,
																	  ConnectionProvider connectionProvider,
																	  Dialect dialect) {
						// we create a local persister because we can't call getJoinedStrategiesSelectExecutor().getJoinedStrategiesSelect()
						// because it requires to be a JoinedStrategiesSelectExecutor which is not, but a SingleTablePolymorphismSelectExecutor
						defaultJoinedStrategiesSelectExecutor =
								new JoinedStrategiesSelectExecutor<>(getMappingStrategy(), dialect, getConnectionProvider());
						
						return new JoinedTablesPolymorphismSelectExecutor<>(persisterPerSubclass, persisterPerSubclass2, getMainTable(),
								getConnectionProvider(), dialect, true);
						
					}
					
					@Override
					protected IEntitySelectExecutor<C> newEntitySelectExecutor(Dialect dialect) {
						return new JoinedTablesPolymorphismEntitySelectExecutor<>(persisterPerSubclass, persisterPerSubclass2, getMainTable(),
								defaultJoinedStrategiesSelectExecutor.getJoinedStrategiesSelect(), getConnectionProvider(), dialect);
					}
					
					@Override
					protected DeleteExecutor<C, I, T> newDeleteExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy,
																		ConnectionProvider connectionProvider, DMLGenerator dmlGenerator,
																		Retryer writeOperationRetryer, int jdbcBatchSize, int inOperatorMaxSize) {
						Map<Class<? extends C>, DeleteExecutor<C, I, T>> subclassesDeleteExecutors =
								Iterables.map(persisterPerSubclass.entrySet(), Entry::getKey, e -> e.getValue().getDeleteExecutor());
						
						DeleteExecutor<C, I, T> parentClassDeleteExecutor = super.newDeleteExecutor(mappingStrategy, connectionProvider,
								dmlGenerator,
								writeOperationRetryer, jdbcBatchSize, inOperatorMaxSize);
						
						return new SingleTablePolymorphicDeleteExecutor<>(mappingStrategy, connectionProvider, dmlGenerator, writeOperationRetryer,
								jdbcBatchSize, inOperatorMaxSize, subclassesDeleteExecutors, parentClassDeleteExecutor);
					}
				};
				persistenceContext.addPersister(result);
				return result;
			}
			
		};
		
		
		return polymorphicMappingBuilder.build(persistenceContext, targetTable);
	}
	
	protected <T extends Table<?>> JoinedTablesPersister<C, I, T> buildPolymorphism(
			TablePerClassPolymorphism<C, I> polymorphismPolicy,
			PersistenceContext persistenceContext,
			T targetTable) {
		
		// grouping all mapped properties into a mapping strategy so they can fit into the same table
		List<Linkage> linkages = new ArrayList<>(configurationSupport.getPropertiesMapping().getPropertiesMapping());
		EmbeddableMappingConfiguration allInheritedPropertiesConfiguration = new MethodReferenceDispatcher()
				.redirect(EmbeddableMappingConfiguration<ColumnNamingStrategy>::getPropertiesMapping, () -> linkages)
				.fallbackOn(configurationSupport.getPropertiesMapping())
				.build(EmbeddableMappingConfiguration.class);
		
		// we give parent configuration to child one (because it hasn't it) by assembling a new configuration mainly made of subclass one
		// and inheritance elements by "overriding" properties given by sub configuration
		EntityMappingConfiguration<C, I> subClassEffectiveConfiguration = new MethodReferenceDispatcher()
				// TODO: check if redirecting inheritance wouldn't be better (would allow to get relations too)
				.redirect(EntityMappingConfiguration<C, I>::getPropertiesMapping, () -> allInheritedPropertiesConfiguration)
				.fallbackOn(configurationSupport)
				.build((Class<EntityMappingConfiguration<C, I>>) (Class) EntityMappingConfiguration.class);
		
		Map<SubEntityMappingConfiguration, Table> subclassTables = new HashMap<>();
		for (SubEntityMappingConfiguration<? extends C, I> subConfiguration : polymorphismPolicy.getSubClasses()) {
			Table subclassTable = Nullable.nullable(polymorphismPolicy.giveTable(subConfiguration))
					.getOr(() -> new Table(configurationSupport.getTableNamingStrategy().giveName(subConfiguration.getEntityType())));
			subclassTables.put(subConfiguration, subclassTable);
		}
		
		EntityMappingBuilder<C, I> polymorphicMappingBuilder = new EntityMappingBuilder<C, I>(subClassEffectiveConfiguration, methodSpy) {
			@Override
			protected <T extends Table> JoinedTablesPersister<C, I, T> newJoinedTablesPersister(PersistenceContext persistenceContext,
																								IEntityMappingStrategy<C, I, T> mainMappingStrategy) {
				
				Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> persisterPerSubclass = new HashMap<>();
				
				for (SubEntityMappingConfiguration<? extends C, I> subConfiguration : polymorphismPolicy.getSubClasses()) {
					EntityMappingConfiguration<C, I> subEntityEffectiveConfiguration = composeEntityMappingConfiguration(
							subConfiguration, configurationSupport, subClassEffectiveConfiguration);
					
					EntityMappingBuilder<C, I> subclassMappingBuilder = new EntityMappingBuilder<C, I>(subEntityEffectiveConfiguration, methodSpy) {
						
						@Override
						protected void registerPersister(JoinedTablesPersister persister, PersistenceContext persistenceContext) {
							// does nothing because we don't want subclasses to be part of persistence context, else their persister would be available
							// through PersistenceContext#getPersister(Class) which is not expected from a implicit polymorphism (as opposed to an explicit one)
						}
					};
					JoinedTablesPersister subclassPersister = subclassMappingBuilder.build(persistenceContext, subclassTables.get(subConfiguration));
					persisterPerSubclass.put(subConfiguration.getEntityType(), subclassPersister);
				}
				
				return new JoinedTablesPersister<C, I, T>(persistenceContext, mainMappingStrategy) {
					
					@Override
					public Set<Table> giveImpliedTables() {
						return new HashSet<>(subclassTables.values());
					}
					
					@Override
					protected InsertExecutor<C, I, T> newInsertExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy, ConnectionProvider connectionProvider,
																		DMLGenerator dmlGenerator, Retryer writeOperationRetryer, int jdbcBatchSize,
																		int inOperatorMaxSize) {
						
						Map<Class<? extends C>, InsertExecutor<C, I, T>> subclassInsertExecutors =
								Iterables.map(persisterPerSubclass.entrySet(), Entry::getKey, e -> e.getValue().getInsertExecutor());
						
						return new PolymorphicInsertExecutor<>(mappingStrategy, connectionProvider, dmlGenerator,
								writeOperationRetryer, jdbcBatchSize, inOperatorMaxSize,
								subclassInsertExecutors);
					}
					
					@Override
					protected UpdateExecutor<C, I, T> newUpdateExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy,
																		ConnectionProvider connectionProvider, DMLGenerator dmlGenerator,
																		Retryer writeOperationRetryer, int jdbcBatchSize,
																		int inOperatorMaxSize) {
						Map<Class<? extends C>, UpdateExecutor<C, I, T>> subclassUpdateExecutors =
								Iterables.map(persisterPerSubclass.entrySet(), Entry::getKey, e -> e.getValue().getUpdateExecutor());
						
						return new PolymorphicUpdateExecutor<>(mappingStrategy, connectionProvider, dmlGenerator,
								writeOperationRetryer, jdbcBatchSize, inOperatorMaxSize,
								subclassUpdateExecutors);
					}
					
					@Override
					protected DeleteExecutor<C, I, T> newDeleteExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy,
																		ConnectionProvider connectionProvider, DMLGenerator dmlGenerator,
																		Retryer writeOperationRetryer, int jdbcBatchSize, int inOperatorMaxSize) {
						Map<Class<? extends C>, DeleteExecutor<C, I, T>> subclassesDeleteExecutors =
								Iterables.map(persisterPerSubclass.entrySet(), Entry::getKey, e -> e.getValue().getDeleteExecutor());
						
						return new PolymorphicDeleteExecutor<>(mappingStrategy, connectionProvider, dmlGenerator, writeOperationRetryer,
								jdbcBatchSize, inOperatorMaxSize, subclassesDeleteExecutors);
					}
					
					@Override
					protected ISelectExecutor<C, I> newSelectExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy,
																	  ConnectionProvider connectionProvider,
																	  Dialect dialect) {
						
						return new TablePerClassPolymorphicSelectExecutor<>(subclassTables, persisterPerSubclass, getMainTable(), connectionProvider, dialect.getColumnBinderRegistry(), true);
					}
					
					@Override
					protected IEntitySelectExecutor<C> newEntitySelectExecutor(Dialect dialect) {
						return new TablePerClassPolymorphicEntitySelectExecutor<>(subclassTables, persisterPerSubclass,
								getMainTable(), getConnectionProvider(), dialect.getColumnBinderRegistry(), true);
					}
				};
			}
			
		};
		
		
		return polymorphicMappingBuilder.build(persistenceContext, targetTable);
	}
	
	private static class PolymorphicUpdateExecutor<C, I, T extends Table> extends UpdateExecutor<C, I, T> {
		
		private final Map<Class<? extends C>, UpdateExecutor<C, I, T>> subclassUpdateExecutors;
		
		public PolymorphicUpdateExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy,
										 ConnectionProvider connectionProvider,
										 DMLGenerator dmlGenerator,
										 Retryer writeOperationRetryer,
										 int batchSize,
										 int inOperatorMaxSize,
										 Map<Class<? extends C>, UpdateExecutor<C, I, T>> subclassUpdateExecutors) {
			super(mappingStrategy, connectionProvider, dmlGenerator, writeOperationRetryer, batchSize, inOperatorMaxSize);
			this.subclassUpdateExecutors = subclassUpdateExecutors;
		}
		
		@Override
		public void setRowCountManager(RowCountManager rowCountManager) {
			this.subclassUpdateExecutors.values().forEach(executor -> executor.setRowCountManager(rowCountManager));
		}
		
		@Override
		public void setVersioningStrategy(VersioningStrategy versioningStrategy) {
			// do nothing : subclasses doesn't need to insert version into their table
		}
		
		@Override
		public void setOptimisticLockManager(OptimisticLockManager<T> optimisticLockManager) {
			// do nothing : subclasses doesn't need to insert version into their table
		}
		
		@Override
		public void setOperationListener(SQLOperationListener<UpwhereColumn<T>> listener) {
			this.subclassUpdateExecutors.values().forEach(executor -> executor.setOperationListener(listener));
		}
		
		@Override
		public int update(Iterable<? extends Duo<? extends C, ? extends C>> differencesIterable, boolean allColumnsStatement) {
			Map<Class, Set<Duo<? extends C, ? extends C>>> entitiesPerType = new HashMap<>();
			differencesIterable.forEach(payload -> {
				C entity = Objects.preventNull(payload.getLeft(), payload.getRight());
				entitiesPerType.computeIfAbsent(entity.getClass(), k -> new HashSet<>()).add(payload);
			});
			ModifiableInt updateCount = new ModifiableInt();
			this.subclassUpdateExecutors.forEach((subclass, updateExecutor) -> {
				Set<Duo<? extends C, ? extends C>> entitiesToUpdate = entitiesPerType.get(subclass);
				if (entitiesToUpdate != null) {
					updateCount.increment(updateExecutor.update(entitiesToUpdate, allColumnsStatement));
				}
			});
			
			return updateCount.getValue();
		}
		
	}
	
	private static class PolymorphicInsertExecutor<C, I, T extends Table> extends InsertExecutor<C, I, T> {
		
		private final Map<Class<? extends C>, InsertExecutor<C, I, T>> subclassInsertExecutors;
		
		public PolymorphicInsertExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy,
										 ConnectionProvider connectionProvider,
										 DMLGenerator dmlGenerator,
										 Retryer writeOperationRetryer,
										 int batchSize,
										 int inOperatorMaxSize,
										 Map<Class<? extends C>, InsertExecutor<C, I, T>> subclassInsertExecutors) {
			super(mappingStrategy, connectionProvider, dmlGenerator, writeOperationRetryer, batchSize, inOperatorMaxSize);
			this.subclassInsertExecutors = subclassInsertExecutors;
		}
		
		@Override
		public void setOperationListener(SQLOperationListener<Column<T, Object>> listener) {
			this.subclassInsertExecutors.values().forEach(executor -> executor.setOperationListener(listener));
		}
		
		@Override
		public void setOptimisticLockManager(OptimisticLockManager optimisticLockManager) {
			// do nothing : subclasses doesn't need to insert version into their table
		}
		
		@Override
		public void setVersioningStrategy(VersioningStrategy versioningStrategy) {
			// do nothing : subclasses doesn't need to insert version into their table
		}
		
		@Override
		public int insert(Iterable<? extends C> entities) {
			Map<Class, Set<C>> entitiesPerType = new HashMap<>();
			for (C entity : entities) {
				entitiesPerType.computeIfAbsent(entity.getClass(), cClass -> new HashSet<>()).add(entity);
			}
			ModifiableInt insertCount = new ModifiableInt();
			this.subclassInsertExecutors.forEach((subclass, insertExecutor) -> {
				Set<C> subtypeEntities = entitiesPerType.get(subclass);
				if (subtypeEntities != null) {
					insertCount.increment(insertExecutor.insert(subtypeEntities));
				}
			});
			
			return insertCount.getValue();
		}
	}
	
	private static class PolymorphicDeleteExecutor<C, I, T extends Table> extends DeleteExecutor<C, I, T> {
		
		private final Map<Class<? extends C>, DeleteExecutor<C, I, T>> subclassDeleteExecutors;
		
		public PolymorphicDeleteExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy,
										 ConnectionProvider connectionProvider,
										 DMLGenerator dmlGenerator,
										 Retryer writeOperationRetryer,
										 int batchSize,
										 int inOperatorMaxSize,
										 Map<Class<? extends C>, DeleteExecutor<C, I, T>> subclassDeleteExecutors) {
			super(mappingStrategy, connectionProvider, dmlGenerator, writeOperationRetryer, batchSize, inOperatorMaxSize);
			this.subclassDeleteExecutors = subclassDeleteExecutors;
		}
		
		@Override
		public void setOperationListener(SQLOperationListener<Column<T, Object>> listener) {
			this.subclassDeleteExecutors.values().forEach(executor -> executor.setOperationListener(listener));
		}
		
		@Override
		public int delete(Iterable<C> entities) {
			Map<Class, Set<C>> entitiesPerType = new HashMap<>();
			for (C entity : entities) {
				entitiesPerType.computeIfAbsent(entity.getClass(), cClass -> new HashSet<>()).add(entity);
			}
			ModifiableInt insertCount = new ModifiableInt();
			this.subclassDeleteExecutors.forEach((subclass, insertExecutor) -> {
				Set<C> subtypeEntities = entitiesPerType.get(subclass);
				if (subtypeEntities != null) {
					insertCount.increment(insertExecutor.delete(subtypeEntities));
				}
			});
			
			return insertCount.getValue();
		}
		
	}
	
	private static class SingleTablePolymorphicInsertExecutor<C, I, T extends Table> extends PolymorphicInsertExecutor<C, I, T> {
		private final InsertExecutor<C, I, T> mainInsertExecutor;
		
		public SingleTablePolymorphicInsertExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy, ConnectionProvider connectionProvider,
													DMLGenerator dmlGenerator, Retryer writeOperationRetryer, int jdbcBatchSize, int inOperatorMaxSize,
													Map<Class<? extends C>, InsertExecutor<C, I, T>> subclassesInsertExecutors,
													InsertExecutor<C, I, T> parentClassInsertExecutor) {
			super(mappingStrategy, connectionProvider, dmlGenerator, writeOperationRetryer, jdbcBatchSize, inOperatorMaxSize, subclassesInsertExecutors);
			this.mainInsertExecutor = parentClassInsertExecutor;
		}
		
		@Override
		public int insert(Iterable<? extends C> entities) {
			// first insert parent then subentities to comply with integrity constraint 
			mainInsertExecutor.insert(entities);
			// NB: row count shall be the same between main inserts and subentities one
			return super.insert(entities);
		}
	}
	
	
	private static class SingleTablePolymorphicDeleteExecutor<C, I, T extends Table> extends PolymorphicDeleteExecutor<C, I, T> {
		private final DeleteExecutor<C, I, T> mainDeleteExecutor;
		
		public SingleTablePolymorphicDeleteExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy, ConnectionProvider connectionProvider,
													DMLGenerator dmlGenerator, Retryer writeOperationRetryer, int batchSize, int inOperatorMaxSize,
													Map<Class<? extends C>, DeleteExecutor<C, I, T>> subclassDeleteExecutors,
													DeleteExecutor<C, I, T> parentClassInsertExecutor) {
			super(mappingStrategy, connectionProvider, dmlGenerator, writeOperationRetryer, batchSize, inOperatorMaxSize, subclassDeleteExecutors);
			this.mainDeleteExecutor = parentClassInsertExecutor;
		}
		
		@Override
		public int delete(Iterable<C> entities) {
			// first subentities then insert parent to comply with integrity constraint 
			super.delete(entities);
			// NB: row count shall be the same between main inserts and subentities one
			return mainDeleteExecutor.delete(entities);
		}
	}
	
	/**
	 * Composes an {@link EntityMappingConfiguration} from a {@link SubEntityMappingConfiguration} making it looks like a real {@link EntityMappingConfiguration}.
	 * Missing informations (such as identification, inheritance, etc) are taken from the given {@link EntityMappingConfiguration} which is expected
	 * to be the parent configuration of the sub-entity one.
	 * 
	 * @param subEntityConfiguration the sub-entity configuration to be completed as an entity one
	 * @param entityConfiguration the configuration that contains missing informations to the sub-entity configuration
	 * @param entityInheritedConfiguration may be null, necessary to be given when table contains all properties (single-table or table-per-class)
	 * @param <C> entity type
	 * @param <I> identifier type
	 * @return a new {@link EntityMappingConfiguration} that is a composition of sub-entity informations and entity ones
	 */
	private static <C, I> EntityMappingConfiguration<C, I> composeEntityMappingConfiguration(SubEntityMappingConfiguration<? extends C, I> subEntityConfiguration,
																							 EntityMappingConfiguration<C, I> entityConfiguration,
																							 EntityMappingConfiguration<C, I> entityInheritedConfiguration) {
		return new MethodReferenceDispatcher()
				.redirect(EntityMappingConfiguration<C, I>::getTableNamingStrategy, () -> entityConfiguration.getTableNamingStrategy())
				.redirect(EntityMappingConfiguration<C, I>::getIdentifierPolicy, () -> entityConfiguration.getIdentifierPolicy())
				.redirect(EntityMappingConfiguration<C, I>::getIdentifierAccessor, () -> entityConfiguration.getIdentifierAccessor())
				.redirect(EntityMappingConfiguration<C, I>::getOptimisticLockOption, () -> entityConfiguration.getOptimisticLockOption())
				// Adding parent properties to subclass mapping, mainly for select purpose, because it is done by subclass persister
				// whereas insert / update / delete are done by their own executor
				.redirect(EntityMappingConfiguration<C, I>::getInheritanceConfiguration, () -> entityInheritedConfiguration == null ? null : new InheritanceConfiguration() {
					@Override
					public EntityMappingConfiguration getConfiguration() {
						return entityInheritedConfiguration;
					}
					
					@Override
					public boolean isJoinTable() {
						return false;
					}
					
					@Override
					public Table getTable() {
						return null;
					}
				})
				.redirect(EntityMappingConfiguration<C, I>::getForeignKeyNamingStrategy, () -> entityConfiguration.getForeignKeyNamingStrategy())
				.redirect(EntityMappingConfiguration<C, I>::getAssociationTableNamingStrategy, () -> entityConfiguration.getAssociationTableNamingStrategy())
				.redirect(EntityMappingConfiguration<C, I>::getJoinColumnNamingStrategy, () -> entityConfiguration.getJoinColumnNamingStrategy())
				.redirect(EntityMappingConfiguration<C, I>::getPolymorphismPolicy, () -> null)	// throw new IllegalStateException() ?
				//java.lang.IllegalArgumentException: Wrong given instance while invoking o.g.s.p.e.EntityMappingConfiguration.getPropertiesMapping(): expected org.gama.stalactite.persistence.engine.EntityMappingConfiguration but Dispatcher to org.gama.stalactite.persistence.engine.FluentSubEntityMappingConfigurationSupport@4516af24 was given
				.redirect(EntityMappingConfiguration<C, I>::getPropertiesMapping, () -> subEntityConfiguration.getPropertiesMapping())
				.redirect(EntityMappingConfiguration<C, I>::getEntityType, () -> subEntityConfiguration.getEntityType())
				.redirect(EntityMappingConfiguration<C, I>::getEntityFactory, () -> subEntityConfiguration.getEntityFactory())
				.redirect(EntityMappingConfiguration<C, I>::getOneToOnes, () -> subEntityConfiguration.getOneToOnes())
				.redirect(EntityMappingConfiguration<C, I>::getOneToManys, () -> subEntityConfiguration.getOneToManys())
				.redirect(EntityMappingConfiguration<C, I>::inheritanceIterable, () -> Arrays.asSet(entityConfiguration))
				.fallbackOn(subEntityConfiguration)
				.build((Class<EntityMappingConfiguration<C, I>>) (Class) EntityMappingConfiguration.class);
	}
}
