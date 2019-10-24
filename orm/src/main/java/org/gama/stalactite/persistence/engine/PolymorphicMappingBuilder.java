package org.gama.stalactite.persistence.engine;

import java.sql.ResultSet;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import org.gama.lang.Duo;
import org.gama.lang.Reflections;
import org.gama.lang.Retryer;
import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.lang.trace.ModifiableInt;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.reflection.MethodReferenceDispatcher;
import org.gama.stalactite.persistence.engine.EmbeddableMappingConfiguration.Linkage;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.JoinedTablesPolymorphism;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.TablePerClassPolymorphism;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect.StrategyJoins.Join;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelectExecutor;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.mapping.IMappingStrategy.UpwhereColumn;
import org.gama.stalactite.persistence.query.EntitySelectExecutor;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.dml.DMLGenerator;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.builder.SQLQueryBuilder;
import org.gama.stalactite.query.model.CriteriaChain;
import org.gama.stalactite.query.model.Operators;
import org.gama.stalactite.query.model.Query;
import org.gama.stalactite.query.model.QueryEase;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.SimpleConnectionProvider;
import org.gama.stalactite.sql.binder.ResultSetReader;
import org.gama.stalactite.sql.dml.PreparedSQL;
import org.gama.stalactite.sql.dml.ReadOperation;
import org.gama.stalactite.sql.dml.SQLExecutionException;
import org.gama.stalactite.sql.dml.SQLOperation.SQLOperationListener;
import org.gama.stalactite.sql.result.RowIterator;

/**
 * @author Guillaume Mary
 */
public class PolymorphicMappingBuilder<C, I> extends AbstractEntityMappingBuilder<C, I> {
	
	public PolymorphicMappingBuilder(EntityMappingConfiguration<C, I> entityMappingConfiguration, MethodReferenceCapturer methodSpy) {
		super(entityMappingConfiguration, methodSpy);
	}
	
	@Override
	protected <T extends Table<?>> JoinedTablesPersister<C, I, T> doBuild(PersistenceContext persistenceContext, T targetTable) {
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
		for (EntityMappingConfigurationProvider<? extends C, I> subClass : polymorphismPolicy.getSubClasses()) {
			// TODO: shall be removed when subclasses configurations will be embeddable ones, not entity ones (because we don't need identification to be redefined)
			linkages.removeIf(linkage -> linkage.getAccessor() == subClass.getConfiguration().getIdentifierAccessor());
		}
		
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
				.redirect(EntityMappingConfiguration<C, I>::getEntityFactory, () -> row -> {
					// type to instanciate is determined by discriminator
					D dtype = (D) row.apply(discriminatorColumn);
					return Reflections.newInstance(polymorphismPolicy.getClass(dtype));
				})
				.fallbackOn(configurationSupport)
				.build((Class<EntityMappingConfiguration<C, I>>) (Class) EntityMappingConfiguration.class);
		
		
		EntityMappingBuilder<C, I> polymorphicMappingBuilder = new EntityMappingBuilder<C, I>(subClassEffectiveConfiguration, methodSpy) {
			@Override
			protected <T extends Table> JoinedTablesPersister<C, I, T> newJoinedTablesPersister(PersistenceContext persistenceContext,
																								IEntityMappingStrategy<C, I, T> mainMappingStrategy) {
				
				Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> persisterPerSubclass = new HashMap<>();
				mainMappingStrategy.addSilentColumnToSelect(discriminatorColumn);
				
				for (EntityMappingConfigurationProvider<? extends C, I> subConfiguration : polymorphismPolicy.getSubClasses()) {
					EntityMappingConfiguration<C, I> subEntityConfiguration = (EntityMappingConfiguration<C, I>) subConfiguration.getConfiguration();
					
					
					// Adding parent properties to subclass mapping, mainly for select purpose, because it is done by subclass persister
					// whereas insert / update / delete are done by their own executor 
					EntityMappingConfiguration<C, I> subEntityEffectiveConfiguration = new MethodReferenceDispatcher()
							.redirect(EntityMappingConfiguration<C, I>::getInheritanceConfiguration, () -> subClassEffectiveConfiguration)
							.fallbackOn(subEntityConfiguration)
							.build((Class<EntityMappingConfiguration<C, I>>) (Class) EntityMappingConfiguration.class);
					
					
					EntityMappingBuilder<C, I> subclassMappingBuilder = new EntityMappingBuilder<C, I>(subEntityEffectiveConfiguration, methodSpy) {
						
						@Override
						protected void registerPersister(JoinedTablesPersister persister, PersistenceContext persistenceContext) {
							// does nothing because we don't want subclasses to be part of persistence context, else their persister would be available
							// through PersistenceContext#getPersister(Class) which is not expected from a implicit polymorphism (as opposed to an explicit one)
						}
					};
					JoinedTablesPersister subclassPersister = subclassMappingBuilder.build(persistenceContext, targetTable);
					persisterPerSubclass.put(subConfiguration.getConfiguration().getEntityType(), subclassPersister);
				}
				
				return new JoinedTablesPersister<C, I, T>(persistenceContext, mainMappingStrategy) {
					private JoinedStrategiesSelectExecutor<C, I, T> originalJoinedStrategiesSelectExecutor;
					
					/* No need to override delete executor instanciation because all subclasses are stored in the same table than the one
					 * given to constructor, and since there's no need to distinguish subclass instances (no field computation), they'll deleted correctly
					 */
					
					@Override
					protected InsertExecutor<C, I, T> newInsertExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy, ConnectionProvider connectionProvider,
															   DMLGenerator dmlGenerator, Retryer writeOperationRetryer, int jdbcBatchSize,
															   int inOperatorMaxSize) {
						
						Map<Class<? extends C>, InsertExecutor<C, I, T>> subclassInsertExecutors =
								Iterables.map(persisterPerSubclass.entrySet(), Entry::getKey, e -> e.getValue().getInsertExecutor());
						
						persisterPerSubclass.values().forEach(subclassPersister -> 
								subclassPersister.getMappingStrategy().addSilentColumnInserter(discriminatorColumn, c -> polymorphismPolicy.getDiscriminatorValue((Class<? extends C>) c.getClass()))
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
					protected JoinedStrategiesSelectExecutor<C, I, T> newSelectExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy,
																		ConnectionProvider connectionProvider,
																		Dialect dialect) {
						
						// necessary to be able to invoke getJoinedStrategiesSelect() below
						originalJoinedStrategiesSelectExecutor = super.newSelectExecutor(mappingStrategy, connectionProvider, dialect);
						
						return new JoinedStrategiesSelectExecutor<C, I, T>(mappingStrategy, dialect, connectionProvider) {
							@Override
							public List<C> select(Iterable<I> ids) {
								// Doing this in 2 phases
								// - make a select with id + discriminator in select clause and ids in where to determine ids per subclass type
								// - call the right subclass joinExecutor with dedicated ids
								// TODO : (with which listener ?)
								
								// We ensure that the same Connection is used for all operations
								ConnectionProvider localConnectionProvider = new SimpleConnectionProvider(getConnectionProvider().getCurrentConnection());
								
								
								
								Column<T, I> primaryKey = (Column<T, I>) Iterables.first(getMainTable().getPrimaryKey().getColumns());
								Set<Column<T, ?>> columns = new HashSet<>(getMainTable().getPrimaryKey().getColumns());
								columns.add(discriminatorColumn);
								SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder(QueryEase.
										select(primaryKey, discriminatorColumn)
										.from(targetTable)
										.where(primaryKey, Operators.in(ids)));
								PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL(dialect.getColumnBinderRegistry());
								Map<Class, Set<I>> idsPerSubclass = new HashMap<>();
								try(ReadOperation readOperation = new ReadOperation<>(preparedSQL, localConnectionProvider)) {
									ResultSet resultSet = readOperation.execute();
									Map<String, ResultSetReader> aliases = new HashMap<>();
									columns.forEach(c -> aliases.put(c.getName(), dialect.getColumnBinderRegistry().getBinder(c)));
									
									RowIterator resultSetIterator = new RowIterator(resultSet, aliases);
									ColumnedRow columnedRow = new ColumnedRow(Column::getName);
									resultSetIterator.forEachRemaining(row -> {
										D dtype = (D) columnedRow.getValue(discriminatorColumn, row);
										Class<? extends C> entitySubclass = polymorphismPolicy.getClass(dtype);
										idsPerSubclass.computeIfAbsent(entitySubclass, k -> new HashSet<>())
												.add((I) columnedRow.getValue(primaryKey, row));
									});
								}
								
								List<C> result = new ArrayList<>();
								idsPerSubclass.forEach((subclass, subclassIds) -> result.addAll(persisterPerSubclass.get(subclass).select(subclassIds)));
								
								return result;
							}
						};
					}
					
					@Override
					protected EntitySelectExecutor<C, I, T> newEntitySelectExecutor(JoinedStrategiesSelectExecutor<C, I, T> joinedStrategiesSelectExecutor, ConnectionProvider connectionProvider, Dialect dialect) {
						// we get all joins of originally-computed JoinedStrategiesSelectExecutor (built by super.newSelectExecutor(..)))
						// and pass them to our dispatching one (passed as argument)
						String currentJoinName = JoinedStrategiesSelect.FIRST_STRATEGY_NAME;
						Queue<Join> joins = new ArrayDeque<>();
						joins.addAll(originalJoinedStrategiesSelectExecutor.getJoinedStrategiesSelect().getJoinsRoot().getJoins());
						while(!joins.isEmpty()) {
							Join join = joins.poll();
							// TODO: finish tree traversal (in depth)
							joinedStrategiesSelectExecutor.addComplementaryTable(currentJoinName, join.getStrategy().getStrategy(),
									join.getBeanRelationFixer(), join.getLeftJoinColumn(), join.getRightJoinColumn(), join.isOuter());
						}
						
						return new EntitySelectExecutor<C, I, T>(joinedStrategiesSelectExecutor.getJoinedStrategiesSelect(), connectionProvider,
								dialect.getColumnBinderRegistry(), joinedStrategiesSelectExecutor.getStrategyJoinsRowTransformer()) {
							
							private static final String DISCRIMINATOR_ALIAS = "DISCRIMINATOR";
							private static final String PRIMARY_KEY_ALIAS = "PK";
							
							@Override
							public List<C> loadGraph(CriteriaChain where) {
								JoinedStrategiesSelect<C, I, T> joinedStrategiesSelect = joinedStrategiesSelectExecutor.getJoinedStrategiesSelect();
								Query query = joinedStrategiesSelect.buildSelectQuery();
								
								SQLQueryBuilder sqlQueryBuilder = createQueryBuilder(where, query);
								
								// First phase : selecting ids (made by clearing selected elements for performance issue)
								Column<T, I> pk = (Column<T, I>) Iterables.first(joinedStrategiesSelect.getJoinsRoot().getTable().getPrimaryKey().getColumns());
								query.select(pk, PRIMARY_KEY_ALIAS);
								query.select(discriminatorColumn, DISCRIMINATOR_ALIAS);
								List<Duo<I, D>> ids = readIds(sqlQueryBuilder, pk);
								
								Map<Class, Set<I>> xx = new HashMap<>();
								ids.forEach(id -> xx.computeIfAbsent(polymorphismPolicy.getClass(id.getRight()), k -> new HashSet<>()).add(id.getLeft()));
								
								List<C> result = new ArrayList<>(); 
								
								xx.forEach((k, v) -> result.addAll(persisterPerSubclass.get(k).select(v)));
								return result;
							}
							
							private List<Duo<I, D>> readIds(SQLQueryBuilder sqlQueryBuilder, Column<T, I> pk) {
								PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL(dialect.getColumnBinderRegistry());
								try (ReadOperation<Integer> closeableOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
									ResultSet resultSet = closeableOperation.execute();
									RowIterator rowIterator = new RowIterator(resultSet,
											Maps.asMap(PRIMARY_KEY_ALIAS, dialect.getColumnBinderRegistry().getBinder(pk))
											.add(DISCRIMINATOR_ALIAS, dialect.getColumnBinderRegistry().getBinder(discriminatorColumn)));
									return Iterables.collectToList(() -> rowIterator, row -> new Duo<>((I) row.get(PRIMARY_KEY_ALIAS), (D) row.get(DISCRIMINATOR_ALIAS)));
								} catch (RuntimeException e) {
									throw new SQLExecutionException(preparedSQL.getSQL(), e);
								}
							}
						};
					}
				};
			}
			
		};
		
		
		return polymorphicMappingBuilder.build(persistenceContext, targetTable);
	}
	
	
	protected <T extends Table<?>> JoinedTablesPersister<C, I, T> buildPolymorphism(
			JoinedTablesPolymorphism<C, I> polymorphismPolicy,
			PersistenceContext persistenceContext,
			T targetTable) {
		Collection<EntityMappingConfigurationProvider<? extends C, I>> subClasses = polymorphismPolicy.getSubClasses();
		return null;
	}
	
	protected <T extends Table<?>> JoinedTablesPersister<C, I, T> buildPolymorphism(
			TablePerClassPolymorphism<C, I> polymorphismPolicy,
			PersistenceContext persistenceContext,
			T targetTable) {
		Collection<EntityMappingConfigurationProvider<? extends C, I>> subClasses = polymorphismPolicy.getSubClasses();
		return null;
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
			this.subclassUpdateExecutors.values().forEach(executor -> executor.setVersioningStrategy(versioningStrategy));
		}
		
		@Override
		public void setOptimisticLockManager(OptimisticLockManager<T> optimisticLockManager) {
			this.subclassUpdateExecutors.values().forEach(executor -> executor.setOptimisticLockManager(optimisticLockManager));
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
			this.subclassInsertExecutors.values().forEach(executor -> executor.setOptimisticLockManager(optimisticLockManager));
		}
		
		@Override
		public void setVersioningStrategy(VersioningStrategy versioningStrategy) {
			this.subclassInsertExecutors.values().forEach(executor -> executor.setVersioningStrategy(versioningStrategy));
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
}
