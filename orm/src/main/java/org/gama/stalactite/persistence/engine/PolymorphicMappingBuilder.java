package org.gama.stalactite.persistence.engine;

import java.sql.ResultSet;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;

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
import org.gama.stalactite.persistence.engine.cascade.AbstractJoin;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
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
import org.gama.stalactite.query.model.Select.AliasedColumn;
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
								
								Column<T, I> primaryKey = (Column<T, I>) Iterables.first(getMainTable().getPrimaryKey().getColumns());
								Set<Column<T, ?>> columns = new HashSet<>(getMainTable().getPrimaryKey().getColumns());
								columns.add(discriminatorColumn);
								SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder(QueryEase.
										select(primaryKey, discriminatorColumn)
										.from(targetTable)
										.where(primaryKey, Operators.in(ids)));
								PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL(dialect.getColumnBinderRegistry());
								Map<Class, Set<I>> idsPerSubclass = new HashMap<>();
								try(ReadOperation readOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
									ResultSet resultSet = readOperation.execute();
									Map<String, ResultSetReader> aliases = new HashMap<>();
									columns.forEach(c -> aliases.put(c.getName(), dialect.getColumnBinderRegistry().getBinder(c)));
									
									RowIterator resultSetIterator = new RowIterator(resultSet, aliases);
									ColumnedRow columnedRow = new ColumnedRow(Column::getName);
									resultSetIterator.forEachRemaining(row -> {
										D dtype = (D) columnedRow.getValue(discriminatorColumn, row);
										Class<? extends C> entitySubclass = polymorphismPolicy.getClass(dtype);
										// adding identifier to subclass' ids
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
						Queue<AbstractJoin> joins = new ArrayDeque<>();
						joins.addAll(originalJoinedStrategiesSelectExecutor.getJoinedStrategiesSelect().getJoinsRoot().getJoins());
						while(!joins.isEmpty()) {
							AbstractJoin join = joins.poll();
							// TODO: finish tree traversal (in depth)
							joinedStrategiesSelectExecutor.getJoinedStrategiesSelect().addJoin(currentJoinName, join.getStrategy().getStrategy(), s -> join);
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
								
								// selecting ids and their discriminator
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
	
	
	protected <T extends Table> JoinedTablesPersister<C, I, T> buildPolymorphism(
			JoinedTablesPolymorphism<C, I> polymorphismPolicy,
			PersistenceContext persistenceContext,
			T targetTable) {
		
		// grouping all mapped properties into a mapping strategy so they can fit into the same table
		List<Linkage> linkages = new ArrayList<>(configurationSupport.getPropertiesMapping().getPropertiesMapping());
		for (EntityMappingConfigurationProvider<? extends C, I> subClass : polymorphismPolicy.getSubClasses()) {
			// TODO: shall be removed when subclasses configurations will be embeddable ones, not entity ones (because we don't need identification to be redefined)
			linkages.removeIf(linkage -> linkage.getAccessor() == subClass.getConfiguration().getIdentifierAccessor());
		}

		EntityMappingBuilder<C, I> polymorphicMappingBuilder = new EntityMappingBuilder<C, I>(configurationSupport, methodSpy) {
			@Override
			protected <T extends Table> JoinedTablesPersister<C, I, T> newJoinedTablesPersister(PersistenceContext persistenceContext,
																								IEntityMappingStrategy<C, I, T> mainMappingStrategy) {
				
				
				
				// subclass persister : contains only subclass properties
				Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> persisterPerSubclass = new HashMap<>();
				
				for (EntityMappingConfigurationProvider<? extends C, I> subConfiguration : polymorphismPolicy.getSubClasses()) {
					EntityMappingConfiguration<C, I> subEntityConfiguration = (EntityMappingConfiguration<C, I>) subConfiguration.getConfiguration();
					
					EntityMappingBuilder<C, I> subclassMappingBuilder = new EntityMappingBuilder<C, I>(subEntityConfiguration, methodSpy) {

						@Override
						protected void registerPersister(JoinedTablesPersister persister, PersistenceContext persistenceContext) {
							// does nothing because we don't want subclasses to be part of persistence context, else their persister would be available
							// through PersistenceContext#getPersister(Class) which is not expected from a implicit polymorphism (as opposed to an explicit one)
						}
					};
					Table subEntityTargetTable = new Table(configurationSupport.getTableNamingStrategy().giveName(subEntityConfiguration.getEntityType()));
					JoinedTablesPersister subclassPersister = subclassMappingBuilder.build(persistenceContext, subEntityTargetTable);
					
					persisterPerSubclass.put(subConfiguration.getConfiguration().getEntityType(), subclassPersister);
					Column subEntityPrimaryKey = (Column) Iterables.first(subEntityTargetTable.getPrimaryKey().getColumns());
					Column entityPrimaryKey = (Column) Iterables.first(targetTable.getPrimaryKey().getColumns());
					subEntityTargetTable.addForeignKey(configurationSupport.getForeignKeyNamingStrategy().giveName(subEntityPrimaryKey, entityPrimaryKey),
							new ArrayList<>(subEntityTargetTable.getPrimaryKey().getColumns()), new ArrayList<>(targetTable.getPrimaryKey().getColumns()));
				}
				
				// subclass persister 2 : contains subclass properties only
				Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> persisterPerSubclass2 = new HashMap<>();
				
				for (EntityMappingConfigurationProvider<? extends C, I> subConfiguration : polymorphismPolicy.getSubClasses()) {
					final EntityMappingConfiguration<? extends C, I> subEntityEffectiveConfiguration = subConfiguration.getConfiguration();
					
					EntityMappingConfiguration<C, I> parentEffectiveConfiguration = new MethodReferenceDispatcher()
							.redirect(EntityMappingConfiguration<C, I>::getPropertiesMapping, () -> {
								// overriden to fix bean type to prevent to be parent class because it is unecessary and conflicts with
								// uppest parent join persister that will be registered further
								// also done to instantiate rigth type : subentity one
								return new MethodReferenceDispatcher()
										.redirect(EmbeddableMappingConfiguration<C>::getBeanType, () -> subEntityEffectiveConfiguration.getEntityType())
										.fallbackOn(configurationSupport.getPropertiesMapping())
										.build((Class<EmbeddableMappingConfiguration<C>>) (Class) EmbeddableMappingConfiguration.class);
							})
							.redirect(EntityMappingConfiguration<C, I>::getEntityFactory, () -> (Function<Column, Object> f) -> Reflections.newInstance(subEntityEffectiveConfiguration.getEntityType()))
							.fallbackOn(configurationSupport)
							.build((Class<EntityMappingConfiguration<C, I>>) (Class) EntityMappingConfiguration.class);
					
					
					T subentityTargetTable =
							persisterPerSubclass.get(subEntityEffectiveConfiguration.getEntityType()).getMappingStrategy().getTargetTable();
					
					EntityMappingBuilder<C, I> joinedTablesEntityMappingBuilder = new EntityMappingBuilder<C, I>(parentEffectiveConfiguration, methodSpy) {
						@Override
						protected void registerPersister(JoinedTablesPersister persister, PersistenceContext persistenceContext) {
							// does nothing because we don't want subclasses to be part of persistence context, else their persister would be available
							// through PersistenceContext#getPersister(Class) which is not expected from a implicit polymorphism (as opposed to an explicit one)
						}
					};
					JoinedTablesPersister<C, I, T> pseudoParentPersister = (JoinedTablesPersister<C, I, T>) joinedTablesEntityMappingBuilder.doBuild(persistenceContext, targetTable);
					
					
					EntityMappingBuilder<C, I> subEntityJoinedTablesEntityMappingBuilder = new EntityMappingBuilder<C, I>(
							(EntityMappingConfiguration<C, I>) subEntityEffectiveConfiguration, methodSpy) {
						
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
					String joinName =
							pseudoParentPersister.getJoinedStrategiesSelectExecutor().addComplementaryJoin(JoinedStrategiesSelect.FIRST_STRATEGY_NAME,
									subEntityJoinedTablesPersister.getMappingStrategy(), entityPrimaryKey, subEntityPrimaryKey);
					
					persisterPerSubclass2.put(subEntityEffectiveConfiguration.getEntityType(), pseudoParentPersister);
				}
				
				JoinedTablesPersister<C, I, T> result = new JoinedTablesPersister<C, I, T>(persistenceContext, mainMappingStrategy) {
					private JoinedStrategiesSelectExecutor<C, I, T> originalJoinedStrategiesSelectExecutor;
					
					@Override
					public Set<Table> giveImpliedTables() {
						Set<Table> tables = super.giveImpliedTables();
						persisterPerSubclass.values().forEach(persister -> tables.addAll(persister.giveImpliedTables()));
						return tables;
					}
					
					/* No need to override delete executor instanciation because all subclasses are stored in the same table than the one
					 * given to constructor, and since there's no need to distinguish subclass instances (no field computation), they'll deleted 
					 * correctly
					 */
					
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
					protected JoinedStrategiesSelectExecutor<C, I, T> newSelectExecutor(IEntityMappingStrategy<C, I, T> mappingStrategy,
																						ConnectionProvider connectionProvider,
																						Dialect dialect) {
						
						
						// necessary to be able to invoke getJoinedStrategiesSelect() below
						originalJoinedStrategiesSelectExecutor = super.newSelectExecutor(mappingStrategy, connectionProvider, dialect);
						
						return new JoinedStrategiesSelectExecutor<C, I, T>(mappingStrategy, dialect, connectionProvider) {
							@Override
							public List<C> select(Iterable<I> ids) {
								// 2 possibilities :
								// - execute a request that join all tables and all relations, then give result to transfomer
								//   Pros : one request, simple approach
								//   Cons : one eventually big/complex request, has some drawback on how to create this request (impacts on parent
								//          Persister behavior) and how to build the transformer. In conclusion quite complex
								// - do it in 2+ phases : one request to determine which id matches which type, then ask each sub classes to load
								//   their own type
								//   Pros : suclasses must know common properties/trunk which will be necessary for updates too (to compute 
								//   differences)
								//   Cons : first request not so easy to write. Performance may be lower because of 1+N (one per subclass) database 
								//   requests
								// => option 2 choosen. May be reviewed later, or make this policy configurable.
								
								// Doing this in 2 phases
								// - make a select with id + discriminator in select clause and ids in where to determine ids per subclass type
								// - call the right subclass joinExecutor with dedicated ids
								// TODO : (with which listener ?)
								
								// We ensure that the same Connection is used for all operations
								ConnectionProvider localConnectionProvider =
										new SimpleConnectionProvider(getConnectionProvider().getCurrentConnection());
								
								
								Column<T, I> primaryKey = (Column<T, I>) Iterables.first(getMainTable().getPrimaryKey().getColumns());
								Set<Column<T, ?>> columns = new HashSet<>(getMainTable().getPrimaryKey().getColumns());
								Query query = QueryEase.
										select(primaryKey, primaryKey.getAlias())
										.from(targetTable)
										.where(primaryKey, Operators.in(ids)).getSelectQuery();
								persisterPerSubclass.values().forEach(subclassPersister -> {
									Column subclassPrimaryKey = Iterables.first(
											(Set<Column>) subclassPersister.getMainTable().getPrimaryKey().getColumns());
									query.select(subclassPrimaryKey, subclassPrimaryKey.getAlias());
									query.getFrom().leftOuterJoin(primaryKey, subclassPrimaryKey);
								});
								SQLQueryBuilder sqlQueryBuilder = new SQLQueryBuilder(query);
								PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL(dialect.getColumnBinderRegistry());
								Map<Class, Set<I>> idsPerSubclass = new HashMap<>();
								try (ReadOperation readOperation = new ReadOperation<>(preparedSQL, localConnectionProvider)) {
									ResultSet resultSet = readOperation.execute();
									Map<String, ResultSetReader> aliases = new HashMap<>();
									Iterables.stream(query.getSelectSurrogate())
											.map(AliasedColumn.class::cast).map(AliasedColumn::getColumn)
											.forEach(c -> aliases.put(c.getAlias(), dialect.getColumnBinderRegistry().getBinder(c)));
									
									RowIterator resultSetIterator = new RowIterator(resultSet, aliases);
									ColumnedRow columnedRow = new ColumnedRow(Column::getAlias);
									resultSetIterator.forEachRemaining(row -> {
										
										// looking for entity type on row : we read each subclass PK and check for nullity. The non-null one is the 
										// right one
										Class<? extends C> entitySubclass;
										Set<Entry<Class<? extends C>, JoinedTablesPersister<C, I, T>>> entries = persisterPerSubclass.entrySet();
										Entry<Class<? extends C>, JoinedTablesPersister<C, I, T>> subclassEntityOnRow = Iterables.find(entries,
												e -> {
											boolean isPKEmpty = true;
											Iterator<Column> columnIt = e.getValue().getMainTable().getPrimaryKey().getColumns().iterator();
											while (isPKEmpty && columnIt.hasNext()) {
												Column column = columnIt.next();
												isPKEmpty = columnedRow.getValue(column, row) != null;
											}
											return isPKEmpty;
										});
										entitySubclass = subclassEntityOnRow.getKey();
										
										// adding identifier to subclass' ids
										idsPerSubclass.computeIfAbsent(entitySubclass, k -> new HashSet<>())
												.add((I) columnedRow.getValue(primaryKey, row));
									});
								}
								
								List<C> result = new ArrayList<>();
								idsPerSubclass.forEach((subclass, subclassIds) -> result.addAll(persisterPerSubclass2.get(subclass).select(subclassIds)));
								
								return result;
							}
						};
					}
					
					@Override
					protected EntitySelectExecutor<C, I, T> newEntitySelectExecutor(JoinedStrategiesSelectExecutor<C, I, T> joinedStrategiesSelectExecutor, ConnectionProvider connectionProvider, Dialect dialect) {
						// we get all joins of originally-computed JoinedStrategiesSelectExecutor (built by super.newSelectExecutor(..)))
						// and pass them to our dispatching one (passed as argument)
						String currentJoinName = JoinedStrategiesSelect.FIRST_STRATEGY_NAME;
						Queue<AbstractJoin> joins = new ArrayDeque<>();
						joins.addAll(originalJoinedStrategiesSelectExecutor.getJoinedStrategiesSelect().getJoinsRoot().getJoins());
						while (!joins.isEmpty()) {
							AbstractJoin join = joins.poll();
							// TODO: finish tree traversal (in depth)
							joinedStrategiesSelectExecutor.getJoinedStrategiesSelect().addJoin(currentJoinName, join.getStrategy().getStrategy(), s -> join);
						}
						
						return new EntitySelectExecutor<C, I, T>(joinedStrategiesSelectExecutor.getJoinedStrategiesSelect(), connectionProvider,
								dialect.getColumnBinderRegistry(), joinedStrategiesSelectExecutor.getStrategyJoinsRowTransformer()) {
							
							@Override
							public List<C> loadGraph(CriteriaChain where) {
								JoinedStrategiesSelect<C, I, T> joinedStrategiesSelect = joinedStrategiesSelectExecutor.getJoinedStrategiesSelect();
								Query query = joinedStrategiesSelect.buildSelectQuery();
								
								Column<T, I> primaryKey = (Column<T, I>) Iterables.first(getMainTable().getPrimaryKey().getColumns());
								Set<Column<T, ?>> columns = new HashSet<>(getMainTable().getPrimaryKey().getColumns());
								persisterPerSubclass.values().forEach(subclassPersister -> {
									Column subclassPrimaryKey = Iterables.first(
											(Set<Column>) subclassPersister.getMainTable().getPrimaryKey().getColumns());
									query.select(subclassPrimaryKey, subclassPrimaryKey.getAlias());
									query.getFrom().leftOuterJoin(primaryKey, subclassPrimaryKey);
								});
								
								SQLQueryBuilder sqlQueryBuilder = createQueryBuilder(where, query);
								
								// selecting ids and their entity type
								Map<String, ResultSetReader> aliases = new HashMap<>();
								Iterables.stream(query.getSelectSurrogate())
										.map(AliasedColumn.class::cast).map(AliasedColumn::getColumn)
										.forEach(c -> aliases.put(c.getAlias(), dialect.getColumnBinderRegistry().getBinder(c)));
								Map<Class, Set<I>> idsPerSubtype = readIds(sqlQueryBuilder, aliases, primaryKey);
								
								List<C> result = new ArrayList<>();
								idsPerSubtype.forEach((k, v) -> result.addAll(persisterPerSubclass2.get(k).select(v)));
								return result;
							}
							
							private Map<Class, Set<I>> readIds(SQLQueryBuilder sqlQueryBuilder, Map<String, ResultSetReader> aliases,
															   Column<T, I> primaryKey) {
								Map<Class, Set<I>> result = new HashMap<>();
								PreparedSQL preparedSQL = sqlQueryBuilder.toPreparedSQL(dialect.getColumnBinderRegistry());
								try (ReadOperation readOperation = new ReadOperation<>(preparedSQL, connectionProvider)) {
									ResultSet resultSet = readOperation.execute();
									
									RowIterator resultSetIterator = new RowIterator(resultSet, aliases);
									ColumnedRow columnedRow = new ColumnedRow(Column::getAlias);
									resultSetIterator.forEachRemaining(row -> {
										
										// looking for entity type on row : we read each subclass PK and check for nullity. The non-null one is the 
										// right one
										Class<? extends C> entitySubclass;
										Set<Entry<Class<? extends C>, JoinedTablesPersister<C, I, T>>> entries = persisterPerSubclass.entrySet();
										Entry<Class<? extends C>, JoinedTablesPersister<C, I, T>> subclassEntityOnRow = Iterables.find(entries,
												e -> {
											boolean isPKEmpty = true;
											Iterator<Column> columnIt = e.getValue().getMainTable().getPrimaryKey().getColumns().iterator();
											while (isPKEmpty && columnIt.hasNext()) {
												Column column = columnIt.next();
												isPKEmpty = columnedRow.getValue(column, row) != null;
											}
											return isPKEmpty;
										});
										entitySubclass = subclassEntityOnRow.getKey();
										
										// adding identifier to subclass' ids
										result.computeIfAbsent(entitySubclass, k -> new HashSet<>())
												.add((I) columnedRow.getValue(primaryKey, row));
									});
									return result;
								} catch (RuntimeException e) {
									throw new SQLExecutionException(preparedSQL.getSQL(), e);
								}

							}
						};
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
	
	private class SingleTablePolymorphicInsertExecutor<C, I, T extends Table> extends PolymorphicInsertExecutor<C, I, T> {
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
	
	
	private class SingleTablePolymorphicDeleteExecutor<C, I, T extends Table> extends PolymorphicDeleteExecutor<C, I, T> {
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
}
