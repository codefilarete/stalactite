package org.gama.stalactite.persistence.engine.configurer;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.gama.lang.Reflections;
import org.gama.lang.StringAppender;
import org.gama.lang.collection.Collections;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.IteratorIterator;
import org.gama.lang.collection.KeepOrderSet;
import org.gama.lang.function.Functions;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.MemberDefinition;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.reflection.ValueAccessPointSet;
import org.gama.stalactite.persistence.engine.AssociationTableNamingStrategy;
import org.gama.stalactite.persistence.engine.CascadeManyConfigurer;
import org.gama.stalactite.persistence.engine.CascadeOne;
import org.gama.stalactite.persistence.engine.CascadeOneConfigurer;
import org.gama.stalactite.persistence.engine.ColumnNamingStrategy;
import org.gama.stalactite.persistence.engine.ColumnOptions;
import org.gama.stalactite.persistence.engine.ColumnOptions.BeforeInsertIdentifierPolicy;
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.EmbeddableMappingConfiguration;
import org.gama.stalactite.persistence.engine.EntityMappingConfiguration;
import org.gama.stalactite.persistence.engine.EntityMappingConfiguration.InheritanceConfiguration;
import org.gama.stalactite.persistence.engine.EntityMappingConfigurationProvider;
import org.gama.stalactite.persistence.engine.ForeignKeyNamingStrategy;
import org.gama.stalactite.persistence.engine.MappingConfigurationException;
import org.gama.stalactite.persistence.engine.NotYetSupportedOperationException;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.Persister;
import org.gama.stalactite.persistence.engine.PersisterBuilder;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.JoinedTablesPolymorphism;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.TablePerClassPolymorphism;
import org.gama.stalactite.persistence.engine.SubEntityMappingConfiguration;
import org.gama.stalactite.persistence.engine.TableNamingStrategy;
import org.gama.stalactite.persistence.engine.builder.CascadeMany;
import org.gama.stalactite.persistence.engine.cascade.AfterDeleteByIdSupport;
import org.gama.stalactite.persistence.engine.cascade.AfterDeleteSupport;
import org.gama.stalactite.persistence.engine.cascade.AfterUpdateSupport;
import org.gama.stalactite.persistence.engine.cascade.BeforeInsertSupport;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.MappingPerTable.Mapping;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.listening.UpdateByIdListener;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.assembly.SimpleIdentifierAssembler;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.id.manager.BeforeInsertIdentifierManager;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.id.manager.JDBCGeneratedKeysIdentifierManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.SimpleIdMappingStrategy;
import org.gama.stalactite.persistence.mapping.SinglePropertyIdAccessor;
import org.gama.stalactite.persistence.query.EntityCriteriaSupport.EntityGraphNode;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.dml.GeneratedKeysReader;

import static org.gama.lang.function.Predicates.not;
import static org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy.AFTER_INSERT;

/**
 * @author Guillaume Mary
 */
public class PersisterBuilderImpl<C, I> implements PersisterBuilder<C, I> {
	
	private final EntityMappingConfiguration<C, I> entityMappingConfiguration;
	private final MethodReferenceCapturer methodSpy;
	private ColumnBinderRegistry columnBinderRegistry;
	private Table table;
	private TableNamingStrategy tableNamingStrategy;
	private final Map<EntityMappingConfiguration, Table> tableMap = new HashMap<>();
	private ColumnNamingStrategy columnNamingStrategy;
	private ForeignKeyNamingStrategy foreignKeyNamingStrategy;
	private ColumnNamingStrategy joinColumnNamingStrategy;
	private AssociationTableNamingStrategy associationTableNamingStrategy;
	
	public PersisterBuilderImpl(EntityMappingConfigurationProvider<C, I> entityMappingConfigurationProvider) {
		this(entityMappingConfigurationProvider.getConfiguration());
	}
	
	public PersisterBuilderImpl(EntityMappingConfiguration<C, I> entityMappingConfiguration) {
		this.entityMappingConfiguration = entityMappingConfiguration;
		// to be reviewed if MethodReferenceCapturer instance must be reused from another PersisterBuilderImpl (not expected because should be stateless)
		this.methodSpy = new MethodReferenceCapturer();
	}
	
	PersisterBuilderImpl<C, I> setColumnBinderRegistry(ColumnBinderRegistry columnBinderRegistry) {
		this.columnBinderRegistry = columnBinderRegistry;
		return this;
	}
	
	PersisterBuilderImpl<C, I> setTable(Table table) {
		this.table = table;
		return this;
	}
	
	PersisterBuilderImpl<C, I> setTableNamingStrategy(TableNamingStrategy tableNamingStrategy) {
		this.tableNamingStrategy = tableNamingStrategy;
		return this;
	}
	
	PersisterBuilderImpl<C, I> setColumnNamingStrategy(ColumnNamingStrategy columnNamingStrategy) {
		this.columnNamingStrategy = columnNamingStrategy;
		return this;
	}
	
	PersisterBuilderImpl<C, I> setForeignKeyNamingStrategy(ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		this.foreignKeyNamingStrategy = foreignKeyNamingStrategy;
		return this;
	}
	
	@Override
	public JoinedTablesPersister<C, I, Table> build(PersistenceContext persistenceContext) {
		return build(persistenceContext, null);
	}
	
	@Override
	public <T extends Table> JoinedTablesPersister<C, I, T> build(PersistenceContext persistenceContext, @Nullable T table) {
		init(persistenceContext.getDialect().getColumnBinderRegistry(), table);
		
		if (this.table == null) {
			this.table = (T) new Table(tableNamingStrategy.giveName(this.entityMappingConfiguration.getEntityType()));
		}
		
		mapEntityConfigurationPerTable();
		
		Identification identification = determineIdentification();
		
		// collecting mapping from inheritance
		MappingPerTable inheritanceMappingPerTable = collectEmbeddedMappingFromInheritance();
		// TODO: check if this can be done just before cascade ones addition
		inheritanceMappingPerTable.getMappings().forEach(mapping -> {
			if (mapping.getMappingConfiguration() instanceof EntityMappingConfiguration) {
				((EntityMappingConfiguration<C, I>) mapping.getMappingConfiguration()).getOneToOnes().forEach(cascadeOne -> {
					if (!cascadeOne.isOwnedByReverseSide()) {
						mapping.getMapping().put(
								cascadeOne.getTargetProvider(),
								mapping.getTargetTable().addColumn(joinColumnNamingStrategy.giveName(MemberDefinition.giveMemberDefinition(cascadeOne.getTargetProvider())),
										MemberDefinition.giveMemberDefinition(cascadeOne.getTargetMappingConfiguration().getIdentifierAccessor()).getMemberType()));
					}
				});
			}
		});
		
		// collecting mapping of sub-entities
		MappingPerTable subEntitiesMappingPerTable = collectEmbeddedMappingFromSubEntities();
		
		// add primary key and foreign key to all tables
		Set<Table> impliedTables = new HashSet<>(Collections.cat(inheritanceMappingPerTable.giveTables(), subEntitiesMappingPerTable.giveTables()));
		addPrimarykeys(identification, impliedTables);
		addIdentificationToMapping(identification, () -> new IteratorIterator<>(inheritanceMappingPerTable.getMappings(), subEntitiesMappingPerTable.getMappings()));
		
		// Creating JoinedTablePersister from adhoc Insert/Update/Delete/Select Executors
		// with discriminator for polymorphism single-table
		Mapping mainMapping = Iterables.first(inheritanceMappingPerTable.getMappings());
		JoinedTablesPersister<C, I, T> mainPersister = buildMainPersister(identification, mainMapping, impliedTables, persistenceContext);
		
		// parent persister must be kept in ascending order for further treatments
		Iterator<Mapping> mappings = Iterables.filter(Iterables.reverseIterator(inheritanceMappingPerTable.getMappings().asSet()), not(mainMapping::equals));
		KeepOrderSet<Persister<C, I, Table>> parentPersisters = createParentPersisters(() -> mappings,
				identification, mainPersister, persistenceContext
		);
		
		addCascadesBetweenChildAndParentTable(mainPersister.getPersisterListener(), parentPersisters);
		
		inheritanceMappingPerTable.getMappings().stream().map(Mapping::getMappingConfiguration)
				.filter(EntityMappingConfiguration.class::isInstance).map(EntityMappingConfiguration.class::cast).forEach(entityMappingConfiguration -> {
			configureRelations(entityMappingConfiguration, persistenceContext, mainPersister);
		});
		
		persistenceContext.addPersister(mainPersister);
		
		return mainPersister;
	}
	
	protected  <T extends Table> void configureRelations(EntityMappingConfiguration<C, I> entityMappingConfiguration,
														 PersistenceContext persistenceContext,
														 JoinedTablesPersister<C, I, T> sourcePersister) {
		for (CascadeOne<C, ?, ?> cascadeOne : entityMappingConfiguration.getOneToOnes()) {
			CascadeOneConfigurer cascadeOneConfigurer = new CascadeOneConfigurer<>(persistenceContext, new PersisterBuilderImpl<>(cascadeOne.getTargetMappingConfiguration()));
			cascadeOneConfigurer.appendCascade(cascadeOne, sourcePersister,
					this.foreignKeyNamingStrategy,
					this.joinColumnNamingStrategy);
		}
		for (CascadeMany<C, ?, ?, ? extends Collection> cascadeMany : entityMappingConfiguration.getOneToManys()) {
			CascadeManyConfigurer cascadeManyConfigurer = new CascadeManyConfigurer<>(persistenceContext, new PersisterBuilderImpl<>(cascadeMany.getTargetMappingConfiguration()));
			cascadeManyConfigurer.appendCascade(cascadeMany, sourcePersister,
					this.foreignKeyNamingStrategy,
					this.joinColumnNamingStrategy,
					this.associationTableNamingStrategy);
		}
		registerRelations(entityMappingConfiguration, sourcePersister.getCriteriaSupport().getRootConfiguration(), persistenceContext);
	}
	
	/**
	 * Adds one-to-one and one-to-many graph node to the given root. Used for select by entity properties because without this it could not load
	 * while entity graph
	 *
	 * @param configurationSupport entity mapping configuration which relations must be registered onto target
	 * @param target the node on which to add sub graph elements
	 * @param persistenceContext used as a per-entity {@link org.gama.stalactite.persistence.mapping.IEntityMappingStrategy} registry 
	 */
	private void registerRelations(EntityMappingConfiguration configurationSupport, EntityGraphNode target, PersistenceContext persistenceContext) {
		List<CascadeMany> oneToManys = configurationSupport.getOneToManys();
		oneToManys.forEach((CascadeMany cascadeMany) -> {
			EntityGraphNode entityGraphNode = target.registerRelation(cascadeMany.getCollectionProvider(),
					persistenceContext.getPersister(cascadeMany.getTargetMappingConfiguration().getEntityType()).getMappingStrategy());
			registerRelations(cascadeMany.getTargetMappingConfiguration(), entityGraphNode, persistenceContext);
		});
		List<CascadeOne> oneToOnes = configurationSupport.getOneToOnes();
		oneToOnes.forEach((CascadeOne cascadeOne) -> {
			EntityGraphNode entityGraphNode = target.registerRelation(cascadeOne.getTargetProvider(),
					persistenceContext.getPersister(cascadeOne.getTargetMappingConfiguration().getEntityType()).getMappingStrategy());
			registerRelations(cascadeOne.getTargetMappingConfiguration(), entityGraphNode, persistenceContext);
		});
	}
	
	private <T extends Table> KeepOrderSet<Persister<C, I, Table>> createParentPersisters(Iterable<Mapping> mappings,
																						  Identification identification,
																						  JoinedTablesPersister<C, I, T> mainPersister,
																						  PersistenceContext persistenceContext) {
		KeepOrderSet<Persister<C, I, Table>> parentPersisters = new KeepOrderSet<>();
		Column superclassPK = (Column) Iterables.first(mainPersister.getMainTable().getPrimaryKey().getColumns());
		mappings.forEach(mapping -> {
			Column subclassPK = (Column) Iterables.first(mapping.targetTable.getPrimaryKey().getColumns());
			mapping.mapping.put(identification.getIdAccessor(), subclassPK);
			ClassMappingStrategy<C, I, Table> currentMappingStrategy = createClassMappingStrategy(
					mapping.giveEmbeddableConfiguration(),
					mapping.targetTable,
					mapping.mapping,
					identification,
					persistenceContext.getDialect()::buildGeneratedKeysReader);
			
			JoinedTablesPersister<C, I, Table> currentPersister = new JoinedTablesPersister<>(persistenceContext, currentMappingStrategy);
			parentPersisters.add(currentPersister);
			mainPersister.getJoinedStrategiesSelectExecutor().addRelation(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, currentMappingStrategy,
					(target, input) -> {
				// applying values from inherited bean (input) to subclass one (target)
				for (IReversibleAccessor columnFieldEntry : currentMappingStrategy.getPropertyToColumn().keySet()) {
					columnFieldEntry.toMutator().set(target, columnFieldEntry.get(input));
				}
			}, superclassPK, subclassPK, false);
		});
		return parentPersisters;
	}
	
	private <T extends Table> JoinedTablesPersister<C, I, T> buildMainPersister(Identification identification, Mapping mapping, Set<Table> impliedTables, PersistenceContext persistenceContext) {
		ClassMappingStrategy<C, I, T> parentMappingStrategy = createClassMappingStrategy(
				((EntityMappingConfiguration) mapping.mappingConfiguration).getPropertiesMapping(),
				mapping.targetTable,
				mapping.mapping,
				identification,
				persistenceContext.getDialect()::buildGeneratedKeysReader);
		return (JoinedTablesPersister<C, I, T>) new JoinedTablesPersister(persistenceContext, parentMappingStrategy);
	}
	
	private void addCascadesBetweenChildAndParentTable(PersisterListener<C, I> persisterListener, KeepOrderSet<Persister<C, I, Table>> superPersisters) {
		superPersisters.forEach(superPersister -> {
			// Before insert of child we must insert parent
			persisterListener.addInsertListener(new BeforeInsertSupport<>(superPersister::insert, Function.identity()));
			
			// On child update, parent must be updated too, no constraint on order for this, after is arbitrarly choosen
			persisterListener.addUpdateListener(new AfterUpdateSupport<>(superPersister::update, Function.identity()));
			// idem for updateById
			persisterListener.addUpdateByIdListener(new UpdateByIdListener<C>() {
				@Override
				public void afterUpdateById(Iterable<C> entities) {
					superPersister.updateById(entities);
				}
			});
			
		});
		
		List<Persister<C, I, Table>> copy = Iterables.copy(superPersisters);
		java.util.Collections.reverse(copy);
		copy.forEach(superPersister -> {
			// On child deletion, parent must be deleted after
			persisterListener.addDeleteListener(new AfterDeleteSupport<>(superPersister::delete, Function.identity()));
			// idem for deleteById
			persisterListener.addDeleteByIdListener(new AfterDeleteByIdSupport<>(superPersister::deleteById, Function.identity()));
		});
	}
	
	void mapEntityConfigurationPerTable() {
		visitInheritedEntityMappingConfigurations(new Consumer<EntityMappingConfiguration>() {
			
			private Table currentTable = PersisterBuilderImpl.this.table;
			
			@Override
			public void accept(EntityMappingConfiguration entityMappingConfiguration) {
				InheritanceConfiguration inheritanceConfiguration = entityMappingConfiguration.getInheritanceConfiguration();
				boolean changeTable = org.gama.lang.Nullable.nullable(inheritanceConfiguration)
						.map(InheritanceConfiguration::isJoinTable).getOr(false);
				Table table;
				tableMap.put(entityMappingConfiguration, currentTable);
				if (changeTable) {
					table = org.gama.lang.Nullable.nullable(inheritanceConfiguration.getTable())
							.getOr(() -> new Table(tableNamingStrategy.giveName(inheritanceConfiguration.getConfiguration().getEntityType())));
					currentTable = table;
				}
			}
		});
	}
	
	/**
	 * Made only for testing purpose : initialize the build process without running it as the opposit of {@link #build(PersistenceContext, Table)}
	 *
	 * @param columnBinderRegistry registry used to declare columns binding
	 * @param table optional target table of the main mapping
	 * @param <T> table type
	 */
	@VisibleForTesting
	<T extends Table> void init(ColumnBinderRegistry columnBinderRegistry, @Nullable T table) {
		this.columnBinderRegistry = columnBinderRegistry;
		this.table = table;
		org.gama.lang.Nullable<TableNamingStrategy> optionalTableNamingStrategy = org.gama.lang.Nullable.empty();
		visitInheritedEntityMappingConfigurations(entityMappingConfiguration -> {
			if (entityMappingConfiguration.getTableNamingStrategy() != null && !optionalTableNamingStrategy.isPresent()) {
				optionalTableNamingStrategy.set(entityMappingConfiguration.getTableNamingStrategy());
			}
		});
		tableNamingStrategy = optionalTableNamingStrategy.getOr(TableNamingStrategy.DEFAULT);
		
		// When a ColumnNamingStrategy is defined on mapping, it must be applied to super classes too
		org.gama.lang.Nullable<ColumnNamingStrategy> optionalColumnNamingStrategy = org.gama.lang.Nullable.empty();
		class ColumnNamingStrategyCollector implements Consumer<EmbeddableMappingConfiguration> {
			@Override
			public void accept(EmbeddableMappingConfiguration embeddableMappingConfiguration) {
				if (embeddableMappingConfiguration.getColumnNamingStrategy() != null && !optionalColumnNamingStrategy.isPresent()) {
					optionalColumnNamingStrategy.set(embeddableMappingConfiguration.getColumnNamingStrategy());
				}
			}
		}
		ColumnNamingStrategyCollector columnNamingStrategyCollector = new ColumnNamingStrategyCollector();
		visitInheritedEmbeddableMappingConfigurations(entityConfigurationConsumer ->
				columnNamingStrategyCollector.accept(entityConfigurationConsumer.getPropertiesMapping()), columnNamingStrategyCollector);
		columnNamingStrategy = optionalColumnNamingStrategy.getOr(ColumnNamingStrategy.DEFAULT);
		
		org.gama.lang.Nullable<ForeignKeyNamingStrategy> optionalForeignKeyNamingStrategy = org.gama.lang.Nullable.empty();
		visitInheritedEntityMappingConfigurations(entityMappingConfiguration -> {
			if (entityMappingConfiguration.getForeignKeyNamingStrategy() != null && !optionalForeignKeyNamingStrategy.isPresent()) {
				optionalForeignKeyNamingStrategy.set(entityMappingConfiguration.getForeignKeyNamingStrategy());
			}
		});
		foreignKeyNamingStrategy = optionalForeignKeyNamingStrategy.getOr(ForeignKeyNamingStrategy.DEFAULT);
		
		org.gama.lang.Nullable<ColumnNamingStrategy> optionalJoinColumnNamingStrategy = org.gama.lang.Nullable.empty();
		visitInheritedEntityMappingConfigurations(entityMappingConfiguration -> {
			if (entityMappingConfiguration.getJoinColumnNamingStrategy() != null && !optionalJoinColumnNamingStrategy.isPresent()) {
				optionalJoinColumnNamingStrategy.set(entityMappingConfiguration.getJoinColumnNamingStrategy());
			}
		});
		joinColumnNamingStrategy = optionalJoinColumnNamingStrategy.getOr(ColumnNamingStrategy.JOIN_DEFAULT);
		
		org.gama.lang.Nullable<AssociationTableNamingStrategy> optionalAssociationTableNamingStrategy = org.gama.lang.Nullable.empty();
		visitInheritedEntityMappingConfigurations(entityMappingConfiguration -> {
			if (entityMappingConfiguration.getAssociationTableNamingStrategy() != null && !optionalAssociationTableNamingStrategy.isPresent()) {
				optionalAssociationTableNamingStrategy.set(entityMappingConfiguration.getAssociationTableNamingStrategy());
			}
		});
		associationTableNamingStrategy = optionalAssociationTableNamingStrategy.getOr(AssociationTableNamingStrategy.DEFAULT);
	}
	
	void addIdentificationToMapping(Identification identification, Iterable<Mapping> mappings) {
		mappings.forEach(mapping -> {
			Column primaryKey = (Column) Iterables.first(mapping.getTargetTable().getPrimaryKey().getColumns());
			mapping.getMapping().put(identification.getIdAccessor(), primaryKey);
		});
	}
	
	void addPrimarykeys(Identification identification, Set<Table> joinedTables) {
		Table pkTable = this.tableMap.get(identification.identificationDefiner);
		if (pkTable == null) {
			// Should not happen except during this class development
			throw new IllegalArgumentException("Table for primary key wasn't found in given tables : looking for "
					+ this.tableNamingStrategy.giveName(identification.identificationDefiner.getEntityType())
					+ " in [" + new StringAppender().ccat(Iterables.collectToList(joinedTables, Table::getAbsoluteName), ", ") + "]");
		}
		
		MemberDefinition memberDefinition = MemberDefinition.giveMemberDefinition(identification.getIdAccessor());
		Column primaryKey = pkTable.addColumn(columnNamingStrategy.giveName(memberDefinition), memberDefinition.getMemberType());
		primaryKey.setNullable(false);	// may not be necessary because of primary key, let for principle
		primaryKey.primaryKey();
		if (identification.getIdentifierPolicy() == AFTER_INSERT) {
			primaryKey.autoGenerated();
		}
		final Column[] previousPk = { primaryKey };
		joinedTables.stream().filter(not(primaryKey.getTable()::equals)).forEach(t -> {
			Column newColumn = t.addColumn(columnNamingStrategy.giveName(memberDefinition), memberDefinition.getMemberType());
			newColumn.setNullable(false);	// may not be necessary because of primary key, let for principle
			newColumn.primaryKey();
			t.addForeignKey(foreignKeyNamingStrategy.giveName(newColumn, previousPk[0]), newColumn, previousPk[0]);
			previousPk[0] = newColumn;
		});
		
	}
	
	/**
	 * Gives embedded (non relational) properties mapping, including those from super classes
	 *
	 * @return the mapping between property accessor and their column in target tables, never null
	 */
	@VisibleForTesting
	MappingPerTable collectEmbeddedMappingFromInheritance() {
		MappingPerTable result = new MappingPerTable();
		BeanMappingBuilder beanMappingBuilder = new BeanMappingBuilder();
		
		class MappingCollector implements Consumer<EmbeddableMappingConfiguration> {
			
			private Table currentTable;
			
			private Map<IReversibleAccessor, Column> currentColumnMap = new HashMap<>();
			
			private Mapping currentMapping;
			
			private Object currentKey;
			
			@Override
			public void accept(EmbeddableMappingConfiguration embeddableMappingConfiguration) {
				Map<IReversibleAccessor, Column> propertiesMapping = beanMappingBuilder.build(
						embeddableMappingConfiguration, this.currentTable,
						PersisterBuilderImpl.this.columnBinderRegistry, new ColumnNameProvider(columnNamingStrategy));
				ValueAccessPointSet localMapping = new ValueAccessPointSet(currentColumnMap.keySet());
				propertiesMapping.keySet().forEach(propertyAccessor -> {
					if (localMapping.contains(propertyAccessor)) {
						throw new MappingConfigurationException(MemberDefinition.toString(propertyAccessor) + " is mapped twice");
					}
				});
				currentColumnMap.putAll(propertiesMapping);
				if (currentMapping == null) {
					currentMapping = result.add(currentKey == null ? embeddableMappingConfiguration : currentKey, this.currentTable, currentColumnMap);
				} else {
					currentMapping.getMapping().putAll(currentColumnMap);
				}
			}
			
			public void accept(EntityMappingConfiguration entityMappingConfiguration) {
				accept(entityMappingConfiguration.getPropertiesMapping());
			}
		}
		
		
		MappingCollector mappingCollector = new MappingCollector();
		visitInheritedEmbeddableMappingConfigurations(new Consumer<EntityMappingConfiguration>() {
			private boolean initMapping = false;

			@Override
			public void accept(EntityMappingConfiguration entityMappingConfiguration) {
				mappingCollector.currentKey = entityMappingConfiguration;
				if (initMapping) {
					mappingCollector.currentColumnMap = new HashMap<>();
					mappingCollector.currentMapping = null;
				}
				
				mappingCollector.currentTable = tableMap.get(entityMappingConfiguration);
				mappingCollector.accept(entityMappingConfiguration);
				
				// we must reinit mapping when table changes (which is a join table case), then mapping doesn't target always the same Map 
				initMapping = org.gama.lang.Nullable.nullable(entityMappingConfiguration.getInheritanceConfiguration())
						.map(InheritanceConfiguration::isJoinTable).getOr(false);
			}
		}, mappingCollector);
		return result;
	}
	
	void visitInheritedEmbeddableMappingConfigurations(Consumer<EntityMappingConfiguration> entityConfigurationConsumer,
													   Consumer<EmbeddableMappingConfiguration> mappedSuperClassConfigurationConsumer) {
		// iterating over mapping from inheritance
		final EntityMappingConfiguration[] last = new EntityMappingConfiguration[1];
		visitInheritedEntityMappingConfigurations(entityMappingConfiguration -> {
			entityConfigurationConsumer.accept(entityMappingConfiguration);
			last[0] = entityMappingConfiguration;
		});
		if (last[0].getPropertiesMapping().getMappedSuperClassConfiguration() != null) {
			// iterating over mapping from mapped super classes
			last[0].getPropertiesMapping().getMappedSuperClassConfiguration().inheritanceIterable().forEach(mappedSuperClassConfigurationConsumer);
		}
	}
	
	void visitInheritedEntityMappingConfigurations(Consumer<EntityMappingConfiguration> configurationConsumer) {
		// iterating over mapping from inheritance
		entityMappingConfiguration.inheritanceIterable().forEach(configurationConsumer);
	}
	
	@VisibleForTesting
	MappingPerTable collectEmbeddedMappingFromSubEntities() {
		MappingPerTable result = new MappingPerTable();
		BeanMappingBuilder beanMappingBuilder = new BeanMappingBuilder();
		PolymorphismPolicy polymorphismPolicy = entityMappingConfiguration.getPolymorphismPolicy();
		if (polymorphismPolicy != null) {
			if (polymorphismPolicy instanceof SingleTablePolymorphism) {
				Collection<? extends SubEntityMappingConfiguration<?, ?>> subClasses = ((SingleTablePolymorphism<?, ?, ?>) polymorphismPolicy).getSubClasses();
				subClasses.forEach(o -> result.add(o, this.table, beanMappingBuilder.build(o.getPropertiesMapping(),
						this.table, this.columnBinderRegistry, new ColumnNameProvider(columnNamingStrategy))));
			} else if (polymorphismPolicy instanceof TablePerClassPolymorphism) {
				Collection<? extends SubEntityMappingConfiguration<?, ?>> subClasses = ((TablePerClassPolymorphism<?, ?>) polymorphismPolicy).getSubClasses();
				subClasses.forEach(o -> result.add(o, this.table, beanMappingBuilder.build(o.getPropertiesMapping(),
						this.table, this.columnBinderRegistry, new ColumnNameProvider(columnNamingStrategy))));
			} else if (polymorphismPolicy instanceof JoinedTablesPolymorphism) {
				JoinedTablesPolymorphism<?, ?> joinedTablesPolymorphismPolicy = (JoinedTablesPolymorphism<?, ?>) polymorphismPolicy;
				Collection<? extends SubEntityMappingConfiguration<?, ?>> subClasses = joinedTablesPolymorphismPolicy.getSubClasses();
				subClasses.forEach(o -> {
					Table table = org.gama.lang.Nullable.nullable(joinedTablesPolymorphismPolicy.giveTable(o)).getOr(() -> new Table(tableNamingStrategy.giveName(o.getEntityType())));
					result.add(o, table, beanMappingBuilder.build(o.getPropertiesMapping(), table, this.columnBinderRegistry, new ColumnNameProvider(columnNamingStrategy)));
				});
			}
		}
		return result;
	}
	
	/**
	 * Looks for identifier as well as policy by going up through inheritance hierarchy.
	 *
	 * @return a couple that defines identification of the mapping
	 * @throws UnsupportedOperationException when identifiation was not found, because it doesn't make sense to have an entity without identification
	 */
	@VisibleForTesting
	<T extends Table<?>> Identification determineIdentification() {
		if (entityMappingConfiguration.getInheritanceConfiguration() != null && entityMappingConfiguration.getPropertiesMapping().getMappedSuperClassConfiguration() != null) {
			throw new MappingConfigurationException("Mapped super class and inheritance are not supported when they are combined, please remove one of them");
		}
		
		// if mappedSuperClass is used, then identifier is expected to be declared on the configuration
		// because mappedSuperClass can't define it (it is an EmbeddableMappingConfiguration)
		EntityMappingConfiguration<? super C, I> configurationDefiningIdentification;
		if (entityMappingConfiguration.getPropertiesMapping().getMappedSuperClassConfiguration() != null) {
			if (entityMappingConfiguration.getIdentifierPolicy() == null) {
				// no ClassMappingStratey in hierarchy, so we can't get an identifier from it => impossible
				throw newMissingIdentificationException();
			} else {
				// NB: identifierAccessor is expected to be non null since policy can't be declared without it
				configurationDefiningIdentification = entityMappingConfiguration;
			}
		} else {
			// hierarchy must be scanned to find the very first configuration that defines identification
			configurationDefiningIdentification = Iterables.last(entityMappingConfiguration.inheritanceIterable());
			if (configurationDefiningIdentification == null) {
				if (configurationDefiningIdentification.getPropertiesMapping().getMappedSuperClassConfiguration() != null) {
					
				}
				// no ClassMappingStratey in hierarchy, so we can't get an identifier from it => impossible
				throw newMissingIdentificationException();
			} 
		}
		return new Identification(configurationDefiningIdentification);
	}
	
	private UnsupportedOperationException newMissingIdentificationException() {
		SerializableBiFunction<ColumnOptions, IdentifierPolicy, ColumnOptions> identifierMethodReference = ColumnOptions::identifier;
		Method identifierSetter = this.methodSpy.findMethod(identifierMethodReference);
		return new UnsupportedOperationException("Identifier is not defined for " + Reflections.toString(entityMappingConfiguration.getEntityType())
				+ ", please add one throught " + Reflections.toString(identifierSetter));
	}
	
	private <T extends Table> ClassMappingStrategy<C, I, T> createClassMappingStrategy(
			EmbeddableMappingConfiguration<C> entityMappingConfiguration,
			T targetTable,
			Map<? extends IReversibleAccessor, Column> mapping,
			Identification identification,
			GeneratedKeysReaderBuilder generatedKeysReaderBuilder) {
		// Child class insertion manager is always an "Already assigned" one because parent manages it for her
		IdentifierInsertionManager<C, I> identifierInsertionManager = null;
		
		Column primaryKey = (Column) Iterables.first(targetTable.getPrimaryKey().getColumns());
		IdentifierPolicy identifierPolicy = identification.getIdentificationDefiner().getIdentifierPolicy();
		IReversibleAccessor idAccessor = identification.getIdAccessor();
		MemberDefinition idDefinition = MemberDefinition.giveMemberDefinition(idAccessor);
		Class identifierType = idDefinition.getMemberType();
		if (identification.getIdentificationDefiner().getPropertiesMapping() == entityMappingConfiguration) {
			if (identifierPolicy == AFTER_INSERT) {
				identifierInsertionManager = new JDBCGeneratedKeysIdentifierManager<>(
						new SinglePropertyIdAccessor<>(idAccessor),
						generatedKeysReaderBuilder.buildGeneratedKeysReader(primaryKey.getName(), primaryKey.getJavaType()),
						primaryKey.getJavaType()
				);
			} else if (identifierPolicy instanceof ColumnOptions.BeforeInsertIdentifierPolicy) {
				identifierInsertionManager = new BeforeInsertIdentifierManager<>(
						new SinglePropertyIdAccessor<>(idAccessor), ((BeforeInsertIdentifierPolicy<I>) identifierPolicy).getIdentifierProvider(), identifierType);
			} else if (identifierPolicy == IdentifierPolicy.ALREADY_ASSIGNED) {
				if (Identified.class.isAssignableFrom(idDefinition.getDeclaringClass()) && Identifier.class.isAssignableFrom(identifierType)) {
					identifierInsertionManager = new IdentifiedIdentifierManager<>(identifierType);
				} else {
					throw new NotYetSupportedOperationException(
							"Already-assigned identifier policy is only supported with entities that implement " + Reflections.toString(Identified.class));
				}
			}
		} else {
			identifierInsertionManager = new AlreadyAssignedIdentifierManager<>(identifierType);
		}
		SimpleIdMappingStrategy<C, I> simpleIdMappingStrategy = new SimpleIdMappingStrategy<>(idAccessor, identifierInsertionManager,
				new SimpleIdentifierAssembler<>(primaryKey));
		
		return new ClassMappingStrategy<C, I, T>(entityMappingConfiguration.getBeanType(),
				targetTable, (Map) mapping, simpleIdMappingStrategy, c -> Reflections.newInstance(this.entityMappingConfiguration.getEntityType()));
	}
	
	/**
	 * Identifier manager dedicated to {@link Identified} entities
	 * @param <C> entity type
	 * @param <I> identifier type
	 */
	private static class IdentifiedIdentifierManager<C, I> extends AlreadyAssignedIdentifierManager<C, I> {
		public IdentifiedIdentifierManager(Class<I> identifierType) {
			super(identifierType);
		}
		
		@Override
		public void setPersistedFlag(C e) {
			((Identified) e).getId().setPersisted();
		}
	}
	
	
	static class Identification {
		
		private final IReversibleAccessor idAccessor;
		private final IdentifierPolicy identifierPolicy;
		private final EntityMappingConfiguration identificationDefiner;
		
		
		public Identification(EntityMappingConfiguration identificationDefiner) {
			this.idAccessor = identificationDefiner.getIdentifierAccessor();
			this.identifierPolicy = identificationDefiner.getIdentifierPolicy();
			this.identificationDefiner = identificationDefiner;
		}
		
		public IReversibleAccessor getIdAccessor() {
			return idAccessor;
		}
		
		public IdentifierPolicy getIdentifierPolicy() {
			return identifierPolicy;
		}
		
		public EntityMappingConfiguration getIdentificationDefiner() {
			return identificationDefiner;
		}
	}
	
	@VisibleForTesting
	static class MappingPerTable {
		private final KeepOrderSet<Mapping> mappings = new KeepOrderSet<>();
		
		Mapping add(Object /* EntityMappingConfiguration, EmbeddableMappingConfiguration, SubEntityMappingConfiguration */ mappingConfiguration,
				 Table table, Map<IReversibleAccessor, Column> mapping) {
			Mapping newMapping = new Mapping(mappingConfiguration, table, mapping);
			this.mappings.add(newMapping);
			return newMapping;
		}
		
		Map<IReversibleAccessor, Column> giveMapping(Table table) {
			Mapping foundMapping = Iterables.find(this.mappings, m -> m.getTargetTable().equals(table));
			if (foundMapping == null) {
				throw new IllegalArgumentException("Can't find table '" + table.getAbsoluteName()
						+ "' in " + Iterables.collectToList(this.mappings, Functions.chain(Mapping::getTargetTable, Table::getAbsoluteName)).toString());
			}
			return foundMapping.mapping;
		}
		
		Set<Table> giveTables() {
			return Iterables.collect(this.mappings, Mapping::getTargetTable, HashSet::new);
		}
		
		public KeepOrderSet<Mapping> getMappings() {
			return mappings;
		}
		
		public class Mapping {
			private final Object /* EntityMappingConfiguration, EmbeddableMappingConfiguration, SubEntityMappingConfiguration */ mappingConfiguration;
			private final Table targetTable;
			private final Map<IReversibleAccessor, Column> mapping;
			
			private Mapping(Object mappingConfiguration, Table targetTable, Map<IReversibleAccessor, Column> mapping) {
				this.mappingConfiguration = mappingConfiguration;
				this.targetTable = targetTable;
				this.mapping = mapping;
			}
			
			public Object getMappingConfiguration() {
				return mappingConfiguration;
			}
			
			public EmbeddableMappingConfiguration giveEmbeddableConfiguration() {
				return (EmbeddableMappingConfiguration) (this.mappingConfiguration instanceof EmbeddableMappingConfiguration
						? this.mappingConfiguration
						: (this.mappingConfiguration instanceof EntityMappingConfiguration ?
						((EntityMappingConfiguration) this.mappingConfiguration).getPropertiesMapping()
						: null));
			}
			
			private Table getTargetTable() {
				return targetTable;
			}
			
			public Map<IReversibleAccessor, Column> getMapping() {
				return mapping;
			}
		}
	}
	
	@FunctionalInterface
	private interface GeneratedKeysReaderBuilder {
		GeneratedKeysReader buildGeneratedKeysReader(String keyName, Class columnType);
	}
}
