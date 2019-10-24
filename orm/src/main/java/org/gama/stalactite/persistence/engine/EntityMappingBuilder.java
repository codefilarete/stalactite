package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import java.util.function.Function;

import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.gama.lang.Duo;
import org.gama.lang.Nullable;
import org.gama.lang.Reflections;
import org.gama.lang.collection.Iterables;
import org.gama.lang.exception.NotImplementedException;
import org.gama.lang.function.Predicates;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.MemberDefinition;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.reflection.ValueAccessPoint;
import org.gama.reflection.ValueAccessPointSet;
import org.gama.stalactite.persistence.engine.ColumnOptions.BeforeInsertIdentifierPolicy;
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.EmbeddableMappingBuilder.ColumnNameProvider;
import org.gama.stalactite.persistence.engine.EmbeddableMappingConfiguration.Linkage;
import org.gama.stalactite.persistence.engine.EntityMappingBuilder.EntityDecoratedEmbeddableMappingBuilder.InheritanceInfo;
import org.gama.stalactite.persistence.engine.FluentEmbeddableMappingConfigurationSupport.Inset;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupport.EntityLinkage;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupport.EntityLinkageByColumn;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupport.OverridableColumnInset;
import org.gama.stalactite.persistence.engine.builder.CascadeMany;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.assembly.SimpleIdentifierAssembler;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.id.manager.BeforeInsertIdentifierManager;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.id.manager.JDBCGeneratedKeysIdentifierManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.mapping.IdAccessor;
import org.gama.stalactite.persistence.mapping.IdMappingStrategy;
import org.gama.stalactite.persistence.mapping.SimpleIdMappingStrategy;
import org.gama.stalactite.persistence.mapping.SinglePropertyIdAccessor;
import org.gama.stalactite.persistence.query.EntityCriteriaSupport.EntityGraphNode;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.Nullable.nullable;
import static org.gama.lang.collection.Iterables.collect;

/**
 * Engine that converts mapping definition of a {@link EntityMappingConfiguration} into a {@link Persister}.
 * Please note that even if some abstraction is intented through {@link EntityMappingConfiguration} this class remains closely tied to
 * {@link FluentEntityMappingConfigurationSupport} mainly by referencing one of its "internal" classes or interfaces. Decoupling it requires even more
 * abstraction.
 * 
 * @author Guillaume Mary
 * @see #build(PersistenceContext)
 * @see #build(PersistenceContext, Table) 
 */
class EntityMappingBuilder<C, I> extends AbstractEntityMappingBuilder<C, I> {
	
	EntityMappingBuilder(EntityMappingConfiguration<C, I> entityBuilderSupport, MethodReferenceCapturer methodSpy) {
		super(entityBuilderSupport, methodSpy);
	}
	
	/**
	 * Build a {@link Persister} from the given configuration at contruction time.
	 * The (optional) given {@link Table} may be fulfilled with some required columns and foreign keys as needed by the configuration
	 * if they are not already declared in the table.
	 *
	 * @param persistenceContext the {@link PersistenceContext} in which the resulting {@link Persister} will be put, also needed for Dialect purpose 
	 * @return the built {@link Persister}
	 */
	@Override
	protected <T extends Table<?>> JoinedTablesPersister<C, I, T> doBuild(PersistenceContext persistenceContext, T table) {
		IdentificationDeterminer<C, I> identificationDeterminer = new IdentificationDeterminer<>(
				configurationSupport.getEntityType(),
				configurationSupport.inheritanceIterable().iterator(),
				configurationSupport.getIdentifierPolicy(),
				columnNameProvider,
				this.methodSpy);
		Duo<IReversibleAccessor<C, I>, IdentifierInsertionManager<C, I>> identification = identificationDeterminer.determineIdentification(persistenceContext.getDialect(), table);
		
		IEntityMappingStrategy<? super C, I, Table> inheritanceMapping = giveInheritanceMapping(persistenceContext, table);
		
		InheritanceInfo<C> inheritanceInfo = null;
		if (inheritanceMapping != null) {
			inheritanceInfo = new InheritanceInfo<C>() {
				@Override
				public IdMappingStrategy<? super C, ?> getIdMappingStrategy() {
					return inheritanceMapping.getIdMappingStrategy();
				}
				
				@Override
				public boolean inheritancePropertiesAreInTargetTable() {
					// table-per-class and single-table needs inheritance properties to be present in target table (for insert, update, etc.), not join-table
					return !configurationSupport.getInheritanceConfiguration().isJoinTable();
				}
				
				@Override
				public Map<IReversibleAccessor<C, Object>, Column<? extends Table, Object>> getPropertyToColumn() {
					return (Map) inheritanceMapping.getPropertyToColumn();
				}
			};
		}
		
		EntityDecoratedEmbeddableMappingBuilder<C> embeddableMappingBuilder = new EntityDecoratedEmbeddableMappingBuilder<>(
				configurationSupport.getEntityFactory(),
				configurationSupport.getPropertiesMapping(),
				columnNameProvider,
				configurationSupport.getOneToOnes(),
				inheritanceInfo);
		
		JoinedTablesPersister<C, I, T> result = createPersister(persistenceContext,
				embeddableMappingBuilder.buildClassMappingStrategy(persistenceContext.getDialect(), table, identification));
		
		handleVersioningStrategy(result);
		
		return result;
	}
	
	@javax.annotation.Nullable
	private <T extends Table<?>> IEntityMappingStrategy<? super C, I, Table> giveInheritanceMapping(PersistenceContext persistenceContext, T table) {
		IEntityMappingStrategy<? super C, I, Table> result = null;
		if (configurationSupport.getInheritanceConfiguration() != null) {
			if (configurationSupport.isJoinTable()) {
				// Note that generics can't be used because "<? super C> can't be instantiated directly"
				result = new JoinedTablesEntityMappingBuilder(configurationSupport, methodSpy)
						.build(persistenceContext, table)
						.getMappingStrategy();
			} else {
				result = new EntityMappingBuilder(configurationSupport.getInheritanceConfiguration(), methodSpy) {
					/**
					 * Overriden to invoke enclosing instance method because it can be overriden too, and this instance must call en enclosing one.
					 * Else it is impossible to overriden inheritance registration
					 */
					@Override
					protected void registerPersister(JoinedTablesPersister persister, PersistenceContext persistenceContext) {
						EntityMappingBuilder.this.registerPersister(persister, persistenceContext);
					}
				}
						.build(persistenceContext, table)
						.getMappingStrategy();
			}
		}
		return result;
	}
	
	/**
	 * Configures relations of our {@link EntityMappingConfiguration} onto the given {@link ClassMappingStrategy} previously built.
	 * 
	 * @param persistenceContext our current {@link PersistenceContext}
	 * @param mainMappingStrategy the mapping of simple properties
	 * @param <T> table type
	 * @return a {@link JoinedTablesPersister} that allow to persist the entity with its relations
	 */
	<T extends Table> JoinedTablesPersister<C, I, T> createPersister(PersistenceContext persistenceContext, IEntityMappingStrategy<C, I, T> mainMappingStrategy) {
		// Please note that we could have created a simple Persister for cases of no one-to-one neither one-to-many relations
		// because in such cases no join is required, but it would have introduced inconsistent return type depending on configuration
		// which is hard to manage by JoinedTablesEntityMappingBuilder that uses this method
		JoinedTablesPersister<C, I, T> result = newJoinedTablesPersister(persistenceContext, mainMappingStrategy);
		// don't forget to register this new persister, it's usefull for schema deployment
		registerPersister(result, persistenceContext);
		
		configureRelations(persistenceContext, result);
		
		return result;
	}
	
	protected <T extends Table> JoinedTablesPersister<C, I, T> newJoinedTablesPersister(PersistenceContext persistenceContext,
																						IEntityMappingStrategy<C, I, T> mainMappingStrategy) {
		return new JoinedTablesPersister<>(persistenceContext, mainMappingStrategy);
	}
	
	protected <T extends Table> void registerPersister(JoinedTablesPersister<C, I, T> persister, PersistenceContext persistenceContext) {
		Persister<C, Object, T> existingPersister = persistenceContext.getPersister(persister.getMappingStrategy().getClassToPersist());
		if (existingPersister == null) {
			persistenceContext.addPersister(persister);
		} else {
			BiPredicate<IEntityMappingStrategy, IEntityMappingStrategy> mappingConfigurationComparator = Predicates.and(
					IEntityMappingStrategy::getTargetTable,
					IEntityMappingStrategy::getPropertyToColumn,
					IEntityMappingStrategy::getVersionedKeys,
					IEntityMappingStrategy::getSelectableColumns,
					IEntityMappingStrategy::getUpdatableColumns,
					IEntityMappingStrategy::getInsertableColumns
			);
			if (!mappingConfigurationComparator.test(existingPersister.getMappingStrategy(), persister.getMappingStrategy())) {
				throw new IllegalArgumentException("Persister already exists for " + Reflections.toString(persister.getMappingStrategy().getClassToPersist()));
			}
		}
	}
	
	private <T extends Table> void configureRelations(PersistenceContext persistenceContext, JoinedTablesPersister<C, I, T> result) {
		CascadeOneConfigurer cascadeOneConfigurer = new CascadeOneConfigurer<>(persistenceContext);
		for (CascadeOne<C, ?, ?> cascadeOne : this.configurationSupport.getOneToOnes()) {
			cascadeOneConfigurer.appendCascade(cascadeOne, result, this.configurationSupport.getForeignKeyNamingStrategy(), this.configurationSupport.getJoinColumnNamingStrategy());
		}
		CascadeManyConfigurer cascadeManyConfigurer = new CascadeManyConfigurer(persistenceContext);
		for (CascadeMany<C, ?, ?, ? extends Collection> cascadeMany : this.configurationSupport.getOneToManys()) {
			cascadeManyConfigurer.appendCascade(cascadeMany, result,
					this.configurationSupport.getForeignKeyNamingStrategy(),
					this.configurationSupport.getJoinColumnNamingStrategy(),
					this.configurationSupport.getAssociationTableNamingStrategy());
		}
		registerRelations(configurationSupport, result.getCriteriaSupport().getRootConfiguration(), persistenceContext);
	}
	
	private <T extends Table> void handleVersioningStrategy(JoinedTablesPersister<C, I, T> result) {
		Nullable<VersioningStrategy> versioningStrategy = nullable(this.configurationSupport.getOptimisticLockOption());
		if (versioningStrategy.isPresent()) {
			// we have to declare it to the mapping strategy. To do that we must find the versionning column
			Column column = result.getMappingStrategy().getPropertyToColumn().get(this.configurationSupport.getOptimisticLockOption().getVersionAccessor());
			((ClassMappingStrategy) result.getMappingStrategy()).addVersionedColumn(this.configurationSupport.getOptimisticLockOption().getVersionAccessor(), column);
			// and don't forget to give it to the workers !
			result.getUpdateExecutor().setVersioningStrategy(versioningStrategy.get());
			result.getInsertExecutor().setVersioningStrategy(versioningStrategy.get());
		}
	}
	
	/**
	 * Enhanced version of {@link EmbeddableMappingBuilder} to add entity features such as id mapping and inheritance.
	 * This class indents to target only one table without join, it is used to build table-per-class and single-table inheritance : caller may
	 * add cascades for instance.
	 * 
	 * @param <C> entity type
	 * @see #buildClassMappingStrategy(Dialect, Table, Duo) 
	 */
	static class EntityDecoratedEmbeddableMappingBuilder<C> extends EmbeddableMappingBuilder<C> {
		
		/**
		 * Small interface to describe necessary inheritance inputs for this class 
		 * @param <C> entity type
		 */
		public interface InheritanceInfo<C> {
			
			IdMappingStrategy<? super C, ?> getIdMappingStrategy();
			
			boolean inheritancePropertiesAreInTargetTable();
			
			Map<IReversibleAccessor<C, Object>, Column<? extends Table, Object>> getPropertyToColumn();
			
		}
		
		private final Function<Function<Column, Object>, C> entityFactory;
		private final List<CascadeOne<C, ?, ?>> oneToOnes;
		private final InheritanceInfo<C> inheritanceInfo;
		/** Keep track of oneToOne properties to be removed of direct mapping */
		private final ValueAccessPointSet oneToOnePropertiesOwnedByReverseSide;
		
		EntityDecoratedEmbeddableMappingBuilder(Function<Function<Column, Object>, C> entityFactory,
												EmbeddableMappingConfiguration<C> propertiesMapping,
												ColumnNameProvider columnNameProvider,
												List<CascadeOne<C, ?, ?>> oneToOnes,
												@javax.annotation.Nullable InheritanceInfo<C> inheritanceInfo) {
			super(propertiesMapping, columnNameProvider);
			this.entityFactory = entityFactory;
			this.oneToOnes = oneToOnes;
			this.inheritanceInfo = inheritanceInfo;
			// CascadeOne.getTargetProvider() returns a method reference that can't be compared to PropertyAccessor (of Linkage.getAccessor
			// in mappingConfiguration.getPropertiesMapping() keys) so we use a ValueAccessPoint to do it 
			this.oneToOnePropertiesOwnedByReverseSide = collect(
					oneToOnes,
					CascadeOne::isOwnedByReverseSide,
					CascadeOne::getTargetProvider,
					ValueAccessPointSet::new);
		}
		
		/** Overriden to take property definition by column into account */
		@Override
		protected Column findColumn(ValueAccessPoint valueAccessPoint, String defaultColumnName, Map<String, Column<Table, Object>> tableColumnsPerName, Inset<C, ?> configuration) {
			Column overridenColumn = ((OverridableColumnInset<C, ?>) configuration).getOverridenColumns().get(valueAccessPoint);
			return nullable(overridenColumn).getOr(() -> super.findColumn(valueAccessPoint, defaultColumnName, tableColumnsPerName, configuration));
		}
		
		/**
		 * Overriden to remove properties of one-to-one relation that are not owned by this side but owned by the reverse side
		 */
		@Override
		protected void includeDirectMapping() {
			mappingConfiguration.getPropertiesMapping().stream()
					.filter(linkage -> !oneToOnePropertiesOwnedByReverseSide.contains(linkage.getAccessor()))
					.forEach(this::includeMapping);
		}
		
		@Override
		protected void ensureColumnBindingInRegistry(Linkage linkage, Column column) {
			if (linkage instanceof FluentEntityMappingConfigurationSupport.EntityLinkageByColumn) {
				// we ensure that column has an associated binder
				Column mappedColumn = ((EntityLinkageByColumn) linkage).getColumn();
				if (!dialect.getColumnBinderRegistry().keys().contains(mappedColumn)) {
					ensureColumnBindingExceptOneToOne(linkage, column);
				}
				// and it should be in target table !
				if (!targetTable.getColumns().contains(mappedColumn)) {
					throw new MappingConfigurationException("Column specified for mapping " + MemberDefinition.toString(linkage.getAccessor())
							+ " is not in target table : column " + mappedColumn.getAbsoluteName() + " is not in table " + targetTable.getName());
				}
			} else {
				ensureColumnBindingExceptOneToOne(linkage, column);
			}
		}
		
		/**
		 * Same as {@link #ensureColumnBindingInRegistry(Linkage, Column)} but will do nothing for one-to-one relation because it would try to find a
		 * binder for an entity (which may exist if user declare it) which can hardly/never implement correctly the reading process because
		 * target entity should be created with a complete information set, which is not possible throught {@link org.gama.stalactite.sql.binder.ResultSetReader#get(ResultSet, String)}.
		 * Entity will be created correctly by {@link org.gama.stalactite.persistence.engine.cascade.StrategyJoinsRowTransformer}
		 * 
		 * @param linkage
		 * @param column
		 */
		private void ensureColumnBindingExceptOneToOne(Linkage linkage, Column column) {
			// we should not check for column binding of owner because its column will be manage by CascadeOneConfigurer. Without this, we get a
			// BindingException at each OneToOne
			// NB: we can use equals(..) here because Linkage was created from oneToOne information, no need to create a MemberDefinition for comparison
			if (!Iterables.contains(this.oneToOnes.iterator(), cascadeOne -> cascadeOne.getTargetProvider().equals(linkage.getAccessor()))) {
				super.ensureColumnBindingInRegistry(linkage, column);
			}
		}
		
		/** Overriden to take primary key into account */
		@Override
		protected Column addLinkage(Linkage linkage) {
			Column column;
			if (linkage instanceof EntityLinkageByColumn) {
				column = ((EntityLinkageByColumn) linkage).getColumn();
			} else {
				column = super.addLinkage(linkage);
			}
			// setting the primary key option as asked
			if (linkage instanceof FluentEntityMappingConfigurationSupport.EntityLinkage) {    // should always be true, see FluentEntityMappingConfigurationSupport.newLinkage(..)
				if (((EntityLinkage) linkage).isPrimaryKey()) {
					column.primaryKey();
				}
			} else {
				throw new NotImplementedException("Expect at least " + Reflections.toString(EntityLinkage.class)
						+ " but argument was " + Reflections.toString(linkage.getClass()));
			}
			
			return column;
		}
		
		@Override
		protected Map<IReversibleAccessor, Column> giveMappingFromInheritance() {
			// Here inheritance comes either from usual inheritance or (exclusive) from mapped super class (configured by super class)
			if (inheritanceInfo != null) {
				Map<IReversibleAccessor, Column> result = new HashMap<>();
				if (inheritanceInfo.inheritancePropertiesAreInTargetTable()) {
					// we copy mapped columns into target table so then can be selected, updated, etc.
					result.putAll(projectColumns((Map) inheritanceInfo.getPropertyToColumn(), getTargetTable(), (a, c) -> c.getName()));
				}
				// Adding identifier to result because result is given to a ClassMappingStrategy which expects identifier to be present in mapping
				// Note that idMapping can't be empty because when inheritance is defined, it is required that it also defines identification
				// (see determineIdentification(..))
				Duo<IReversibleAccessor, Column> idMapping = giveIdentifierMapping(inheritanceInfo.getIdMappingStrategy());
				result.put(idMapping.getLeft(), idMapping.getRight());
				return result;
			} else {
				// adding mapped super class properties (if present)
				return super.giveMappingFromInheritance();
			}
		}
		
		private Duo<IReversibleAccessor, Column> giveIdentifierMapping(IdMappingStrategy<? super C, ?> idMappingStrategy) {
			Duo<IReversibleAccessor, Column> result = new Duo<>();
			IdAccessor<? super C, ?> idAccessor = idMappingStrategy.getIdAccessor();
			if (!(idAccessor instanceof SinglePropertyIdAccessor)) {
				throw new NotYetSupportedOperationException();
			}
			IReversibleAccessor<? super C, ?> entityIdentifierAccessor = ((SinglePropertyIdAccessor<? super C, ?>) idAccessor).getIdAccessor();
			// Because IdAccessor is a single column one (see assertion above) we can get the only column composing the primary key
			Column primaryKey = ((SimpleIdentifierAssembler) idMappingStrategy.getIdentifierAssembler()).getColumn();
			Column projectedPrimarykey = getTargetTable().addColumn(primaryKey.getName(), primaryKey.getJavaType()).primaryKey();
			projectedPrimarykey.setAutoGenerated(primaryKey.isAutoGenerated());
			result.setLeft(entityIdentifierAccessor);
			result.setRight(projectedPrimarykey);
			return result;
		}
		
		/**
		 * Creates a {@link IEntityMappingStrategy} from elements given at construction time targeting given {@link Dialect} and {@link Table}.
		 * 
		 * @param dialect {@link Dialect} to be used to create {@link Column}s in table
		 * @param targetTable entity persistence {@link Table}
		 * @param identification identifier elements, expecting that primary key is present in target table
		 * @param <T> target table type
		 * @param <I> entity identifier type
		 * @return a new {@link IEntityMappingStrategy} that can persist entities onto target table
		 */
		public <T extends Table, I> IEntityMappingStrategy<C, I, T> buildClassMappingStrategy(Dialect dialect,
																							Table targetTable,
																							Duo<IReversibleAccessor<C, I>, IdentifierInsertionManager<C, I>> identification) {
			IReversibleAccessor<C, I> identifierAccessor = identification.getLeft();
			IdentifierInsertionManager<C, I> identifierInsertionManager = identification.getRight();
			
			Map<IReversibleAccessor, Column> columnMapping = super.build(dialect, targetTable);
			
			Column<T, I> primaryKey = (Column<T, I>) Iterables.first(targetTable.getPrimaryKey().getColumns());
			return new ClassMappingStrategy<C, I, T>(mappingConfiguration.getBeanType(), (T) targetTable,
					(Map) columnMapping,
					new SimpleIdMappingStrategy<>(identifierAccessor, identifierInsertionManager, new SimpleIdentifierAssembler<>(primaryKey)),
					entityFactory);
		}
	}
	
	/**
	 * Adds one-to-one and one-to-many graph node to the given root. Used for select by entity properties because without this it could not load
	 * while entity graph
	 * 
	 * @param configurationSupport entity mapping configuration which relations must be registered onto target
	 * @param target the node on which to add sub graph elements
	 * @param persistenceContext used as a per-entity {@link IEntityMappingStrategy} registry 
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
	
	/**
	 * Local class aimed at helping to determine identifier elements
	 * - gives identification elements
	 * - create primary key in table that owns identification (not in intermediary table on inheritance case)
	 * - asserts that identifier is given as properties mapping configuration (prerequisite of {@link ClassMappingStrategy})
	 *
	 * @param <C> entity type
	 * @param <I> identifier type
	 * @see #determineIdentification(Dialect, Table) 
	 */
	private static class IdentificationDeterminer<C, I> {
		
		private final Class<C> persistedClass;
		private final Iterator<EntityMappingConfiguration> inheritanceIterator;
		private final IdentifierPolicy identifierPolicy;
		private final ColumnNameProvider columnNameProvider;
		private final MethodReferenceCapturer methodSpy;
		
		/**
		 * Constructor with necessary elements for identification determination.
		 * Throws exception if inheritanceConfiguration and identifierAccessor are both not null.
		 * 
		 * @param persistedClass source class
		 * @param inheritanceIterator helper to go throught entity mapping configurations in hierarchy
		 * @param identifierPolicy source class identifier policy
		 * @param columnNameProvider {@link ColumnNameProvider} to be used to create primary key in table that owns identification
		 * @param methodSpy method reference help
		 */
		private IdentificationDeterminer(Class<C> persistedClass,
										 Iterator<EntityMappingConfiguration> inheritanceIterator,
										 IdentifierPolicy identifierPolicy,
										 ColumnNameProvider columnNameProvider,
										 MethodReferenceCapturer methodSpy) {
			this.persistedClass = persistedClass;
			this.inheritanceIterator = inheritanceIterator;
			this.identifierPolicy = identifierPolicy;
			this.columnNameProvider = columnNameProvider;
			this.methodSpy = methodSpy;
		}
		
		/**
		 * Looks for identifier as well as its inserter by going up through inheritance hierarchy.
		 * Creates necessary primary key column in table that owns identification.
		 *
		 * @return a couple that defines identification of the mapping
		 * @param dialect database dialect to deal with generated keys reading which differs from database vendor to database vendor
		 * @throws UnsupportedOperationException when identifiation was not found, because it doesn't make sense to have an entity without identification
		 */
		public <T extends Table<?>> Duo<IReversibleAccessor<C, I>, IdentifierInsertionManager<C, I>> determineIdentification(Dialect dialect, T table) {
			IReversibleAccessor<? super C, I> localIdentifierAccessor = null;
			EntityMappingConfiguration<? super C, I> pawn = null;
			while (inheritanceIterator.hasNext()) {
				pawn = inheritanceIterator.next();
				localIdentifierAccessor = pawn.getIdentifierAccessor();
			}
			if (localIdentifierAccessor == null) {
				// no ClassMappingStratey in hierarchy, so we can't get an identifier from it => impossible
				throw newMissingIdentificationException();
			} else {
				addPrimaryKey(table, localIdentifierAccessor, identifierPolicy == IdentifierPolicy.AFTER_INSERT);
				
				IdentifierInsertionManager<? super C, I> identifierInsertionManager = buildIdentifierInsertionManager(
						dialect,
						localIdentifierAccessor,
						pawn.getIdentifierPolicy(),
						table);
				
				return new Duo(localIdentifierAccessor, identifierInsertionManager);
			}
		}
		
		/**
		 * Adds primary key to given {@link Table}. Primary key information are took on internal configuration
		 *
		 * @param table the {@link Table} to add primary key to
		 * @param identifierAccessor the accessor to get primary key value
		 * @param isAutoGenerated indicates if primary key is auto-generated by database (valuable only for single-column primary key)
		 * @param <T> fine-grained table type
		 */
		private <T extends Table<?>> void addPrimaryKey(T table, IReversibleAccessor<? super C, I> identifierAccessor, boolean isAutoGenerated) {
			String primaryKeyColumnName = giveColumnName(identifierAccessor);
			MemberDefinition identifierDefinition = MemberDefinition.giveMemberDefinition(identifierAccessor);
			table.addColumn(primaryKeyColumnName, identifierDefinition.getMemberType()).primaryKey();
			if (isAutoGenerated) {
				table.getPrimaryKey().getColumns().forEach((Column c) -> c.setAutoGenerated(true));
			}
		}
		
		private UnsupportedOperationException newMissingIdentificationException() {
			SerializableBiFunction<ColumnOptions, IdentifierPolicy, ColumnOptions> identifierMethodReference = ColumnOptions::identifier;
			Method identifierSetter = this.methodSpy.findMethod(identifierMethodReference);
			return new UnsupportedOperationException("Identifier is not defined for " + Reflections.toString(persistedClass)
					+ ", please add one throught " + Reflections.toString(identifierSetter));
		}
		
		private IdentifierInsertionManager<? super C, I> buildIdentifierInsertionManager(Dialect dialect,
																						 IReversibleAccessor<? super C, I> identifierAccessor,
																						 IdentifierPolicy identifierPolicy,
																						 Table table) {
			IdentifierInsertionManager<? super C, I> identifierInsertionManager;
			MemberDefinition methodReference = MemberDefinition.giveMemberDefinition(identifierAccessor);
			Class<I> identifierType = methodReference.getMemberType();
			if (identifierPolicy == IdentifierPolicy.ALREADY_ASSIGNED) {
				if (Identified.class.isAssignableFrom(methodReference.getDeclaringClass()) && Identifier.class.isAssignableFrom(identifierType)) {
					identifierInsertionManager = new IdentifiedIdentifierManager<>(identifierType);
				} else {
					throw new NotYetSupportedOperationException(
							"Already-assigned identifier policy is only supported with entities that implement " + Reflections.toString(Identified.class));
				}
			} else if (identifierPolicy == IdentifierPolicy.AFTER_INSERT) {
				identifierInsertionManager = new JDBCGeneratedKeysIdentifierManager<>(
						new SinglePropertyIdAccessor<>(identifierAccessor),
						dialect.buildGeneratedKeysReader(Iterables.first(((Table<?>) table).getPrimaryKey().getColumns()).getName(), identifierType),
						identifierType
				);
			} else if (identifierPolicy instanceof ColumnOptions.BeforeInsertIdentifierPolicy) {
				identifierInsertionManager = new BeforeInsertIdentifierManager<>(
						new SinglePropertyIdAccessor<>(identifierAccessor), ((BeforeInsertIdentifierPolicy<I>) identifierPolicy).getIdentifierProvider(), identifierType);
			} else {
				throw new UnsupportedOperationException(identifierPolicy + " is not supported");
			}
			return identifierInsertionManager;
		}
		
		private String giveColumnName(IReversibleAccessor accessor) {
			return columnNameProvider.giveColumnName(MemberDefinition.giveMemberDefinition(accessor));
		}
		
	}
}
