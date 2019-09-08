package org.gama.stalactite.persistence.engine;

import javax.annotation.Nonnull;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;

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
import org.gama.stalactite.persistence.mapping.IdAccessor;
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
 */
class EntityMappingBuilder<C, I> extends AbstractEntityMappingBuilder<C, I> {
	
	EntityMappingBuilder(EntityMappingConfiguration<C, I> entityBuilderSupport, MethodReferenceCapturer methodSpy) {
		super(entityBuilderSupport, methodSpy);
	}
	
	/**
	 * Build a {@link Persister} from the given configuration at contruction time.
	 * {@link Table}s and columns will be created to fulfill the requirements needed by the configuration.
	 * 
	 * @param persistenceContext the {@link PersistenceContext} in which the resulting {@link Persister} will be put, also needed for Dialect purpose 
	 * @return the built {@link Persister}
	 */
	public Persister<C, I, Table> build(PersistenceContext persistenceContext) {
		return build(persistenceContext, null);
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
	protected <T extends Table<?>> JoinedTablesPersister<C, I, T> doBuild(PersistenceContext persistenceContext, @javax.annotation.Nullable T table) {
		// Very first thing, determine identifier management and check some configuration
		// Table must be created before giving it to further methods because it is mandatory for them
		if (table == null) {
			table = (T) nullable(giveTableUsedInMapping()).getOr(() -> new Table(configurationSupport.getTableNamingStrategy().giveName(configurationSupport.getPersistedClass())));
		}
		
		IdentificationDeterminer<C, I> identificationDeterminer = new IdentificationDeterminer<>(
				configurationSupport.getInheritanceConfiguration(),
				configurationSupport.getIdentifierAccessor(),
				configurationSupport.getIdentifierPolicy(),
				configurationSupport.getPersistedClass(),
				columnNameProvider,
				configurationSupport.getPropertiesMapping().getPropertiesMapping(),
				this.methodSpy);
		Duo<IReversibleAccessor<C, I>, IdentifierInsertionManager<C, I>> identification = identificationDeterminer.determineIdentification(persistenceContext.getDialect(), table);
		
		ClassMappingStrategy<? super C, I, Table> inheritanceMappingStrategy = null;
		if (configurationSupport.getInheritanceConfiguration() != null) {
			if (configurationSupport.isJoinTable()) {
				// Note that generics can't be used because "<? super C> can't be instantiated directly"
				inheritanceMappingStrategy = new JoinedTablesEntityMappingBuilder(configurationSupport, methodSpy)
						.build(persistenceContext, table)
						.getMappingStrategy();
			} else {
				inheritanceMappingStrategy = new EntityMappingBuilder(configurationSupport.getInheritanceConfiguration(), methodSpy)
						.build(persistenceContext, table)
						.getMappingStrategy();
			}
		}
		
		EntityDecoratedEmbeddableMappingBuilder<C> embeddableMappingBuilder = new EntityDecoratedEmbeddableMappingBuilder<>(
				configurationSupport.getPropertiesMapping(),
				columnNameProvider, 
				configurationSupport.getOneToOnes(),
				inheritanceMappingStrategy);
		
		JoinedTablesPersister<C, I, T> result = configureRelations(persistenceContext,
				embeddableMappingBuilder.buildClassMappingStrategy(persistenceContext.getDialect(), table, identification));
		
		handleVersioningStrategy(result);
		
		return result;
	}
	
	private Table giveTableUsedInMapping() {
		Set<Table> usedTablesInMapping = Iterables.collect(configurationSupport.getPropertiesMapping().getPropertiesMapping(),
				linkage -> linkage instanceof EntityLinkageByColumn,
				linkage -> ((EntityLinkageByColumn) linkage).getColumn().getTable(),
				HashSet::new);
		switch (usedTablesInMapping.size()) {
			case 0:
				return null;
			case 1:
				return Iterables.first(usedTablesInMapping);
			default:
				throw new MappingConfigurationException("Different tables found in columns given as parameter of methods mapping : " + usedTablesInMapping);
		}
	}
	
	/**
	 * Configures relations of our {@link EntityMappingConfiguration} onto the given {@link ClassMappingStrategy} previously built.
	 * 
	 * @param persistenceContext our current {@link PersistenceContext}
	 * @param mainMappingStrategy the mapping of simple properties
	 * @param <T> table type
	 * @return a {@link JoinedTablesPersister} that allow to persist the entity with its relations
	 */
	<T extends Table> JoinedTablesPersister<C, I, T> configureRelations(PersistenceContext persistenceContext, ClassMappingStrategy<C, I, T> mainMappingStrategy) {
		// Please note that we could have created a simple Persister for cases of no one-to-one neither one-to-many relations
		// because in such cases no join is required, but it would have introduced inconsistent return type depending on configuration
		// which is hard to manage by JoinedTablesEntityMappingBuilder that uses this method
		JoinedTablesPersister<C, I, T> result = new JoinedTablesPersister<>(persistenceContext, mainMappingStrategy);
		// don't forget to register this new persister, it's usefull for schema deployment
		Persister<C, Object, T> existingPersister = persistenceContext.getPersister(result.getMappingStrategy().getClassToPersist());
		if (existingPersister == null) {
			persistenceContext.addPersister(result);
		} else {
			ClassMappingStrategy<C, Object, T> existingMappingStrategy = existingPersister.getMappingStrategy();
			
			BiPredicate<ClassMappingStrategy, ClassMappingStrategy> mappingConfigurationComparator = Predicates.and(
					ClassMappingStrategy::getTargetTable,
					ClassMappingStrategy::getPropertyToColumn,
					ClassMappingStrategy::getVersionedKeys,
					ClassMappingStrategy::getSelectableColumns,
					ClassMappingStrategy::getUpdatableColumns,
					ClassMappingStrategy::getInsertableColumns
			);
			if (!mappingConfigurationComparator.test(existingMappingStrategy, mainMappingStrategy)) {
				throw new IllegalArgumentException("Persister already exists for " + Reflections.toString(result.getMappingStrategy().getClassToPersist()));
			}
		}
		
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
		registerRelations(persistenceContext, result.getCriteriaSupport().getRootConfiguration(), configurationSupport);
		
		return result;
	}
	
	private <T extends Table> void handleVersioningStrategy(JoinedTablesPersister<C, I, T> result) {
		Nullable<VersioningStrategy> versioningStrategy = nullable(this.configurationSupport.getOptimisticLockOption());
		if (versioningStrategy.isPresent()) {
			// we have to declare it to the mapping strategy. To do that we must find the versionning column
			Column column = result.getMappingStrategy().getMainMappingStrategy().getPropertyToColumn().get(this.configurationSupport.getOptimisticLockOption().getVersionAccessor());
			result.getMappingStrategy().addVersionedColumn(this.configurationSupport.getOptimisticLockOption().getVersionAccessor(), column);
			// and don't forget to give it to the workers !
			result.getUpdateExecutor().setVersioningStrategy(versioningStrategy.get());
			result.getInsertExecutor().setVersioningStrategy(versioningStrategy.get());
		}
	}
	
	/**
	 * Enhanced version of {@link EmbeddableMappingBuilder} to add entity features such as id mapping and inheritance 
	 * 
	 * @param <C>
	 * @see #buildClassMappingStrategy(Dialect, Table, Duo) 
	 */
	static class EntityDecoratedEmbeddableMappingBuilder<C> extends EmbeddableMappingBuilder<C> {
		
		private final List<CascadeOne<C, ?, ?>> oneToOnes;
		private final ClassMappingStrategy<? super C, ?, Table> inheritanceMappingStrategy;
		/** Keep track of oneToOne properties to be removed of direct mapping */
		private final ValueAccessPointSet oneToOnePropertiesOwnedByReverseSide;
		
		EntityDecoratedEmbeddableMappingBuilder(EmbeddableMappingConfiguration<C> propertiesMapping,
													   ColumnNameProvider columnNameProvider,
													   List<CascadeOne<C, ?, ?>> oneToOnes,
													   @javax.annotation.Nullable ClassMappingStrategy<? super C, ?, Table> inheritanceMappingStrategy) {
			super(propertiesMapping, columnNameProvider);
			this.oneToOnes = oneToOnes;
			this.inheritanceMappingStrategy = inheritanceMappingStrategy;
			// CascadeOne.getTargetProvider() returns a method reference that can't be compared to PropertyAccessor (of Linkage.getAccessor
			// in mappingConfiguration.getPropertiesMapping() keys) so we use a ValueAccessPoint to do it 
			this.oneToOnePropertiesOwnedByReverseSide = Iterables.collect(
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
		protected void ensureColumnBinding(Linkage linkage, Column column) {
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
		 * Same as {@link #ensureColumnBinding(Linkage, Column)} but will do nothing for one-to-one relation because it would try to find a
		 * binder for an entity (which may exist if user declare it) which can hardly/never implement correctly the reading process because
		 * target entity should be created with a complete information set, which is not possible throught {@link org.gama.stalactite.sql.binder.ResultSetReader#get(ResultSet, String)}.
		 * Entity will be crate correctly by {@link org.gama.stalactite.persistence.engine.cascade.StrategyJoinsRowTransformer}
		 * 
		 * @param linkage
		 * @param column
		 */
		private void ensureColumnBindingExceptOneToOne(Linkage linkage, Column column) {
			// we should not check for column binding of owner because its column will be manage by CascadeOneConfigurer. Without this, we get a
			// BindingException at each OneToOne
			// NB: we can use equals(..) here because Linkage was created from oneToOne information, no need to create a MemberDefinition for comparison
			if (!Iterables.contains(this.oneToOnes.iterator(), cascadeOne -> cascadeOne.getTargetProvider().equals(linkage.getAccessor()))) {
				super.ensureColumnBinding(linkage, column);
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
		protected Map<IReversibleAccessor, Column> buildMappingFromInheritance() {
			// adding mapped super class properties (if present)
			Map<IReversibleAccessor, Column> result = super.buildMappingFromInheritance();
			// adding inherited class properties (if present)
			if (inheritanceMappingStrategy != null) {
				result.putAll(collectMapping(inheritanceMappingStrategy, super.getTargetTable(), (a, c) -> c.getName()));
				// Dealing with identifier
				Duo<IReversibleAccessor, Column> idMapping = collectIdMapping();
				result.put(idMapping.getLeft(), idMapping.getRight());
			}
			return result;
		}
		
		private Duo<IReversibleAccessor, Column> collectIdMapping() {
			Duo<IReversibleAccessor, Column> result = new Duo<>();
			IdAccessor<? super C, ?> idAccessor = inheritanceMappingStrategy.getIdMappingStrategy().getIdAccessor();
			if (!(idAccessor instanceof SinglePropertyIdAccessor)) {
				throw new NotYetSupportedOperationException();
			}
			IReversibleAccessor<? super C, ?> entityIdentifierAccessor = ((SinglePropertyIdAccessor<? super C, ?>) idAccessor).getIdAccessor();
			// Because IdAccessor is a single column one (see assertion above) we can get the only column composing the primary key
			Column primaryKey = ((SimpleIdentifierAssembler) inheritanceMappingStrategy.getIdMappingStrategy().getIdentifierAssembler()).getColumn();
			Column projectedPrimarykey = super.getTargetTable().addColumn(primaryKey.getName(), primaryKey.getJavaType()).primaryKey();
			projectedPrimarykey.setAutoGenerated(primaryKey.isAutoGenerated());
			result.setLeft(entityIdentifierAccessor);
			result.setRight(projectedPrimarykey);
			return result;
		}
		
		private <T extends Table, I> ClassMappingStrategy<C, I, T> buildClassMappingStrategy(Dialect dialect,
																							 Table targetTable,
																							 Duo<IReversibleAccessor<C, I>, IdentifierInsertionManager<C, I>> identification) {
			IReversibleAccessor<C, I> identifierAccessor = identification.getLeft();
			IdentifierInsertionManager<C, I> identifierInsertionManager = identification.getRight();
			
			Map<IReversibleAccessor, Column> columnMapping = super.build(dialect, targetTable);
			
			Column primaryKey = columnMapping.get(identifierAccessor);
			if (primaryKey == null) {
				// since this instance manage table_per_class or single_table inheritance, primary key is expected to be on target table 
				throw new UnsupportedOperationException("No matching primary key columns for identifier "
						+ MemberDefinition.toString(identifierAccessor) + " in table " + targetTable.getName());
			} else {
				List<IReversibleAccessor> identifierAccessors = collect(columnMapping.entrySet(), e -> e.getValue().isPrimaryKey(), Entry::getKey, ArrayList::new);
				if (identifierAccessors.size() > 1) {
					throw new NotYetSupportedOperationException("Composed primary key is not yet supported");
				}
			}
			
			return new ClassMappingStrategy<C, I, T>(mappingConfiguration.getClassToPersist(), (T) targetTable,
					(Map) columnMapping, identifierAccessor, identifierInsertionManager);
		}
	}
	
	/**
	 * Adds one-to-one and one-to-many graph node to the given root. Used for select by entity properties because without this it could not load
	 * while entity graph
	 * 
	 * @param persistenceContext used as a per-entity {@link ClassMappingStrategy} registry 
	 * @param root the node on which to add sub graph elements
	 * @param configurationSupport current entity mapping configuration (that contains the root)
	 */
	private void registerRelations(PersistenceContext persistenceContext, EntityGraphNode root, EntityMappingConfiguration configurationSupport) {
		List<CascadeMany> oneToManys = configurationSupport.getOneToManys();
		oneToManys.forEach((CascadeMany cascadeMany) -> {
			EntityGraphNode entityGraphNode = root.registerRelation(cascadeMany.getCollectionProvider(),
					persistenceContext.getPersister(cascadeMany.getTargetMappingConfiguration().getPersistedClass()).getMappingStrategy());
			registerRelations(persistenceContext, entityGraphNode, cascadeMany.getTargetMappingConfiguration());
		});
		List<CascadeOne> oneToOnes = configurationSupport.getOneToOnes();
		oneToOnes.forEach((CascadeOne cascadeOne) -> {
			EntityGraphNode entityGraphNode = root.registerRelation(cascadeOne.getTargetProvider(),
					persistenceContext.getPersister(cascadeOne.getTargetMappingConfiguration().getPersistedClass()).getMappingStrategy());
			registerRelations(persistenceContext, entityGraphNode, cascadeOne.getTargetMappingConfiguration());
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
	 *
	 * @param <C> entity type
	 * @param <I> identifier type
	 */
	private static class IdentificationDeterminer<C, I> {
		
		private final EntityMappingConfiguration<? super C, I> inheritanceConfiguration;
		private final IReversibleAccessor<C, I> identifierAccessor;
		private final IdentifierPolicy identifierPolicy;
		private final Class<C> persistedClass;
		private final ColumnNameProvider columnNameProvider;
		private final List<Linkage> propertiesMapping;
		private final MethodReferenceCapturer methodSpy;
		
		public IdentificationDeterminer(EntityMappingConfiguration<? super C, I> inheritanceConfiguration,
										IReversibleAccessor<C, I> identifierAccessor,
										IdentifierPolicy identifierPolicy, Class<C> persistedClass,
										ColumnNameProvider columnNameProvider,
										List<Linkage> propertiesMapping,
										MethodReferenceCapturer methodSpy) {
			this.inheritanceConfiguration = inheritanceConfiguration;
			this.identifierAccessor = identifierAccessor;
			this.identifierPolicy = identifierPolicy;
			this.persistedClass = persistedClass;
			this.columnNameProvider = columnNameProvider;
			this.propertiesMapping = propertiesMapping;
			this.methodSpy = methodSpy;
		}
		
		/**
		 * Looks for identifier as well as its inserter, throwing exception if configuration is not coherent
		 *
		 * @return a couple that defines identification of the mapping
		 * @param dialect database dialect to deal with generated keys reading which differs from database vendor to database vendor
		 */
		public <T extends Table<?>> Duo<IReversibleAccessor<C, I>, IdentifierInsertionManager<C, I>> determineIdentification(Dialect dialect, T table) {
			if (identifierAccessor != null && inheritanceConfiguration != null) {
				throw new MappingConfigurationException("Defining an identifier while inheritance is used is not supported");
			}
			
			IReversibleAccessor<? super C, I> localIdentifierAccessor = null;
			IdentifierInsertionManager<? super C, I> identifierInsertionManager = null;
			if (inheritanceConfiguration == null) {
				localIdentifierAccessor = this.identifierAccessor;
				if (localIdentifierAccessor == null) {
					// no ClassMappingStratey in hierarchy, so we can't get an identifier from it => impossible
					throw newMissingIdentificationException();
				}
				
				addPrimaryKey(table);
				
				identifierInsertionManager = buildIdentifierInsertionManager(dialect, localIdentifierAccessor, identifierPolicy, propertiesMapping);
			} else {
				// mapping with inheritance : identifier is expected to be defined on it
				// at least one parent ClassMappingStrategy exists, it necessarily defines an identifier : we stop at the very first one
				EntityMappingConfiguration<? super C, I> pawn = inheritanceConfiguration;
				while (localIdentifierAccessor == null && pawn != null) {
					localIdentifierAccessor = pawn.getIdentifierAccessor();
					if (localIdentifierAccessor != null) {
						identifierInsertionManager = buildIdentifierInsertionManager(dialect, localIdentifierAccessor, pawn.getIdentifierPolicy(),
								pawn.getPropertiesMapping().getPropertiesMapping());
					}
					pawn = pawn.getInheritanceConfiguration();
				}
				if (localIdentifierAccessor == null) {
					// no ClassMappingStratey in hierarchy, so we can't get an identifier from it => impossible
					throw newMissingIdentificationException();
				}
			}
			
			return new Duo(localIdentifierAccessor, identifierInsertionManager);
		}
		
		/**
		 * Adds primary key to given {@link Table}. Primary key information are took on internal configuration
		 *
		 * @param table the {@link Table} to add primary key to
		 * @param <T> fine-grained table type
		 */
		private <T extends Table<?>> void addPrimaryKey(T table) {
			String primaryKeyColumnName = giveColumnName(identifierAccessor);
			MemberDefinition identifierDefinition = MemberDefinition.giveMemberDefinition(identifierAccessor);
			table.addColumn(primaryKeyColumnName, identifierDefinition.getMemberType()).primaryKey();
			if (identifierPolicy == IdentifierPolicy.AFTER_INSERT) {
				table.getPrimaryKey().getColumns().forEach((Column c) -> c.setAutoGenerated(true));
			}
		}
		
		private UnsupportedOperationException newMissingIdentificationException() {
			SerializableBiFunction<ColumnOptions, IdentifierPolicy, ColumnOptions> identifierMethodReference = ColumnOptions::identifier;
			Method identifierSetter = this.methodSpy.findMethod(identifierMethodReference);
			return new UnsupportedOperationException("Identifier is not defined for " + Reflections.toString(persistedClass)
					+ ", please add one throught " + Reflections.toString(identifierSetter));
		}
		
		private IdentifierInsertionManager<? super C, I> buildIdentifierInsertionManager(Dialect dialect, IReversibleAccessor<? super C, I> identifierAccessor,
																						 IdentifierPolicy identifierPolicy, List<Linkage> propertiesMapping) {
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
				Linkage identifierLinkage = assertIdentifierIsDefined(identifierAccessor, propertiesMapping);
				identifierInsertionManager = new JDBCGeneratedKeysIdentifierManager<>(
						new SinglePropertyIdAccessor<>(identifierAccessor),
						dialect.buildGeneratedKeysReader(giveColumnName(identifierLinkage.getAccessor()), identifierType),
						identifierType
				);
			} else if (identifierPolicy instanceof ColumnOptions.BeforeInsertIdentifierPolicy) {
				// NB: we can use equals(..) here because Linkage was created from accessor information, no need to create a MemberDefinition for comparison
				Linkage identifierLinkage = assertIdentifierIsDefined(identifierAccessor, propertiesMapping);
				identifierInsertionManager = new BeforeInsertIdentifierManager<>(
						new SinglePropertyIdAccessor<>(identifierAccessor), ((BeforeInsertIdentifierPolicy<I>) identifierPolicy).getIdentifierProvider(), identifierType);
			} else {
				throw new UnsupportedOperationException(identifierPolicy + " is not supported");
			}
			return identifierInsertionManager;
		}
		
		@Nonnull
		private Linkage assertIdentifierIsDefined(IReversibleAccessor<? super C, I> identifierAccessor, List<Linkage> propertiesMapping) {
			// NB: we can use equals(..) here because Linkage was created from accessor information, no need to create a MemberDefinition for 
			// comparison
			Optional<Linkage> identifierLinkage = propertiesMapping.stream().filter(
					linkage -> linkage.getAccessor().equals(identifierAccessor)).findFirst();
			if (!identifierLinkage.isPresent()) {
				throw new IllegalStateException("Identifier accessor expected to be found in mapping but wasn't. Identification cannot be built.");
			}
			return identifierLinkage.get();
		}
		
		private String giveColumnName(IReversibleAccessor accessor) {
			return columnNameProvider.giveColumnName(MemberDefinition.giveMemberDefinition(accessor));
		}
		
	}
}
