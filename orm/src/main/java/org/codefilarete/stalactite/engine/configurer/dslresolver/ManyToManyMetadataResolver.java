package org.codefilarete.stalactite.engine.configurer.dslresolver;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.Set;
import java.util.function.Supplier;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorByMethod;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.DefaultReadWritePropertyAccessPoint;
import org.codefilarete.reflection.MutatorByMethod;
import org.codefilarete.reflection.PropertyMutator;
import org.codefilarete.reflection.ReadWriteAccessPoint;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.reflection.SerializableMutator;
import org.codefilarete.reflection.SerializablePropertyAccessor;
import org.codefilarete.reflection.SerializablePropertyMutator;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.naming.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.AssociationTableNamingStrategy.ReferencedColumnNames;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.NamingConfiguration;
import org.codefilarete.stalactite.engine.configurer.dslresolver.InheritanceConfigurationResolver.ResolvedConfiguration;
import org.codefilarete.stalactite.engine.configurer.dslresolver.MetadataSolvingCache.EntitySource;
import org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelation.MappedByConfiguration;
import org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelation.ShiftedMappedByConfiguration;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.IntermediaryRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedManyToManyRelation;
import org.codefilarete.stalactite.engine.runtime.AssociationTable;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationTable;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.bean.FieldIterator;
import org.codefilarete.tool.bean.InstanceFieldIterator;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.tool.Nullable.nullable;
import static org.codefilarete.tool.collection.Iterables.first;

/**
 * Resolves {@link ManyToManyRelation} DSL configurations into {@link ResolvedManyToManyRelation}
 * model instances. The process:
 * <ol>
 *   <li>Builds the target {@link EntitySource} via the inheritance resolver.</li>
 *   <li>Creates an {@link AssociationTable} (or {@link IndexedAssociationTable} when the relation is ordered).</li>
 *   <li>Wraps the table into an {@link IntermediaryRelationJoin}.</li>
 *   <li>Builds a {@link BeanRelationFixer} that populates both the source collection and, when the mapping is
 *       bidirectional, the reverse collection on the target side.</li>
 *   <li>Registers the resolved relation on the source {@link Entity}.</li>
 * </ol>
 *
 * @author Guillaume Mary
 */
public class ManyToManyMetadataResolver {
	
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	
	public ManyToManyMetadataResolver(Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
	}
	
	/**
	 * Entry point: resolves all many-to-many relations (direct and inset-embedded) declared within the given source.
	 *
	 * @return the set of target {@link EntitySource}s produced by the resolved relations, to be enqueued for further traversal
	 */
	<C, I> Set<EntitySource<?, ?>> resolve(EntitySource<C, I> source) {
		KeepOrderSet<EntitySource<?, ?>> targetEntities = new KeepOrderSet<>();
		// configuring many-to-manys owned by this entity
		source.getResolvedConfigurations().forEach(resolvedConfiguration -> {
			targetEntities.addAll(resolve(source.getEntity(), resolvedConfiguration.getMappingConfiguration()));
		});
		return targetEntities;
	}
	
	private <C, I> Set<EntitySource<?, ?>> resolve(Entity<C, I, ?> entity, EntityMappingConfiguration<C, I> mappingConfiguration) {
		KeepOrderSet<EntitySource<?, ?>> targetEntities = new KeepOrderSet<>();
		mappingConfiguration.getManyToManys().forEach(manyToMany -> {
			EntitySource<Object, Object> resolved = this.resolve(entity, manyToMany);
			targetEntities.add(resolved);
		});
		// treating relations embedded in insets
		mappingConfiguration.getPropertiesMapping().getInsets().forEach(inset -> {
			inset.getConfigurationProvider().getConfiguration().getManyToManys().forEach(manyToMany -> {
				EntitySource<Object, Object> resolved = this.resolve(entity, manyToMany.embedInto(inset.getAccessor(), inset.getEmbeddedClass()));
				targetEntities.add(resolved);
			});
		});
		return targetEntities;
	}
	
	/**
	 * Resolves a single many-to-many relation: determines the association table structure, builds the relation join and
	 * the {@link BeanRelationFixer}, and registers the resulting
	 * {@link ResolvedManyToManyRelation} on the source entity.
	 *
	 * @return the target {@link EntitySource}, ready to be enqueued for further (recursive) resolution
	 */
	<SRC, TRGT, S extends Collection<TRGT>, C2 extends Collection<SRC>, SRCID, TRGTID,
			SRCTABLE extends Table<SRCTABLE>, TRGTTABLE extends Table<TRGTTABLE>,
			ASSOCIATIONTABLE extends AssociationTable<ASSOCIATIONTABLE, SRCTABLE, TRGTTABLE, SRCID, TRGTID>>
	EntitySource<TRGT, TRGTID> resolve(Entity<SRC, SRCID, SRCTABLE> source, ManyToManyRelation<SRC, TRGT, TRGTID, S, C2> manyToMany) {
		
		EntitySource<TRGT, TRGTID> targetEntitySource = buildTargetEntity(manyToMany);
		NamingConfiguration namingConfiguration = first(targetEntitySource.getResolvedConfigurations()).getNamingConfiguration();
		Entity<TRGT, TRGTID, TRGTTABLE> targetEntity = targetEntitySource.getEntity();
		
		AccessorDefinition collectionAccessorDefinition = AccessorDefinition.giveDefinition(manyToMany.getCollectionAccessor());
		// We prefer the target entity type over the raw Collection member type for table/column naming, mirroring OneToMany behaviour
		AccessorDefinition accessorDefinitionForTableNaming = new AccessorDefinition(
				collectionAccessorDefinition.getDeclaringClass(),
				collectionAccessorDefinition.getName(),
				manyToMany.getTargetMappingConfiguration().getEntityType());
		
		Supplier<S> collectionFactory = manyToMany.getCollectionFactory();
		if (collectionFactory == null) {
			collectionFactory = Reflections.giveCollectionFactory((Class<S>) collectionAccessorDefinition.getMemberType());
		}
		
		PrimaryKey<SRCTABLE, SRCID> leftPrimaryKey = source.getTable().getPrimaryKey();
		PrimaryKey<TRGTTABLE, TRGTID> rightPrimaryKey = targetEntity.getTable().getPrimaryKey();
		
		AssociationTableNamingStrategy associationTableNamingStrategy = namingConfiguration.getAssociationTableNamingStrategy();
		ForeignKeyNamingStrategy foreignKeyNamingStrategy = namingConfiguration.getForeignKeyNamingStrategy();
		ColumnNamingStrategy indexColumnNamingStrategy = namingConfiguration.getIndexColumnNamingStrategy();
		
		// We don't create FK for table-per-class polymorphism because source columns would reference different tables
		// (one per concrete entity type) which databases do not allow
		boolean createOneSideForeignKey = !source.isTablePerClass() && !manyToMany.isSourceTablePerClassPolymorphic();
		boolean createManySideForeignKey = !targetEntity.isTablePerClass() && !manyToMany.isTargetTablePerClassPolymorphic();
		
		ReferencedColumnNames<SRCTABLE, TRGTTABLE> columnNames = associationTableNamingStrategy.giveColumnNames(
				accessorDefinitionForTableNaming,
				leftPrimaryKey,
				rightPrimaryKey);
		
		// Apply user-defined column names when provided
		if (manyToMany.getSourceJoinColumnName() != null) {
			columnNames.setLeftColumnName(first(leftPrimaryKey.getColumns()), manyToMany.getSourceJoinColumnName());
		}
		if (manyToMany.getTargetJoinColumnName() != null) {
			columnNames.setRightColumnName(first(rightPrimaryKey.getColumns()), manyToMany.getTargetJoinColumnName());
		}
		
		String associationTableName = nullable(manyToMany.getAssociationTableName())
				.getOr(() -> associationTableNamingStrategy.giveName(accessorDefinitionForTableNaming, leftPrimaryKey, rightPrimaryKey));
		
		IntermediaryRelationJoin<SRCTABLE, TRGTTABLE, ?, SRCID, TRGTID> join;
		if (manyToMany.isOrdered()) {
			String indexingColumnName = nullable(manyToMany.getIndexingColumnName())
					.getOr(() -> indexColumnNamingStrategy.giveName(accessorDefinitionForTableNaming));
			ASSOCIATIONTABLE associationTable = (ASSOCIATIONTABLE) new IndexedAssociationTable<>(
					leftPrimaryKey.getTable().getSchema(),
					associationTableName,
					leftPrimaryKey,
					rightPrimaryKey,
					columnNames,
					foreignKeyNamingStrategy,
					createOneSideForeignKey,
					createManySideForeignKey,
					indexingColumnName
			);
			join = new IntermediaryRelationJoin<>(associationTable);
		} else {
			ASSOCIATIONTABLE associationTable = (ASSOCIATIONTABLE) new AssociationTable<>(
					leftPrimaryKey.getTable().getSchema(),
					associationTableName,
					leftPrimaryKey,
					rightPrimaryKey,
					columnNames,
					foreignKeyNamingStrategy,
					createOneSideForeignKey,
					createManySideForeignKey
			);
			join = new IntermediaryRelationJoin<>(associationTable);
		}
		
		PropertyMutator<TRGT, SRC> reverseCombiner = buildReverseCombiner(manyToMany, source);
		
		BeanRelationFixer<SRC, TRGT> relationFixer;
		if (reverseCombiner == null) {
			// Unidirectional: only populate the source-side collection
			relationFixer = BeanRelationFixer.ofAdapter(
					manyToMany.getCollectionAccessor(), collectionFactory,
					(target, input, collection) -> collection.add(input));
		} else {
			// Bidirectional: populate the source-side collection and trigger the reverse combiner
			relationFixer = BeanRelationFixer.of(manyToMany.getCollectionAccessor(), collectionFactory, reverseCombiner);
		}
		
		ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, SRCTABLE, TRGTTABLE> entitiesLink =
				new ResolvedManyToManyRelation<>(
						targetEntity,
						manyToMany.getCollectionAccessor(),
						manyToMany.getRelationMode(),
						manyToMany.isFetchSeparately(),
						(IntermediaryRelationJoin) join,
						relationFixer,
						collectionFactory
				);
		source.addRelation(entitiesLink);
		
		return targetEntitySource;
	}
	
	/**
	 * Builds the target {@link EntitySource} by resolving the full inheritance hierarchy of the target entity.
	 */
	private <SRC, TRGT, TRGTID, S extends Collection<TRGT>, C2 extends Collection<SRC>> EntitySource<TRGT, TRGTID>
	buildTargetEntity(ManyToManyRelation<SRC, TRGT, TRGTID, S, C2> manyToMany) {
		InheritanceConfigurationResolver<TRGT, TRGTID> inheritanceConfigurationResolver = new InheritanceConfigurationResolver<>();
		KeepOrderSet<ResolvedConfiguration<?, TRGTID>> ancestorsConfigurations =
				inheritanceConfigurationResolver.resolveConfigurations(manyToMany.getTargetMappingConfiguration());
		
		InheritanceMetadataResolver<TRGT, TRGTID, ?> inheritanceMetadataResolver = new InheritanceMetadataResolver<>(dialect, connectionConfiguration);
		return inheritanceMetadataResolver.resolve(ancestorsConfigurations);
	}
	
	/**
	 * Builds the {@link PropertyMutator} used to propagate the source entity back to the target's reverse collection
	 * (i.e. the bidirectional side of the many-to-many), or {@code null} when the relation is unidirectional.
	 *
	 * <p>The logic mirrors {@link org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelationConfigurer}:
	 * <ol>
	 *   <li>Use the explicitly provided collection accessor ({@code getter}/{@code setter} references).</li>
	 *   <li>If absent, try to detect the reverse collection field by type inspection.</li>
	 *   <li>If a custom {@code reverseCombiner} is set, use it; otherwise default to {@link Collection#add}.</li>
	 *   <li>When the configuration is shifted (embedded relation), wrap the result with the shifter accessor.</li>
	 * </ol>
	 */
	private <SRC, TRGT, SRCID, C2 extends Collection<SRC>, S extends Collection<TRGT>, SRCTABLE extends Table<SRCTABLE>>
	PropertyMutator<TRGT, SRC> buildReverseCombiner(ManyToManyRelation<SRC, TRGT, ?, S, C2> manyToMany, Entity<SRC, SRCID, SRCTABLE> source) {
		MappedByConfiguration<SRC, TRGT, C2> mappedByConfiguration = manyToMany.getMappedByConfiguration();
		if (mappedByConfiguration.isEmpty()) {
			// relation is not bidirectional, and not even set by the reverse link, there's nothing to do
			return null;
		} else {
			ReadWritePropertyAccessPoint<TRGT, C2> collectionAccessor = buildReversePropertyAccessor(manyToMany.getMappedByConfiguration());
			if (collectionAccessor == null) {
				// No explicit accessor was configured: try to find a matching reverse field by type inspection
				Class<TRGT> targetClass = manyToMany.getTargetMappingConfiguration().getEntityType();
				FieldIterator targetFields = new InstanceFieldIterator(targetClass);
				Class<SRC> sourceEntityType = source.getEntityType();
				Field reverseField = Iterables.find(targetFields, field -> Collection.class.isAssignableFrom(field.getType())
						&& ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0].equals(sourceEntityType));
				if (reverseField != null) {
					Nullable<AccessorByMethod<TRGT, C2>> reverseGetterMethod = nullable(Accessors.accessorByMethod(reverseField));
					if (reverseGetterMethod.isPresent()) {
						collectionAccessor = new DefaultReadWritePropertyAccessPoint<>(reverseGetterMethod.get());
					} else {
						Nullable<MutatorByMethod<TRGT, C2>> reverseSetterMethod = nullable(Accessors.mutatorByMethod(reverseField));
						if (reverseSetterMethod.isPresent()) {
							collectionAccessor = new DefaultReadWritePropertyAccessPoint<>(reverseSetterMethod.get());
						}
					}
				} // else : relation is not bidirectional, or not a usual one, may be set by reverse link
			}
			
			Nullable<SerializablePropertyMutator<TRGT, SRC>> configuredCombiner = nullable(mappedByConfiguration.getReverseCombiner());
			PropertyMutator<TRGT, SRC> result;
			if (collectionAccessor == null) {
				if (configuredCombiner.isAbsent()) {
					return null;
				}
				// No collection to fill but a combiner was explicitly provided
				result = Accessors.readWriteAccessPoint(configuredCombiner.get());
			} else {
				Supplier<C2> reverseCollectionFactory = mappedByConfiguration.getReverseCollectionFactory();
				if (reverseCollectionFactory == null) {
					Class<C2> collectionType = AccessorDefinition.giveDefinition(collectionAccessor).getMemberType();
					reverseCollectionFactory = Reflections.giveCollectionFactory(collectionType);
				}
				ReadWriteAccessPoint<TRGT, C2> finalCollectionAccessor = collectionAccessor;
				SerializableMutator<TRGT, SRC> combiner = configuredCombiner.getOr((TRGT trgt, SRC src) -> {
					// Default combiner: add source to the target's reverse collection
					finalCollectionAccessor.get(trgt).add(src);
				});
				Supplier<C2> effectiveReverseCollectionFactory = reverseCollectionFactory;
				result = (TRGT trgt, SRC src) -> {
					// Lazily initialise the reverse collection if null
					if (finalCollectionAccessor.get(trgt) == null) {
						finalCollectionAccessor.set(trgt, effectiveReverseCollectionFactory.get());
					}
					combiner.set(trgt, src);
				};
			}
			
			// When the relation is embedded, the "src" passed to the combiner is the root entity, but the reverse setter
			// expects the embedded type; the shifter accessor extracts the embedded instance from the root.
			if (mappedByConfiguration instanceof ShiftedMappedByConfiguration) {
				Accessor shifter = ((ShiftedMappedByConfiguration) mappedByConfiguration).getShifter();
				return (trgt, src) -> {
					SRC src1 = (SRC) shifter.get(src);
					result.set(trgt, src1);
				};
			} else {
				return result;
			}
		}
	}
	
	/**
	 * Builds a {@link ReadWritePropertyAccessPoint} for the reverse collection on the target side from the configured
	 * getter/setter references. Returns {@code null} when neither a getter nor a setter was specified.
	 */
	@javax.annotation.Nullable
	private <SRC, TRGT, C2 extends Collection<SRC>> ReadWritePropertyAccessPoint<TRGT, C2> buildReversePropertyAccessor(MappedByConfiguration<SRC, TRGT, C2> mappedByConfiguration) {
		Nullable<SerializablePropertyAccessor<TRGT, C2>> getterReference = nullable(mappedByConfiguration.getReverseCollectionAccessor());
		Nullable<SerializablePropertyMutator<TRGT, C2>> setterReference = nullable(mappedByConfiguration.getReverseCollectionMutator());
		if (getterReference.isAbsent() && setterReference.isAbsent()) {
			return null;
		} else if (getterReference.isPresent() && setterReference.isPresent()) {
			// Both are provided: honour both method references
			return new DefaultReadWritePropertyAccessPoint<>(getterReference.get(), setterReference.get());
		} else if (getterReference.isPresent()) {
			// Only getter: derive setter from the same method reference
			return Accessors.readWriteAccessPoint(getterReference.get());
		} else {
			// Only setter: derive getter from the same method reference
			return Accessors.readWriteAccessPoint(setterReference.get());
		}
	}
}

