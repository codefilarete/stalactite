package org.codefilarete.stalactite.engine.configurer.dslresolver;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.Set;
import java.util.function.Supplier;

import org.codefilarete.reflection.AccessorByMethod;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.DefaultReadWritePropertyAccessPoint;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.MutatorByMethod;
import org.codefilarete.reflection.ReadWriteAccessPoint;
import org.codefilarete.reflection.SerializableMutator;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.NamingConfiguration;
import org.codefilarete.stalactite.engine.configurer.dslresolver.InheritanceConfigurationResolver.ResolvedConfiguration;
import org.codefilarete.stalactite.engine.configurer.dslresolver.MetadataSolvingCache.EntitySource;
import org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.manytoone.ManyToOneRelation;
import org.codefilarete.stalactite.engine.configurer.manytoone.ManyToOneRelation.MappedByConfiguration;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.IntermediaryRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedManyToOneRelation;
import org.codefilarete.stalactite.engine.runtime.AssociationTable;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationTable;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
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
public class ManyToOneMetadataResolver {
	
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	
	public ManyToOneMetadataResolver(Dialect dialect, ConnectionConfiguration connectionConfiguration) {
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
		mappingConfiguration.getManyToOnes().forEach(manyToOne -> {
			EntitySource<Object, Object> resolved = this.resolve(entity, manyToOne);
			targetEntities.add(resolved);
		});
		// treating relations embedded in insets
		mappingConfiguration.getPropertiesMapping().getInsets().forEach(inset -> {
			inset.getConfigurationProvider().getConfiguration().getManyToOnes().forEach(manyToOne -> {
				EntitySource<Object, Object> resolved = this.resolve(entity, manyToOne.embedInto(inset.getAccessor()));
				targetEntities.add(resolved);
			});
		});
		return targetEntities;
	}
	
	/**
	 * Resolves a single many-to-one relation.
	 *
	 * @return the target {@link EntitySource}, ready to be enqueued for further (recursive) resolution
	 */
	<SRC, TRGT, S extends Collection<SRC>, SRCID, TRGTID, SRCTABLE extends Table<SRCTABLE>, TRGTTABLE extends Table<TRGTTABLE>>
	EntitySource<TRGT, TRGTID> resolve(Entity<SRC, SRCID, SRCTABLE> source, ManyToOneRelation<SRC, TRGT, TRGTID, S> manyToOne) {
		
		EntitySource<TRGT, TRGTID> targetEntitySource = buildTargetEntity(manyToOne);
		NamingConfiguration namingConfiguration = first(targetEntitySource.getResolvedConfigurations()).getNamingConfiguration();
		Entity<TRGT, TRGTID, TRGTTABLE> targetEntity = targetEntitySource.getEntity();
		
		PrimaryKey<TRGTTABLE, TRGTID> rightPrimaryKey = targetEntity.getTable().getPrimaryKey();
		
		ForeignKeyNamingStrategy foreignKeyNamingStrategy = namingConfiguration.getForeignKeyNamingStrategy();
		
		KeyMapping<SRCTABLE, TRGTTABLE, TRGTID> foreignKey = determineForeignKeyColumns(manyToOne, rightPrimaryKey, source.getTable(), namingConfiguration.getJoinColumnNamingStrategy(), foreignKeyNamingStrategy);
		
		DirectRelationJoin<SRCTABLE, TRGTTABLE, TRGTID> join = new DirectRelationJoin<>(foreignKey, foreignKey.getReferencedKey());
		
		BeanRelationFixer<SRC, TRGT> relationFixer = determineRelationFixer(manyToOne, source.getEntityType(), targetEntity.getEntityType());
		
		ResolvedManyToOneRelation<SRC, TRGT, TRGTID, SRCTABLE, TRGTTABLE> entitiesLink =
				new ResolvedManyToOneRelation<>(
						targetEntity,
						manyToOne.getTargetProvider(),
						manyToOne.getRelationMode(),
						manyToOne.isFetchSeparately(),
						join,
						relationFixer,
						manyToOne.isNullable()
				);
		source.addRelation(entitiesLink);
		
		return targetEntitySource;
	}
	
	/**
	 * Builds the target {@link EntitySource} by resolving the full inheritance hierarchy of the target entity.
	 */
	private <SRC, TRGT, TRGTID, S extends Collection<SRC>> EntitySource<TRGT, TRGTID>
	buildTargetEntity(ManyToOneRelation<SRC, TRGT, TRGTID, S> manyToOne) {
		InheritanceConfigurationResolver<TRGT, TRGTID> inheritanceConfigurationResolver = new InheritanceConfigurationResolver<>();
		KeepOrderSet<ResolvedConfiguration<?, TRGTID>> ancestorsConfigurations =
				inheritanceConfigurationResolver.resolveConfigurations(manyToOne.getTargetMappingConfiguration());
		
		InheritanceMetadataResolver<TRGT, TRGTID, ?> inheritanceMetadataResolver = new InheritanceMetadataResolver<>(dialect, connectionConfiguration);
		return inheritanceMetadataResolver.resolve(ancestorsConfigurations);
	}
	
	protected <SRC, TRGT, TRGTID, S extends Collection<SRC>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	KeyMapping<LEFTTABLE, RIGHTTABLE, TRGTID> determineForeignKeyColumns(ManyToOneRelation<SRC, TRGT, TRGTID, S> manyToOne,
	                                                                     PrimaryKey<RIGHTTABLE, TRGTID> rightPrimaryKey,
	                                                                     LEFTTABLE leftTable,
	                                                                     JoinColumnNamingStrategy joinColumnNamingStrategy,
	                                                                     ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		
		Column<LEFTTABLE, TRGTID> providedColumn = (Column<LEFTTABLE, TRGTID>) manyToOne.getOwningColumn();
		// small check for incongruous reverse column definition: it can't be possible when key is composite
		if (providedColumn != null && rightPrimaryKey.isComposed()) {
			throw new UnsupportedOperationException("Can't map composite primary key " + rightPrimaryKey + " on single foreign key : " + providedColumn);
		}
		
		// priority 1: take user definition of the owning column
		Key.KeyBuilder<LEFTTABLE, TRGTID> leftKeyBuilder = Key.from(leftTable);
		if (providedColumn == null) {
			String reverseColumnName = manyToOne.getColumnName();
			if (reverseColumnName != null) {
				Column<RIGHTTABLE, TRGTID> rightPKColumn = (Column<RIGHTTABLE, TRGTID>) first(rightPrimaryKey.getColumns());
				providedColumn = leftTable.addColumn(reverseColumnName, rightPKColumn.getJavaType());
				leftKeyBuilder.addColumn(providedColumn);
			}
		} else {
			leftKeyBuilder.addColumn(providedColumn);
		}
		
		// priority 2: user didn't define the owning column, but we can guess it from the target accessor
		if (providedColumn == null) {
			AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(manyToOne.getTargetProvider());
			rightPrimaryKey.getColumns().forEach(pkColumn -> {
				String effectiveLeftColumnName = joinColumnNamingStrategy.giveName(accessorDefinition, pkColumn);
				Column<LEFTTABLE, ?> column = leftTable.addColumn(effectiveLeftColumnName, pkColumn.getJavaType());
				leftKeyBuilder.addColumn(column);
			});
		}
		
		Key<LEFTTABLE, TRGTID> leftKey = leftKeyBuilder.build();
		
		// According to the nullable option, we specify the ddl schema option
		leftKey.getColumns().forEach(c -> ((Column) c).nullable(manyToOne.isNullable()));
		
		
		if (manyToOne.isTargetTablePerClassPolymorphic()) {
			// We don't create FK for table-per-class polymorphism because source columns would reference different tables
			// (one per concrete entity type) which databases do not allow
			return new KeyMapping<>(leftKey, rightPrimaryKey);
		} else {
			String foreignKeyName = foreignKeyNamingStrategy.giveName(leftKey, rightPrimaryKey);
			return leftTable.addForeignKey(foreignKeyName, leftKey, rightPrimaryKey);
		}
	}
	
	protected <SRC, TRGT, S extends Collection<SRC>>
	BeanRelationFixer<SRC, TRGT> determineRelationFixer(ManyToOneRelation<SRC, TRGT, ?, S> manyToOneRelation, Class<SRC> sourceEntityType, Class<TRGT> targetEntityType) {
		Mutator<SRC, TRGT> targetSetter = manyToOneRelation.getTargetProvider();
		SerializableMutator<TRGT, SRC> reverseCombiner = buildReverseCombiner(manyToOneRelation, sourceEntityType, targetEntityType);
		
		if (reverseCombiner == null) {
			return BeanRelationFixer.of(targetSetter);
		} else {
			return (target, input) -> {
				targetSetter.set(target, input);
				reverseCombiner.set(input, target);
			};
		}
	}
	
	/**
	 * Build the combiner between target entities and source ones.
	 *
	 * @param targetEntityType target entity type, provided to look up for reverse property if no sufficient info was given
	 * @return null if no information was provided about the reverse side (no bidirectionality)
	 */
	private <SRC, TRGT, S extends Collection<SRC>>
	SerializableMutator<TRGT, SRC> buildReverseCombiner(ManyToOneRelation<SRC, TRGT, ?, S> manyToOneRelation, Class<SRC> sourceEntityType, Class<TRGT> targetEntityType) {
		MappedByConfiguration<SRC, TRGT, S> mappedByConfiguration = manyToOneRelation.getMappedByConfiguration();
		if (mappedByConfiguration.isEmpty()) {
			// relation is not bidirectional, and not even set by the reverse link, there's nothing to do
			return null;
		} else {
			ReadWriteAccessPoint<TRGT, S> collectionAccessor = manyToOneRelation.buildReversePropertyAccessor();
			if (collectionAccessor == null) {
				// since some reverse info has been done but not the collection accessor, we try to find the matching property by type
				FieldIterator targetFields = new InstanceFieldIterator(targetEntityType);
				Field reverseField = Iterables.find(targetFields, field -> Collection.class.isAssignableFrom(field.getType())
						&& ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0].equals(sourceEntityType));
				if (reverseField != null) {
					Nullable<AccessorByMethod<TRGT, S>> reverseGetterMethod = nullable(Accessors.accessorByMethod(reverseField));
					if (reverseGetterMethod.isPresent()) {
						collectionAccessor = new DefaultReadWritePropertyAccessPoint<>(reverseGetterMethod.get());
					} else {
						Nullable<MutatorByMethod<TRGT, S>> reverseSetterMethod = nullable(Accessors.mutatorByMethod(reverseField));
						if (reverseSetterMethod.isPresent()) {
							collectionAccessor = new DefaultReadWritePropertyAccessPoint<>(reverseSetterMethod.get());
						}
					}
				} // else : relation is not bidirectional, or not a usual one, may be set by reverse link
			}
			
			Nullable<SerializableMutator<TRGT, SRC>> configuredCombiner = nullable(mappedByConfiguration.getCombiner());
			if (collectionAccessor == null) {
				return configuredCombiner.get();
			} else {
				// collection factory is in priority the one configured
				Supplier<S> collectionFactory = mappedByConfiguration.getFactory();
				if (collectionFactory == null) {
					Class<S> collectionType = AccessorDefinition.giveDefinition(collectionAccessor).getMemberType();
					collectionFactory = Reflections.giveCollectionFactory(collectionType);
				}
				ReadWriteAccessPoint<TRGT, S> finalCollectionAccessor = collectionAccessor;
				SerializableMutator<TRGT, SRC> combiner = configuredCombiner.getOr((TRGT trgt, SRC src) -> {
					// collectionAccessor can't be null due to nullable check
					finalCollectionAccessor.get(trgt).add(src);
				});
				
				Supplier<S> effectiveCollectionFactory = collectionFactory;
				return (TRGT trgt, SRC src) -> {
					// we call the collection factory to ensure that property is initialized
					if (finalCollectionAccessor.get(trgt) == null) {
						finalCollectionAccessor.set(trgt, effectiveCollectionFactory.get());
					}
					// Note that combiner can't be null here thanks to nullable(..) check
					combiner.set(trgt, src);
				};
			}
		}
	}
}

