package org.codefilarete.stalactite.engine.configurer.onetomany;

import java.util.Collection;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorChain.ValueInitializerOnNullValue;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.AccessorDefinitionDefiner;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.PropertyMutator;
import org.codefilarete.reflection.ReadWriteAccessorChain;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.reflection.SerializablePropertyAccessor;
import org.codefilarete.reflection.SerializablePropertyMutator;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.stalactite.dsl.PolymorphismPolicy;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Arrays;

/**
 * 
 * @param <SRC> the "one" type
 * @param <TRGT> the "many" type
 * @param <TRGTID> identifier type of TRGT
 * @param <S> the "many" collection type
 */
public class OneToManyRelation<SRC, TRGT, TRGTID, S extends Collection<TRGT>> {
	
	/** The method that gives the "many" entities from the "one" entity */
	private final ReadWritePropertyAccessPoint<SRC, S> collectionAccessor;
	
	/** Indicator that says if source persister has table-per-class polymorphism */
	private final BooleanSupplier sourceTablePerClassPolymorphic;
	
	/** Configuration used for "many" side beans persistence */
	private final EntityMappingConfigurationProvider<TRGT, TRGTID> targetMappingConfiguration;
	
	private final MappedByConfiguration<TRGT, SRC> mappedByConfiguration;
	
	/**
	 * Source setter on target for bidirectionality (no consequence on database mapping).
	 * Useful only for cases of association table because this case doesn't set any reverse information hence such setter can't be deduced.
	 */
	@Nullable
	private SerializablePropertyMutator<TRGT, SRC> reverseLink;
	
	/** Default relation mode is {@link RelationMode#ALL} */
	private RelationMode relationMode = RelationMode.ALL;
	/** Optional provider of collection instance to be used if collection value is null */
	@Nullable
	private Supplier<S> collectionFactory;
	
	/**
	 * Indicates that relation must be loaded in same main query (through join) or in some separate query
	 */
	private boolean fetchSeparately;
	
	@Nullable
	private Column<?, Integer> indexingColumn;
	
	@Nullable
	private String indexingColumnName;
	
	private boolean ordered = false;
	
	/**
	 * Constructor with lazy configuration provider. To be used when target configuration is not defined while source configuration is defined, for
	 * instance on cycling configuration.
	 *
	 * @param collectionAccessor provider of the property to be persisted
	 * @param sourceTablePerClassPolymorphic must return true if source persister has table-per-class polymorphism
	 * @param targetMappingConfiguration must return persistence configuration of entities stored in the target collection
	 */
	public OneToManyRelation(ReadWritePropertyAccessPoint<SRC, S> collectionAccessor,
							 BooleanSupplier sourceTablePerClassPolymorphic,
							 EntityMappingConfigurationProvider<? super TRGT, TRGTID> targetMappingConfiguration) {
		this.collectionAccessor = collectionAccessor;
		this.sourceTablePerClassPolymorphic = sourceTablePerClassPolymorphic;
		this.targetMappingConfiguration = (EntityMappingConfigurationProvider<TRGT, TRGTID>) targetMappingConfiguration;
		this.mappedByConfiguration = new MappedByConfiguration<>();
	}
	
	private OneToManyRelation(ReadWritePropertyAccessPoint<SRC, S> collectionAccessor,
							  BooleanSupplier sourceTablePerClassPolymorphic,
							  EntityMappingConfigurationProvider<? super TRGT, TRGTID> targetMappingConfiguration,
							  MappedByConfiguration<TRGT, ?> mappedByConfiguration) {
		this.collectionAccessor = collectionAccessor;
		this.sourceTablePerClassPolymorphic = sourceTablePerClassPolymorphic;
		this.targetMappingConfiguration = (EntityMappingConfigurationProvider<TRGT, TRGTID>) targetMappingConfiguration;
		// Note that this cast is wrong, but left for simplicity: this constructor is used for embedded one-to-many relation, which means that the SRC
		// type is the one that embed another one which contains the relation. In such configuration, the relation actually points to the embeddable
		// type, not the one that embeds the relation. This the mappedBy(..) config does the same: it point to the embeddable type, not the SRC type.
		// But fixing it has a lot of impacts due to the necessity to replace MappedByConfiguration "SRC" type by a generic <?> one with has its own
		// complexity. I consider all these impacts doesn't worth it and I prefer to force this cast, even wrong.
		this.mappedByConfiguration = (MappedByConfiguration<TRGT, SRC>) mappedByConfiguration;
	}
	
	public ReadWritePropertyAccessPoint<SRC, S> getCollectionAccessor() {
		return collectionAccessor;
	}
	
	public boolean isSourceTablePerClassPolymorphic() {
		return sourceTablePerClassPolymorphic.getAsBoolean();
	}
	
	/** @return the configuration used for "many" side beans persistence */
	public EntityMappingConfiguration<TRGT, TRGTID> getTargetMappingConfiguration() {
		return targetMappingConfiguration.getConfiguration();
	}
	
	public boolean isTargetTablePerClassPolymorphic() {
		return getTargetMappingConfiguration().getPolymorphismPolicy() instanceof PolymorphismPolicy.TablePerClassPolymorphism;
	}
	
	public void setReverseGetter(SerializablePropertyAccessor<TRGT, ? super SRC> reverseGetter) {
		this.mappedByConfiguration.setReverseGetter((SerializablePropertyAccessor<TRGT, SRC>) reverseGetter);
	}
	
	public void setReverseSetter(SerializablePropertyMutator<TRGT, ? super SRC> reverseSetter) {
		this.mappedByConfiguration.setReverseSetter((SerializablePropertyMutator<TRGT, SRC>) reverseSetter);
	}
	
	@Nullable
	public PropertyMutator<TRGT, SRC> giveReverseSetter() {
		return this.mappedByConfiguration.giveReverseSetter();
	}
	
	@Nullable
	public <O> Column<Table<?>, O> getReverseColumn() {
		return (Column<Table<?>, O>) this.mappedByConfiguration.getReverseColumn();
	}
	
	public void setReverseColumn(Column<?, ?> reverseColumn) {
		this.mappedByConfiguration.setReverseColumn(reverseColumn);
	}
	
	@Nullable
	public String getReverseColumnName() {
		return this.mappedByConfiguration.getReverseColumnName();
	}
	
	public void setReverseColumn(@Nullable String reverseColumnName) {
		this.mappedByConfiguration.setReverseColumnName(reverseColumnName);
	}
	
	@Nullable
	public String getAssociationTableName() {
		return this.mappedByConfiguration.getAssociationTableName();
	}
	
	public void setAssociationTableName(@Nullable String tableName) {
		this.mappedByConfiguration.setAssociationTableName(tableName);
	}
	
	@Nullable
	public String getSourceJoinColumnName() {
		return this.mappedByConfiguration.getSourceJoinColumnName();
	}
	
	public void setSourceJoinColumnName(@Nullable String sourceJoinColumnName) {
		this.mappedByConfiguration.setSourceJoinColumnName(sourceJoinColumnName);
	}
	
	@Nullable
	public String getTargetJoinColumnName() {
		return this.mappedByConfiguration.getTargetJoinColumnName();
	}
	
	public void setTargetJoinColumnName(@Nullable String targetJoinColumnName) {
		this.mappedByConfiguration.setTargetJoinColumnName(targetJoinColumnName);
	}
	
	public Boolean isReverseAsMandatory() {
		return this.mappedByConfiguration.isMandatory();
	}
	
	public void setReverseAsMandatory(boolean mandatory) {
		this.mappedByConfiguration.setMandatory(mandatory);
	}
	
	public ValueAccessPointMap<SRC, Column<Table<?>, Object>, ValueAccessPoint<SRC>> getForeignKeyColumnMapping() {
		return this.mappedByConfiguration.getForeignKeyColumnMapping();
	}
	
	public ValueAccessPointMap<SRC, String, ValueAccessPoint<SRC>> getForeignKeyNameMapping() {
		return this.mappedByConfiguration.getForeignKeyNameMapping();
	}
	
	@Nullable
	public SerializablePropertyMutator<TRGT, SRC> getReverseLink() {
		return reverseLink;
	}
	
	public void setReverseLink(SerializablePropertyMutator<TRGT, SRC> reverseLink) {
		this.reverseLink = reverseLink;
	}
	
	public RelationMode getRelationMode() {
		return relationMode;
	}
	
	public void setRelationMode(RelationMode relationMode) {
		this.relationMode = relationMode;
	}
	
	/**
	 * Indicates if relation is owned by target entities table
	 * @return true if one of {@link #giveReverseSetter()}, {@link #getReverseColumn()} is not null
	 */
	public boolean isOwnedByReverseSide() {
		return this.mappedByConfiguration.isNotEmpty();
	}
	
	@Nullable
	public Supplier<S> getCollectionFactory() {
		return collectionFactory;
	}
	
	public void setCollectionFactory(Supplier<S> collectionFactory) {
		this.collectionFactory = collectionFactory;
	}
	
	public boolean isFetchSeparately() {
		return fetchSeparately;
	}
	
	public void setFetchSeparately(boolean fetchSeparately) {
		this.fetchSeparately = fetchSeparately;
	}
	
	public void fetchSeparately() {
		setFetchSeparately(true);
	}
	
	public void setIndexingColumn(@Nullable Column<?, Integer> indexingColumn) {
		ordered();
		this.indexingColumn = indexingColumn;
	}
	
	@Nullable
	public <T extends Table<T>> Column<T, Integer> getIndexingColumn() {
		return (Column<T, Integer>) indexingColumn;
	}
	
	public void setIndexingColumnName(String columnName) {
		ordered();
		this.indexingColumnName = columnName;
	}
	
	@Nullable
	public String getIndexingColumnName() {
		return indexingColumnName;
	}
	
	public boolean isOrdered() {
		return this.ordered;
	}
	
	public void setOrdered(boolean ordered) {
		this.ordered = ordered;
	}
	
	public void ordered() {
		this.ordered = true;
	}
	
	/**
	 * Clones this object to make one with the given accessor as prefix of current one.
	 * Made to "slide" current instance with an accessor prefix. Used for embeddable objects with relation to make the relation being accessible
	 * from the "root" entity.
	 *
	 * @param accessor the prefix of the clone to be created
	 * @param embeddedType the concrete type of the embeddable bean, because accessor may provide an abstraction
	 * @return a clones of this instance prefixed with the given accessor
	 * @param <C> the root entity type that owns the embeddable which has this relation
	 */
	public <C> OneToManyRelation<C, TRGT, TRGTID, S> embedInto(ReadWritePropertyAccessPoint<C, SRC> accessor, Class<SRC> embeddedType) {
		AccessorChain<C, S> shiftedTargetProvider = new AccessorChain<>(Arrays.asList(accessor, collectionAccessor));
		shiftedTargetProvider.setNullValueHandler(new ValueInitializerOnNullValue() {
			@Override
			protected <T> T newInstance(Accessor<?, T> segmentAccessor, Class<T> valueType) {
				if (segmentAccessor == accessor) {
					return (T) Reflections.newInstance(embeddedType);
				} else if (segmentAccessor == collectionAccessor){
					if (collectionFactory != null) {
						return (T) collectionFactory.get();
					} else {
						return super.newInstance(segmentAccessor, valueType);
					}
				} else {
					return super.newInstance(segmentAccessor, valueType);
				}
			}
		});
		MappedByConfiguration<TRGT, C> slidedMappedByConfiguration = this.mappedByConfiguration.embedInto(accessor);
		
		OneToManyRelation<C, TRGT, TRGTID, S> result = new OneToManyRelation<>(new ReadWriteAccessorChain<>(shiftedTargetProvider),
				this::isSourceTablePerClassPolymorphic,
				this.targetMappingConfiguration,
				slidedMappedByConfiguration);
		result.setRelationMode(this.getRelationMode());
		result.setFetchSeparately(this.isFetchSeparately());
		result.setIndexingColumnName(this.getIndexingColumnName());
		result.setOrdered(this.isOrdered());
		result.setCollectionFactory(this.getCollectionFactory());
		return result;
	}
	
	private static class MappedByConfiguration<TRGT, SRC> {
		
		/** The method that gets the "one" entity from the "many" entities, may be null */
		protected ReadWritePropertyAccessPoint<TRGT, SRC> reverseGetter;
		
		/** The method that sets the "one" entity onto the "many" entities, may be null */
		protected ReadWritePropertyAccessPoint<TRGT, SRC> reverseSetter;
		
		/**
		 * The column that stores relation, may be null.
		 * Its type is undetermined (not forced at SRC) because it can only be a reference, such as an id.
		 */
		@Nullable
		protected Column<Table<?>, Object> reverseColumn;
		
		@Nullable
		protected String reverseColumnName;
		
		@Nullable
		protected String associationTableName;
		
		@Nullable
		protected String sourceJoinColumnName;
		
		@Nullable
		protected String targetJoinColumnName;
		
		private Boolean mandatory;
		
		protected final ValueAccessPointMap<SRC, Column<Table<?>, Object>, ValueAccessPoint<SRC>> foreignKeyColumnMapping = new ValueAccessPointMap<>();
		
		protected final ValueAccessPointMap<SRC, String, ValueAccessPoint<SRC>> foreignKeyNameMapping = new ValueAccessPointMap<>();
		
		/**
		 * Clones this object to create a new one with the given accessor as prefix of current one.
		 * Made to shift the current instance with an accessor prefix. Used for embeddable objects with relation to make the relation being accessible
		 * from the "root" entity.
		 *
		 * @param accessor the prefix of the clone to be created
		 * @return a clone of this instance prefixed with the given accessor
		 * @param <C> the root entity type that owns the embeddable which has this relation
		 */
		<C> MappedByConfiguration<TRGT, C> embedInto(ReadWritePropertyAccessPoint<C, SRC> accessor) {
			return new ShiftedMappedByConfiguration<>(accessor, this);
		}
		
		public void setReverseGetter(SerializablePropertyAccessor<TRGT, SRC> reverseGetter) {
			this.reverseGetter = Accessors.readWriteAccessPoint(reverseGetter);
		}
		
		public void setReverseSetter(SerializablePropertyMutator<TRGT, SRC> reverseSetter) {
			this.reverseSetter = Accessors.readWriteAccessPoint(reverseSetter);
		}
		
		public PropertyMutator<TRGT, SRC> giveReverseSetter() {
			if (this.reverseGetter != null) {
				return this.reverseGetter;
			} else if (this.reverseSetter != null) {
				return this.reverseSetter;
			} else {
				return null;
			}
		}
		
		@Nullable
		public String getReverseColumnName() {
			return reverseColumnName;
		}
		
		public void setReverseColumnName(@Nullable String reverseColumnName) {
			this.reverseColumnName = reverseColumnName;
		}
		
		
		@Nullable
		public String getAssociationTableName() {
			return associationTableName;
		}
		
		public void setAssociationTableName(@Nullable String associationTableName) {
			this.associationTableName = associationTableName;
		}
		
		@Nullable
		public String getSourceJoinColumnName() {
			return sourceJoinColumnName;
		}
		
		public void setSourceJoinColumnName(@Nullable String sourceJoinColumnName) {
			this.sourceJoinColumnName = sourceJoinColumnName;
		}
		
		@Nullable
		public String getTargetJoinColumnName() {
			return targetJoinColumnName;
		}
		
		public void setTargetJoinColumnName(@Nullable String targetJoinColumnName) {
			this.targetJoinColumnName = targetJoinColumnName;
		}
		
		@Nullable
		public Column<Table<?>, ?> getReverseColumn() {
			return reverseColumn;
		}
		
		public void setReverseColumn(@Nullable Column<?, ?> reverseColumn) {
			this.reverseColumn = (Column<Table<?>, Object>) reverseColumn;
		}
		
		public ValueAccessPointMap<SRC, Column<Table<?>, Object>, ValueAccessPoint<SRC>> getForeignKeyColumnMapping() {
			return foreignKeyColumnMapping;
		}
		
		public ValueAccessPointMap<SRC, String, ValueAccessPoint<SRC>> getForeignKeyNameMapping() {
			return foreignKeyNameMapping;
		}
		
		public Boolean isMandatory() {
			return mandatory;
		}
		
		public void setMandatory(boolean mandatory) {
			this.mandatory = mandatory;
		}
		
		public boolean isNotEmpty() {
			return reverseSetter != null || reverseGetter != null || reverseColumn != null || reverseColumnName != null
					|| !foreignKeyColumnMapping.isEmpty() || !foreignKeyNameMapping.isEmpty();
		}
	}
	
	private static class ShiftedMappedByConfiguration<C, TRGT, SRC> extends MappedByConfiguration<TRGT, C> {
		
		private final PropertyMutator<TRGT, C> effectiveReverseMutator;
		
		public ShiftedMappedByConfiguration(ReadWritePropertyAccessPoint<C, SRC> accessor, MappedByConfiguration<TRGT, SRC> mappedByConfiguration) {
			this.reverseColumn = mappedByConfiguration.reverseColumn;
			this.reverseColumnName = mappedByConfiguration.reverseColumnName;
			this.foreignKeyColumnMapping.putAll((ValueAccessPointMap) mappedByConfiguration.foreignKeyColumnMapping);
			this.foreignKeyNameMapping.putAll((ValueAccessPointMap) mappedByConfiguration.foreignKeyNameMapping);
			// Note that we don't set this.reverseGetter nor this.reverseSetter because it raises generics problem, and we can afford not to store them.
			
			// Creates shifted mutator when reverse accessor is present
			if (mappedByConfiguration.reverseGetter != null || mappedByConfiguration.reverseSetter != null) {
				PropertyMutator<TRGT, SRC> localReverseSetter;
				AccessorDefinition reverseDefinition;
				if (mappedByConfiguration.reverseGetter != null) {
					localReverseSetter = mappedByConfiguration.reverseGetter;
					reverseDefinition = AccessorDefinition.giveDefinition(mappedByConfiguration.reverseGetter);
				} else {
					localReverseSetter = mappedByConfiguration.reverseSetter;
					reverseDefinition = AccessorDefinition.giveDefinition(mappedByConfiguration.reverseSetter);
				}
				AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(accessor);
				this.effectiveReverseMutator = new ShiftedMutator<>(
						accessor,
						new AccessorDefinition(accessorDefinition.getDeclaringClass(), accessorDefinition.getName() + "." + reverseDefinition.getName(), reverseDefinition.getMemberType()),
						localReverseSetter);
			} else {
				this.effectiveReverseMutator = null;
			}
		}
		
		@Override
		public PropertyMutator<TRGT, C> giveReverseSetter() {
			return effectiveReverseMutator;
		}
		
		public boolean isNotEmpty() {
			return effectiveReverseMutator != null || super.isNotEmpty();
		}
		
		private class ShiftedMutator<C> implements PropertyMutator<TRGT, C>, AccessorDefinitionDefiner<TRGT> {
			private final ReadWritePropertyAccessPoint<C, SRC> accessor;
			private final AccessorDefinition accessorDefinition;
			private final Mutator<TRGT, SRC> reverseSetter;
			
			public ShiftedMutator(ReadWritePropertyAccessPoint<C, SRC> accessor, AccessorDefinition accessorDefinition, PropertyMutator<TRGT, SRC> reverseSetter) {
				this.accessor = accessor;
				this.accessorDefinition = accessorDefinition;
				this.reverseSetter = reverseSetter;
			}
			
			@Override
			public void set(TRGT trgt, C c) {
				SRC src = accessor.get(c);
				reverseSetter.set(trgt, src);
			}
			
			@Override
			public AccessorDefinition asAccessorDefinition() {
				return accessorDefinition;
			}
		}
	}
}
