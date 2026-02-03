package org.codefilarete.stalactite.engine.configurer.manytomany;

import java.util.Collection;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorChain.ValueInitializerOnNullValue;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.PolymorphismPolicy;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.tool.Reflections;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * 
 * @param <SRC> the "one" type
 * @param <TRGT> the "many" type
 * @param <TRGTID> identifier type of TRGT
 * @param <C1> the "many" collection type
 */
public class ManyToManyRelation<SRC, TRGT, TRGTID, C1 extends Collection<TRGT>, C2 extends Collection<SRC>> {
	
	/** The method that gives the "many" entities from the "one" entity */
	private final ReversibleAccessor<SRC, C1> collectionAccessor;
	
	private final BooleanSupplier sourceTablePerClassPolymorphic;
	
	/** Configuration used for "many" side beans persistence */
	private final EntityMappingConfigurationProvider<TRGT, TRGTID> targetMappingConfiguration;
	
	/** Default relation mode is {@link RelationMode#ALL} */
	private RelationMode relationMode = RelationMode.ALL;
	
	/** Optional provider of collection instance to be used if collection value is null */
	private Supplier<C1> collectionFactory;
	
	private final MappedByConfiguration<SRC, TRGT, C2> mappedByConfiguration;
	
	/**
	 * Indicates that relation must be loaded in same main query (through join) or in some separate query
	 */
	private boolean fetchSeparately;
	
	/**
	 * Indicates if target instances are indexed in their {@link Collection} (meaning that it is capable of storing order)
	 */
	private boolean ordered = false;
	
	private String indexingColumnName;
	
	/**
	 *
	 * @param collectionAccessor provider of the property to be persisted
	 * @param sourceTablePerClassPolymorphic indicates that source is table-per-class polymorphic
	 * @param targetMappingConfiguration persistence configuration provider of entities stored in the target collection
	 */
	public ManyToManyRelation(ReversibleAccessor<SRC, C1> collectionAccessor,
							  BooleanSupplier sourceTablePerClassPolymorphic,
							  EntityMappingConfigurationProvider<? super TRGT, TRGTID> targetMappingConfiguration) {
		this.collectionAccessor = collectionAccessor;
		this.sourceTablePerClassPolymorphic = sourceTablePerClassPolymorphic;
		this.targetMappingConfiguration = (EntityMappingConfigurationProvider<TRGT, TRGTID>) targetMappingConfiguration;
		this.mappedByConfiguration = new MappedByConfiguration<>();
	}
	
	public ManyToManyRelation(ReversibleAccessor<SRC, C1> collectionAccessor,
							  BooleanSupplier sourceTablePerClassPolymorphic,
							  EntityMappingConfigurationProvider<? super TRGT, TRGTID> targetMappingConfiguration,
							  MappedByConfiguration<?, TRGT, ?> mappedByConfiguration) {
		this.collectionAccessor = collectionAccessor;
		this.sourceTablePerClassPolymorphic = sourceTablePerClassPolymorphic;
		this.targetMappingConfiguration = (EntityMappingConfigurationProvider<TRGT, TRGTID>) targetMappingConfiguration;
		// Note that this cast is wrong, but left for simplicity: this constructor is used for embedded many-to-many relation, which means that the SRC
		// type is the one that embed another one which contains the relation. In such configuration, the relation actually points to the embeddable
		// type, not the one that embeds the relation. This the mappedBy(..) config does the same: it point to the embeddable type, not the SRC type.
		// But fixing it has a lot of impacts due to the necessity to replace MappedByConfiguration "SRC" type by a generic <?> one with has its own
		// complexity. I consider all these impacts doesn't worth it and I prefer to force this cast, even wrong.
		this.mappedByConfiguration = (MappedByConfiguration<SRC, TRGT, C2>) mappedByConfiguration;
	}
	
	public ReversibleAccessor<SRC, C1> getCollectionAccessor() {
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
	
	public RelationMode getRelationMode() {
		return relationMode;
	}
	
	public void setRelationMode(RelationMode relationMode) {
		this.relationMode = relationMode;
	}
	
	@Nullable
	public Supplier<C1> getCollectionFactory() {
		return collectionFactory;
	}
	
	public void setCollectionFactory(Supplier<C1> collectionFactory) {
		this.collectionFactory = collectionFactory;
	}
	
	public MappedByConfiguration<SRC, TRGT, C2> getMappedByConfiguration() {
		return mappedByConfiguration;
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
	
	public void setIndexingColumnName(String columnName) {
		ordered();
		this.indexingColumnName = columnName;
	}
	
	@Nullable
	public String getIndexingColumnName() {
		return indexingColumnName;
	}
	
	public boolean isOrdered() {
		return ordered;
	}
	
	public void setOrdered(boolean ordered) {
		this.ordered = ordered;
	}
	
	public void ordered() {
		this.ordered = true;
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
	
	/**
	 * Clones this object to create a new one with the given accessor as prefix of current one.
	 * Made to shift current instance with an accessor prefix. Used for embeddable objects with relation to make the relation being accessible
	 * from the "root" entity.
	 *
	 * @param accessor the prefix of the clone to be created
	 * @return a clones of this instance prefixed with the given accessor
	 * @param <E> the root entity type that owns the embeddable which has this relation
	 */
	public <E, S extends Collection<E>> ManyToManyRelation<E, TRGT, TRGTID, C1, S> embedInto(Accessor<E, SRC> accessor, Class<SRC> embeddedType) {
		AccessorChain<E, C1> shiftedTargetProvider = new AccessorChain<>(accessor, collectionAccessor);
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
		ShiftedMappedByConfiguration<E, SRC, TRGT, C2> shiftedMappedByConfiguration = this.mappedByConfiguration.embedInto(accessor);
		
		ManyToManyRelation<E, TRGT, TRGTID, C1, S> result = new ManyToManyRelation<>(shiftedTargetProvider,
				this::isSourceTablePerClassPolymorphic,
				this.targetMappingConfiguration,
				shiftedMappedByConfiguration);
		result.setRelationMode(this.getRelationMode());
		result.setFetchSeparately(this.isFetchSeparately());
		result.setIndexingColumnName(this.getIndexingColumnName());
		result.setOrdered(this.isOrdered());
		result.setCollectionFactory(this.getCollectionFactory());
		return result;
	}
	
	public static class MappedByConfiguration<SRC, TRGT, C2 extends Collection<SRC>> {
		
		/**
		 * Combiner of target entity with source entity
		 */
		@Nullable
		private SerializableBiConsumer<TRGT, SRC> reverseCombiner;
		
		/**
		 * Source getter on target for bidirectionality (no consequence on database mapping).
		 */
		@Nullable
		private SerializableFunction<TRGT, C2> reverseCollectionAccessor;
		
		/**
		 * Source setter on target for bidirectionality (no consequence on database mapping).
		 */
		@Nullable
		private SerializableBiConsumer<TRGT, C2> reverseCollectionMutator;
		
		/** Optional provider of collection instance to be used if collection value is null */
		@Nullable
		private Supplier<C2> reverseCollectionFactory;
		
		@Nullable
		protected String associationTableName;
		
		@Nullable
		protected String sourceJoinColumnName;
		
		@Nullable
		protected String targetJoinColumnName;
		
		@Nullable
		public SerializableBiConsumer<TRGT, SRC> getReverseCombiner() {
			return reverseCombiner;
		}
		
		public void setReverseCombiner(@Nullable SerializableBiConsumer<TRGT, SRC> reverseCombiner) {
			this.reverseCombiner = reverseCombiner;
		}
		
		@Nullable
		public SerializableFunction<TRGT, C2> getReverseCollectionAccessor() {
			return reverseCollectionAccessor;
		}
		
		public void setReverseCollectionAccessor(@Nullable SerializableFunction<TRGT, C2> reverseCollectionAccessor) {
			this.reverseCollectionAccessor = reverseCollectionAccessor;
		}
		
		@Nullable
		public SerializableBiConsumer<TRGT, C2> getReverseCollectionMutator() {
			return reverseCollectionMutator;
		}
		
		public void setReverseCollectionMutator(@Nullable SerializableBiConsumer<TRGT, C2> reverseCollectionMutator) {
			this.reverseCollectionMutator = reverseCollectionMutator;
		}
		
		@Nullable
		public Supplier<C2> getReverseCollectionFactory() {
			return reverseCollectionFactory;
		}
		
		public void setReverseCollectionFactory(@Nullable Supplier<C2> reverseCollectionFactory) {
			this.reverseCollectionFactory = reverseCollectionFactory;
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
		
		public boolean isEmpty() {
			return reverseCollectionAccessor == null && reverseCollectionMutator == null && reverseCollectionFactory == null && reverseCombiner == null;
		}
		
		<C> ShiftedMappedByConfiguration<C, SRC, TRGT, C2> embedInto(Accessor<C, SRC> accessor) {
			return new ShiftedMappedByConfiguration<>(accessor, this);
		}
	}
	
	public static class ShiftedMappedByConfiguration<C, SRC, TRGT, C2 extends Collection<SRC>> extends MappedByConfiguration<SRC, TRGT, C2> {
		
		private final Accessor<C, SRC> shifter;
		
		private ShiftedMappedByConfiguration(Accessor<C, SRC> shifter, MappedByConfiguration<SRC, TRGT, C2> mappedByConfiguration) {
			setReverseCombiner(mappedByConfiguration.getReverseCombiner());
			setReverseCollectionAccessor(mappedByConfiguration.getReverseCollectionAccessor());
			setReverseCollectionMutator(mappedByConfiguration.getReverseCollectionMutator());
			setReverseCollectionFactory(mappedByConfiguration.getReverseCollectionFactory());
			this.shifter = shifter;
		}
		
		public Accessor<C, SRC> getShifter() {
			return shifter;
		}
	}
}
