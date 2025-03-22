package org.codefilarete.stalactite.engine.configurer.manytomany;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointByMethodReference;
import org.codefilarete.stalactite.engine.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.engine.PolymorphismPolicy;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
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
	
	private final ValueAccessPointByMethodReference<SRC> methodReference;
	
	/** Configuration used for "many" side beans persistence */
	private final EntityMappingConfigurationProvider<SRC, ?> sourceMappingConfiguration;
	
	/** Configuration used for "many" side beans persistence */
	private final EntityMappingConfigurationProvider<TRGT, TRGTID> targetMappingConfiguration;
	
	@Nullable
	private final Table targetTable;
	
	/** Default relation mode is {@link RelationMode#ALL} */
	private RelationMode relationMode = RelationMode.ALL;
	
	/** Optional provider of collection instance to be used if collection value is null */
	private Supplier<C1> collectionFactory;
	
	private final MappedByConfiguration mappedByConfiguration = new MappedByConfiguration();
	
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
	 * Default, simple constructor.
	 * 
	 * @param collectionAccessor provider of the property to be persisted
	 * @param methodReference equivalent to collectionProvider
	 * @param targetMappingConfiguration persistence configuration of entities stored in the target collection
	 * @param targetTable optional table to be used to store target entities
	 */
	public ManyToManyRelation(ReversibleAccessor<SRC, C1> collectionAccessor,
							  ValueAccessPointByMethodReference<SRC> methodReference,
							  EntityMappingConfiguration<? extends SRC, ?> sourceMappingConfiguration,
							  EntityMappingConfiguration<? extends TRGT, TRGTID> targetMappingConfiguration,
							  @Nullable Table targetTable) {
		this(collectionAccessor,
				methodReference,
				() -> (EntityMappingConfiguration<SRC, Object>) sourceMappingConfiguration,
				() -> (EntityMappingConfiguration<TRGT, TRGTID>) targetMappingConfiguration,
				targetTable);
	}
	
	/**
	 * Constructor with lazy configuration provider. To be used when target configuration is not defined while source configuration is defined, for
	 * instance on cycling configuration.
	 *
	 * @param collectionAccessor provider of the property to be persisted
	 * @param methodReference equivalent to collectionProvider
	 * @param targetMappingConfiguration persistence configuration provider of entities stored in the target collection
	 * @param targetTable optional table to be used to store target entities
	 */
	public ManyToManyRelation(ReversibleAccessor<SRC, C1> collectionAccessor,
							  ValueAccessPointByMethodReference<SRC> methodReference,
							  EntityMappingConfigurationProvider<? extends SRC, ?> sourceMappingConfiguration,
							  EntityMappingConfigurationProvider<? super TRGT, TRGTID> targetMappingConfiguration,
							  @Nullable Table targetTable) {
		this.collectionAccessor = collectionAccessor;
		this.methodReference = methodReference;
		this.sourceMappingConfiguration = (EntityMappingConfigurationProvider<SRC, ?>) sourceMappingConfiguration;
		this.targetMappingConfiguration = (EntityMappingConfigurationProvider<TRGT, TRGTID>) targetMappingConfiguration;
		this.targetTable = targetTable;
	}
	
	public ReversibleAccessor<SRC, C1> getCollectionAccessor() {
		return collectionAccessor;
	}
	
	public ValueAccessPointByMethodReference<SRC> getMethodReference() {
		return methodReference;
	}
	
	/** @return the configuration used for "many" side beans persistence */
	public EntityMappingConfiguration<SRC, ?> getSourceMappingConfiguration() {
		return sourceMappingConfiguration.getConfiguration();
	}
	
	public boolean isSourceTablePerClassPolymorphic() {
		return getSourceMappingConfiguration().getPolymorphismPolicy() instanceof PolymorphismPolicy.TablePerClassPolymorphism;
	}
	
	/** @return the configuration used for "many" side beans persistence */
	public EntityMappingConfiguration<TRGT, TRGTID> getTargetMappingConfiguration() {
		return targetMappingConfiguration.getConfiguration();
	}
	
	public boolean isTargetTablePerClassPolymorphic() {
		return getTargetMappingConfiguration().getPolymorphismPolicy() instanceof PolymorphismPolicy.TablePerClassPolymorphism;
	} 
	
	@Nullable
	public Table getTargetTable() {
		return targetTable;
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
	
	public void setIndexingColumnName(String columnName) {
		ordered();
		this.indexingColumnName = columnName;
	}
	
	@Nullable
	public String getIndexingColumnName() {
		return indexingColumnName;
	}
	
	public void ordered() {
		this.ordered = true;
	}
	
	public boolean isOrdered() {
		return ordered;
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
		
		public boolean isEmpty() {
			return reverseCollectionAccessor == null && reverseCollectionMutator == null && reverseCollectionFactory == null && reverseCombiner == null;
		}
	}
}
