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

/**
 * 
 * @param <SRC> the "one" type
 * @param <TRGT> the "many" type
 * @param <TRGTID> identifier type of TRGT
 * @param <C1> the "many" collection type
 */
public class ManyToManyRelation<SRC, TRGT, TRGTID, C1 extends Collection<TRGT>, C2 extends Collection<SRC>> {
	
	/** The method that gives the "many" entities from the "one" entity */
	private final ReversibleAccessor<SRC, C1> collectionProvider;
	
	private final ValueAccessPointByMethodReference<SRC> methodReference;
	
	/** Configuration used for "many" side beans persistence */
	private final EntityMappingConfigurationProvider<TRGT, TRGTID> targetMappingConfiguration;
	
	@Nullable
	private final Table targetTable;
	
	/** Default relation mode is {@link RelationMode#ALL} */
	private RelationMode relationMode = RelationMode.ALL;
	
	/** Optional provider of collection instance to be used if collection value is null */
	private Supplier<C1> collectionFactory;
	
	/** Optional provider of collection instance to be used if collection value is null */
	private Supplier<C2> reverseCollectionFactory;
	
	/**
	 * Indicates if target instances are indexed in their {@link Collection} (meaning that it is capable of storing order)
	 */
	private boolean indexed = false;
	
	/**
	 * Indicates that relation must be loaded in same main query (through join) or in some separate query
	 */
	private boolean fetchSeparately;
	
	
	/**
	 * Default, simple constructor.
	 * 
	 * @param collectionProvider provider of the property to be persisted
	 * @param methodReference equivalent to collectionProvider
	 * @param targetMappingConfiguration persistence configuration of entities stored in the target collection
	 * @param targetTable optional table to be used to store target entities
	 */
	public ManyToManyRelation(ReversibleAccessor<SRC, C1> collectionProvider,
												ValueAccessPointByMethodReference<SRC> methodReference,
												EntityMappingConfiguration<? extends TRGT, TRGTID> targetMappingConfiguration,
												@Nullable Table targetTable) {
		this(collectionProvider, methodReference,
				() -> (EntityMappingConfiguration<TRGT, TRGTID>) targetMappingConfiguration, targetTable);
	}
	
	/**
	 * Constructor with lazy configuration provider. To be used when target configuration is not defined while source configuration is defined, for
	 * instance on cycling configuration.
	 *
	 * @param collectionProvider provider of the property to be persisted
	 * @param methodReference equivalent to collectionProvider
	 * @param targetMappingConfiguration persistence configuration provider of entities stored in the target collection
	 * @param targetTable optional table to be used to store target entities
	 */
	public ManyToManyRelation(ReversibleAccessor<SRC, C1> collectionProvider,
												ValueAccessPointByMethodReference<SRC> methodReference,
												EntityMappingConfigurationProvider<? extends TRGT, TRGTID> targetMappingConfiguration,
												@Nullable Table targetTable) {
		this.collectionProvider = collectionProvider;
		this.methodReference = methodReference;
		this.targetMappingConfiguration = (EntityMappingConfigurationProvider<TRGT, TRGTID>) targetMappingConfiguration;
		this.targetTable = targetTable;
	}
	
	public ReversibleAccessor<SRC, C1> getCollectionProvider() {
		return collectionProvider;
	}
	
	public ValueAccessPointByMethodReference<SRC> getMethodReference() {
		return methodReference;
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
	
	@Nullable
	public Supplier<C2> getReverseCollectionFactory() {
		return reverseCollectionFactory;
	}
	
	public void setReverseCollectionFactory(Supplier<C2> reverseCollectionFactory) {
		this.reverseCollectionFactory = reverseCollectionFactory;
	}
	
	public boolean isIndexed() {
		return indexed;
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
	
}
