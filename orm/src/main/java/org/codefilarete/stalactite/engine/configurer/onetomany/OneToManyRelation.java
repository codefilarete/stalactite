package org.codefilarete.stalactite.engine.configurer.onetomany;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointByMethodReference;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.stalactite.dsl.PolymorphismPolicy;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * 
 * @param <SRC> the "one" type
 * @param <TRGT> the "many" type
 * @param <TRGTID> identifier type of TRGT
 * @param <S> the "many" collection type
 */
public class OneToManyRelation<SRC, TRGT, TRGTID, S extends Collection<TRGT>> {
	
	/** The method that gives the "many" entities from the "one" entity */
	private final ReversibleAccessor<SRC, S> collectionProvider;
	
	private final ValueAccessPointByMethodReference<SRC> methodReference;
	
	/** Indicator that says if source persister has table-per-class polymorphism */
	private final BooleanSupplier sourceTablePerClassPolymorphic;
	
	/** Configuration used for "many" side beans persistence */
	private final EntityMappingConfigurationProvider<TRGT, TRGTID> targetMappingConfiguration;
	
	private final MappedByConfiguration mappedByConfiguration = new MappedByConfiguration();
	
	/**
	 * Source setter on target for bidirectionality (no consequence on database mapping).
	 * Useful only for cases of association table because this case doesn't set any reverse information hence such setter can't be deduced.
	 */
	private SerializableBiConsumer<TRGT, SRC> reverseLink;
	
	/** Default relation mode is {@link RelationMode#ALL} */
	private RelationMode relationMode = RelationMode.ALL;
	/** Optional provider of collection instance to be used if collection value is null */
	private Supplier<S> collectionFactory;
	
	/**
	 * Indicates that relation must be loaded in same main query (through join) or in some separate query
	 */
	private boolean fetchSeparately;
	
	@Nullable
	private Column indexingColumn;
	
	@Nullable
	private String indexingColumnName;
	
	private boolean ordered = false;
	
	/**
	 * Constructor with lazy configuration provider. To be used when target configuration is not defined while source configuration is defined, for
	 * instance on cycling configuration.
	 *
	 * @param collectionProvider provider of the property to be persisted
	 * @param methodReference equivalent to collectionProvider
	 * @param sourceTablePerClassPolymorphic must return true if source persister has table-per-class polymorphism
	 * @param targetMappingConfiguration must return persistence configuration of entities stored in the target collection
	 */
	public OneToManyRelation(ReversibleAccessor<SRC, S> collectionProvider,
							 ValueAccessPointByMethodReference<SRC> methodReference,
							 BooleanSupplier sourceTablePerClassPolymorphic,
							 EntityMappingConfigurationProvider<? super TRGT, TRGTID> targetMappingConfiguration) {
		this.collectionProvider = collectionProvider;
		this.methodReference = methodReference;
		this.sourceTablePerClassPolymorphic = sourceTablePerClassPolymorphic;
		this.targetMappingConfiguration = (EntityMappingConfigurationProvider<TRGT, TRGTID>) targetMappingConfiguration;
	}
	
	public ReversibleAccessor<SRC, S> getCollectionProvider() {
		return collectionProvider;
	}
	
	public ValueAccessPointByMethodReference<SRC> getMethodReference() {
		return methodReference;
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
	
	@Nullable
	public SerializableFunction<TRGT, SRC> getReverseGetter() {
		return this.mappedByConfiguration.reverseGetter;
	}
	
	public void setReverseGetter(SerializableFunction<TRGT, ? super SRC> reverseGetter) {
		this.mappedByConfiguration.reverseGetter = (SerializableFunction<TRGT, SRC>) reverseGetter;
	}
	
	@Nullable
	public SerializableBiConsumer<TRGT, SRC> getReverseSetter() {
		return this.mappedByConfiguration.reverseSetter;
	}
	
	public void setReverseSetter(SerializableBiConsumer<TRGT, ? super SRC> reverseSetter) {
		this.mappedByConfiguration.reverseSetter = (SerializableBiConsumer<TRGT, SRC>) reverseSetter;
	}
	
	@Nullable
	public <O> Column<Table<?>, O> getReverseColumn() {
		return (Column<Table<?>, O>) this.mappedByConfiguration.reverseColumn;
	}
	
	public void setReverseColumn(Column<?, ?> reverseColumn) {
		this.mappedByConfiguration.reverseColumn = (Column<Table<?>, Object>) reverseColumn;
	}
	
	@Nullable
	public String getReverseColumnName() {
		return this.mappedByConfiguration.reverseColumnName;
	}
	
	public void setReverseColumn(@Nullable String reverseColumnName) {
		this.mappedByConfiguration.reverseColumnName = reverseColumnName;
	}
	
	public ValueAccessPointMap<SRC, Column<Table<?>, Object>> getForeignKeyColumnMapping() {
		return this.mappedByConfiguration.getForeignKeyColumnMapping();
	}
	
	public ValueAccessPointMap<SRC, String> getForeignKeyNameMapping() {
		return this.mappedByConfiguration.getForeignKeyNameMapping();
	}
	
	@Nullable
	public SerializableBiConsumer<TRGT, SRC> getReverseLink() {
		return reverseLink;
	}
	
	public void setReverseLink(SerializableBiConsumer<TRGT, SRC> reverseLink) {
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
	 * @return true if one of {@link #getReverseSetter()}, {@link #getReverseGetter()}, {@link #getReverseColumn()} is not null
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
	
	public void setIndexingColumn(Column<? extends Table, Integer> indexingColumn) {
		ordered();
		this.indexingColumn = indexingColumn;
	}
	
	@Nullable
	public <T extends Table, O extends Object> Column<T, O> getIndexingColumn() {
		return indexingColumn;
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
		return this.ordered;
	}
	
	/**
	 * Clones this object to make one with the given accessor as prefix of current one.
	 * Made to "slide" current instance with an accessor prefix. Used for embeddable objects with relation to make the relation being accessible
	 * from the "root" entity.
	 *
	 * @param accessor the prefix of the clone to be created
	 * @return a clones of this instance prefixed with the given accessor
	 * @param <C> the root entity type that owns the embeddable which has this relation
	 */
	public <C> OneToManyRelation<C, TRGT, TRGTID, S> embedInto(Accessor<C, SRC> accessor) {
		AccessorChain<C, S> slidedTargetProvider = new AccessorChain<>(accessor, collectionProvider);
		OneToManyRelation<C, TRGT, TRGTID, S> result = new OneToManyRelation<>(slidedTargetProvider, null, this::isSourceTablePerClassPolymorphic, this.targetMappingConfiguration);
		result.setRelationMode(this.getRelationMode());
		result.setFetchSeparately(this.isFetchSeparately());
		return result;
	}
	
	private class MappedByConfiguration {
		
		/** The method that gets the "one" entity from the "many" entities, may be null */
		private SerializableFunction<TRGT, SRC> reverseGetter;
		
		/** The method that sets the "one" entity onto the "many" entities, may be null */
		private SerializableBiConsumer<TRGT, SRC> reverseSetter;
		
		/**
		 * The column that stores relation, may be null.
		 * Its type is undetermined (not forced at SRC) because it can only be a reference, such as an id.
		 */
		private Column<Table<?>, Object> reverseColumn;
		
		private String reverseColumnName;
		
		private final ValueAccessPointMap<SRC, Column<Table<?>, Object>> foreignKeyColumnMapping = new ValueAccessPointMap<>();
		
		private final ValueAccessPointMap<SRC, String> foreignKeyNameMapping = new ValueAccessPointMap<>();
		
		public SerializableFunction<TRGT, SRC> getReverseGetter() {
			return reverseGetter;
		}
		
		public void setReverseGetter(SerializableFunction<TRGT, SRC> reverseGetter) {
			this.reverseGetter = reverseGetter;
		}
		
		public SerializableBiConsumer<TRGT, SRC> getReverseSetter() {
			return reverseSetter;
		}
		
		public void setReverseSetter(SerializableBiConsumer<TRGT, SRC> reverseSetter) {
			this.reverseSetter = reverseSetter;
		}
		
		public Column<Table<?>, ?> getReverseColumn() {
			return reverseColumn;
		}
		
		public void setReverseColumn(Column<?, ?> reverseColumn) {
			this.reverseColumn = (Column<Table<?>, Object>) reverseColumn;
		}
		
		public ValueAccessPointMap<SRC, Column<Table<?>, Object>> getForeignKeyColumnMapping() {
			return foreignKeyColumnMapping;
		}
		
		public ValueAccessPointMap<SRC, String> getForeignKeyNameMapping() {
			return foreignKeyNameMapping;
		}
		
		public boolean isNotEmpty() {
			return reverseSetter != null || reverseGetter != null || reverseColumn != null
					|| !foreignKeyColumnMapping.isEmpty() || !foreignKeyNameMapping.isEmpty();
		}
	}
}
