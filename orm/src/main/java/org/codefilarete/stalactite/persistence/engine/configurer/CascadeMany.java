package org.codefilarete.stalactite.persistence.engine.configurer;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.function.Supplier;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointByMethodReference;
import org.codefilarete.stalactite.persistence.engine.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.persistence.engine.EntityMappingConfiguration;
import org.codefilarete.stalactite.persistence.engine.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.persistence.engine.PolymorphismPolicy.TablePerClassPolymorphism;
import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.persistence.structure.Table;

/**
 * 
 * @param <SRC> the "one" type
 * @param <TRGT> the "many" type
 * @param <TRGTID> identifier type of TRGT
 * @param <C> the "many" collection type
 */
public class CascadeMany<SRC, TRGT, TRGTID, C extends Collection<TRGT>> {
	
	/** The method that gives the "many" entities from the "one" entity */
	private final ReversibleAccessor<SRC, C> collectionProvider;
	
	private final ValueAccessPointByMethodReference methodReference;
	/** Configuration used for "many" side beans persistence */
	private final EntityMappingConfigurationProvider<TRGT, TRGTID> targetMappingConfiguration;
	
	@Nullable
	private final Table targetTable;
	
	private final MappedByConfiguration mappedByConfiguration = new MappedByConfiguration();
	
	/**
	 * Source setter on target for bidirectionality (no consequence on database mapping).
	 * Usefull only for cases of association table because this case doesn't set any reverse information hence such setter can't be deduced.
	 */
	private SerializableBiConsumer<TRGT, SRC> reverseLink;
	
	/** Default relation mode is {@link RelationMode#ALL} */
	private RelationMode relationMode = RelationMode.ALL;
	/** Optional provider of collection instance to be used if collection value is null */
	private Supplier<C> collectionFactory;
	
	/**
	 * Default, simple constructor.
	 * 
	 * @param collectionProvider provider of the property to be persisted
	 * @param methodReference equivalent to collectionProvider
	 * @param targetMappingConfiguration persistence configuration of entities stored in the target collection
	 * @param targetTable optional table to be used to store target entities
	 * @param <T>
	 */
	public <T extends Table> CascadeMany(ReversibleAccessor<SRC, C> collectionProvider,
										 ValueAccessPointByMethodReference methodReference,
										 EntityMappingConfiguration<? extends TRGT, TRGTID> targetMappingConfiguration,
										 @Nullable T targetTable) {
		this(collectionProvider, methodReference, () -> (EntityMappingConfiguration<TRGT, TRGTID>) targetMappingConfiguration, targetTable);
	}
	
	/**
	 * Constructor with lazy configuration provider. To be used when target configuration is not defined while source configuration is defined, for
	 * instance on cycling configuration.
	 * 
	 * @param collectionProvider
	 * @param methodReference
	 * @param targetMappingConfiguration
	 * @param targetTable
	 * @param <T>
	 */
	public <T extends Table> CascadeMany(ReversibleAccessor<SRC, C> collectionProvider,
										 ValueAccessPointByMethodReference methodReference,
										 EntityMappingConfigurationProvider<? extends TRGT, TRGTID> targetMappingConfiguration,
										 @Nullable T targetTable) {
		this.collectionProvider = collectionProvider;
		this.methodReference = methodReference;
		this.targetMappingConfiguration = (EntityMappingConfigurationProvider<TRGT, TRGTID>) targetMappingConfiguration;
		this.targetTable = targetTable;
	}
	
	public ReversibleAccessor<SRC, C> getCollectionProvider() {
		return collectionProvider;
	}
	
	public ValueAccessPointByMethodReference getMethodReference() {
		return methodReference;
	}
	
	/** @return the configuration used for "many" side beans persistence */
	public EntityMappingConfiguration<TRGT, TRGTID> getTargetMappingConfiguration() {
		return targetMappingConfiguration.getConfiguration();
	}
	
	public boolean isTargetTablePerClassPolymorphic() {
		return getTargetMappingConfiguration().getPolymorphismPolicy() instanceof TablePerClassPolymorphism;
	} 
	
	@Nullable
	public Table getTargetTable() {
		return targetTable;
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
	public <O> Column<Table, O> getReverseColumn() {
		return (Column<Table, O>) this.mappedByConfiguration.reverseColumn;
	}
	
	public void setReverseColumn(Column<Table, ?> reverseColumn) {
		this.mappedByConfiguration.reverseColumn = reverseColumn;
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
	public Supplier<C> getCollectionFactory() {
		return collectionFactory;
	}
	
	public void setCollectionFactory(Supplier<C> collectionFactory) {
		this.collectionFactory = collectionFactory;
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
		private Column<Table, ?> reverseColumn;
		
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
		
		public Column<Table, ?> getReverseColumn() {
			return reverseColumn;
		}
		
		public void setReverseColumn(Column<Table, ?> reverseColumn) {
			this.reverseColumn = reverseColumn;
		}
		
		public boolean isNotEmpty() {
			return reverseSetter != null || reverseGetter != null || reverseColumn != null;
		}
	}
}
