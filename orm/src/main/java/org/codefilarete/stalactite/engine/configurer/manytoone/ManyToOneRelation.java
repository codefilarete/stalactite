package org.codefilarete.stalactite.engine.configurer.manytoone;

import java.util.Collection;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.DefaultReadWritePropertyAccessPoint;
import org.codefilarete.reflection.ReadWriteAccessorChain;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.reflection.SerializablePropertyAccessor;
import org.codefilarete.reflection.SerializablePropertyMutator;
import org.codefilarete.stalactite.dsl.PolymorphismPolicy;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.tool.Nullable;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * @author Guillaume Mary
 */
public class ManyToOneRelation<SRC, TRGT, TRGTID, S extends Collection<SRC>> {
	
	/** The method that gives the target entity from the source one */
	private final ReadWritePropertyAccessPoint<SRC, TRGT> targetProvider;
	
	/** Configuration used for target beans persistence */
	private final EntityMappingConfigurationProvider<TRGT, TRGTID> targetMappingConfiguration;
	
	private final BooleanSupplier sourceTablePerClassPolymorphic;
	
	private boolean nullable = true;
	
	/** Default relation mode is {@link RelationMode#ALL} */
	private RelationMode relationMode = RelationMode.ALL;
	
	private final MappedByConfiguration<SRC, TRGT, S> mappedByConfiguration = new MappedByConfiguration<>();
	
	/**
	 * Indicates that relation must be loaded in same main query (through join) or in some separate query
	 */
	private boolean fetchSeparately;
	
	@javax.annotation.Nullable
	private String columnName;
	
	@javax.annotation.Nullable
	private Column<?, ?> owningColumn;

	/**
	 *
	 * @param targetProvider provider of the property to be persisted
	 * @param sourceTablePerClassPolymorphic indicates that source is table-per-class polymorphic
	 * @param targetMappingConfiguration persistence configuration provider of entities stored in the target collection
	 */
	public ManyToOneRelation(ReadWritePropertyAccessPoint<SRC, TRGT> targetProvider,
							 BooleanSupplier sourceTablePerClassPolymorphic,
							 EntityMappingConfigurationProvider<? extends TRGT, TRGTID> targetMappingConfiguration) {
		this.sourceTablePerClassPolymorphic = sourceTablePerClassPolymorphic;
		this.targetMappingConfiguration = (EntityMappingConfigurationProvider<TRGT, TRGTID>) targetMappingConfiguration;
		this.targetProvider = targetProvider;
	}
	
	/** Original method reference given for mapping */
	public ReadWritePropertyAccessPoint<SRC, TRGT> getTargetProvider() {
		return targetProvider;
	}
	
	public boolean isSourceTablePerClassPolymorphic() {
		return sourceTablePerClassPolymorphic.getAsBoolean();
	}
	
	/** @return the configuration used for target beans persistence */
	public EntityMappingConfiguration<TRGT, TRGTID> getTargetMappingConfiguration() {
		return targetMappingConfiguration.getConfiguration();
	}
	
	public boolean isTargetTablePerClassPolymorphic() {
		return getTargetMappingConfiguration().getPolymorphismPolicy() instanceof PolymorphismPolicy.TablePerClassPolymorphism;
	}
	
	/** Nullable option, mainly for column join and DDL schema generation */
	public boolean isNullable() {
		return nullable;
	}
	
	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}
	
	public RelationMode getRelationMode() {
		return relationMode;
	}
	
	public void setRelationMode(RelationMode relationMode) {
		this.relationMode = relationMode;
	}
	
	public MappedByConfiguration<SRC, TRGT, S> getMappedByConfiguration() {
		return mappedByConfiguration;
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
	
	@javax.annotation.Nullable
	public String getColumnName() {
		return columnName;
	}
	
	public void setColumnName(@javax.annotation.Nullable String columnName) {
		this.columnName = columnName;
	}
	
	@javax.annotation.Nullable
	public Column<?, ?> getOwningColumn() {
		return owningColumn;
	}

	public void setOwningColumn(@javax.annotation.Nullable Column<?, ?> owningColumn) {
		this.owningColumn = owningColumn;
	}

	/**
	 * Build the accessor for the reverse property, made of configured getter and setter. If one of them is unavailable, it's deduced from the
	 * present one. If both are absent, null is returned.
	 *
	 * @return null if no getter nor setter were defined
	 */
	@javax.annotation.Nullable
	public ReadWritePropertyAccessPoint<TRGT, S> buildReversePropertyAccessor() {
		Nullable<SerializablePropertyAccessor<TRGT, S>> getterReference = nullable(mappedByConfiguration.getAccessor());
		Nullable<SerializablePropertyMutator<TRGT, S>> setterReference = nullable(mappedByConfiguration.getMutator());
		if (getterReference.isAbsent() && setterReference.isAbsent()) {
			return null;
		} else if (getterReference.isPresent() && setterReference.isPresent()) {
			// we keep close to user demand : we keep its method references
			return new DefaultReadWritePropertyAccessPoint<>(getterReference.get(), setterReference.get());
		} else if (getterReference.isPresent() && setterReference.isAbsent()) {
			// we keep close to user demand : we keep its method reference ...
			// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
			return Accessors.readWriteAccessPoint(getterReference.get());
		} else {
			// we keep close to user demand : we keep its method reference ...
			// ... but we can't do it for getter, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
			return Accessors.readWriteAccessPoint(setterReference.get());
		}
	}
	
	/**
	 * Clones this object to create a new one with the given accessor as prefix of current one.
	 * Made to shift the current instance with an accessor prefix. Used for embeddable objects with relation to make the relation being accessible
	 * from the "root" entity.
	 *
	 * @param accessor the prefix of the clone to be created
	 * @return a clone of this instance prefixed with the given accessor
	 * @param <E> the root entity type that owns the embeddable which has this relation
	 */
	public <E, CC extends Collection<E>> ManyToOneRelation<E, TRGT, TRGTID, CC> embedInto(Accessor<E, SRC> accessor) {
		AccessorChain<E, TRGT> shiftedTargetProvider = new AccessorChain<>(accessor, targetProvider);
		ManyToOneRelation<E, TRGT, TRGTID, CC> result = new ManyToOneRelation<>(new ReadWriteAccessorChain<>(shiftedTargetProvider), this::isSourceTablePerClassPolymorphic, this::getTargetMappingConfiguration);
		result.setRelationMode(this.getRelationMode());
		result.setNullable(this.isNullable());
		result.setFetchSeparately(this.isFetchSeparately());
		result.setColumnName(this.getColumnName());
		result.setOwningColumn(this.getOwningColumn());
		return result;
	}
	
	public static class MappedByConfiguration<SRC, TRGT, S extends Collection<SRC>> {
		
		/**
		 * Combiner of target entity with source entity
		 */
		@javax.annotation.Nullable
		private SerializablePropertyMutator<TRGT, SRC> combiner;
		
		/**
		 * Source getter on target for bidirectionality (no consequence on database mapping).
		 */
		@javax.annotation.Nullable
		private SerializablePropertyAccessor<TRGT, S> accessor;
		
		/**
		 * Source setter on target for bidirectionality (no consequence on database mapping).
		 */
		@javax.annotation.Nullable
		private SerializablePropertyMutator<TRGT, S> mutator;
		
		/** Optional provider of collection instance to be used if collection value is null */
		@javax.annotation.Nullable
		private Supplier<S> factory;
		
		@javax.annotation.Nullable
		public SerializablePropertyMutator<TRGT, SRC> getCombiner() {
			return combiner;
		}
		
		public void setCombiner(@javax.annotation.Nullable SerializablePropertyMutator<TRGT, SRC> combiner) {
			this.combiner = combiner;
		}
		
		@javax.annotation.Nullable
		public SerializablePropertyAccessor<TRGT, S> getAccessor() {
			return accessor;
		}
		
		public void setAccessor(@javax.annotation.Nullable SerializablePropertyAccessor<TRGT, S> accessor) {
			this.accessor = accessor;
		}
		
		@javax.annotation.Nullable
		public SerializablePropertyMutator<TRGT, S> getMutator() {
			return mutator;
		}
		
		public void setMutator(@javax.annotation.Nullable SerializablePropertyMutator<TRGT, S> mutator) {
			this.mutator = mutator;
		}
		
		@javax.annotation.Nullable
		public Supplier<S> getFactory() {
			return factory;
		}
		
		public void setFactory(@javax.annotation.Nullable Supplier<S> factory) {
			this.factory = factory;
		}
		
		public boolean isEmpty() {
			return accessor == null && mutator == null && factory == null && combiner == null;
		}
	}
}
