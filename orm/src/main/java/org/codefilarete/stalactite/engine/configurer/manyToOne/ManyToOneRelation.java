package org.codefilarete.stalactite.engine.configurer.manyToOne;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorByMethod;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.MethodReferenceCapturer;
import org.codefilarete.reflection.MutatorByMethod;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.PolymorphismPolicy;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.tool.Nullable;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * @author Guillaume Mary
 */
public class ManyToOneRelation<SRC, TRGT, TRGTID, C extends Collection<SRC>> {
	
	/** The method that gives the target entity from the source one */
	private final ReversibleAccessor<SRC, TRGT> targetProvider;
	
	/** Configuration used for target beans persistence */
	private final EntityMappingConfigurationProvider<TRGT, TRGTID> targetMappingConfiguration;
	
	private final BooleanSupplier sourceTablePerClassPolymorphic;
	
	private boolean nullable = true;
	
	/** Default relation mode is {@link RelationMode#ALL} */
	private RelationMode relationMode = RelationMode.ALL;
	
	private final MappedByConfiguration<SRC, TRGT, C> mappedByConfiguration = new MappedByConfiguration<>();
	
	/**
	 * Indicates that relation must be loaded in same main query (through join) or in some separate query
	 */
	private boolean fetchSeparately;
	
	@javax.annotation.Nullable
	private String columnName;
	
	/**
	 *
	 * @param targetProvider provider of the property to be persisted
	 * @param sourceTablePerClassPolymorphic indicates that source is table-per-class polymorphic
	 * @param targetMappingConfiguration persistence configuration provider of entities stored in the target collection
	 */
	public ManyToOneRelation(ReversibleAccessor<SRC, TRGT> targetProvider,
							 BooleanSupplier sourceTablePerClassPolymorphic,
							 EntityMappingConfigurationProvider<? extends TRGT, TRGTID> targetMappingConfiguration) {
		this.sourceTablePerClassPolymorphic = sourceTablePerClassPolymorphic;
		this.targetMappingConfiguration = (EntityMappingConfigurationProvider<TRGT, TRGTID>) targetMappingConfiguration;
		this.targetProvider = targetProvider;
	}
	
	/** Original method reference given for mapping */
	public ReversibleAccessor<SRC, TRGT> getTargetProvider() {
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
	
	public MappedByConfiguration<SRC, TRGT, C> getMappedByConfiguration() {
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
	
	/**
	 * Build the accessor for the reverse property, made of configured getter and setter. If one of them is unavailable, it's deduced from the
	 * present one. If both are absent, null is returned.
	 *
	 * @return null if no getter nor setter were defined
	 */
	@javax.annotation.Nullable
	PropertyAccessor<TRGT, C> buildReversePropertyAccessor() {
		Nullable<AccessorByMethodReference<TRGT, C>> getterReference = nullable(mappedByConfiguration.getAccessor()).map(Accessors::accessorByMethodReference);
		Nullable<MutatorByMethodReference<TRGT, C>> setterReference = nullable(mappedByConfiguration.getMutator()).map(Accessors::mutatorByMethodReference);
		if (getterReference.isAbsent() && setterReference.isAbsent()) {
			return null;
		} else if (getterReference.isPresent() && setterReference.isPresent()) {
			// we keep close to user demand : we keep its method references
			return new PropertyAccessor<>(getterReference.get(), setterReference.get());
		} else if (getterReference.isPresent() && setterReference.isAbsent()) {
			// we keep close to user demand : we keep its method reference ...
			// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
			return new PropertyAccessor<>(getterReference.get(), new AccessorByMethod<TRGT, C>(captureMethod(mappedByConfiguration.getAccessor())).toMutator());
		} else {
			// we keep close to user demand : we keep its method reference ...
			// ... but we can't do it for getter, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
			return new PropertyAccessor<>(new MutatorByMethod<TRGT, C>(captureMethod(mappedByConfiguration.getMutator())).toAccessor(), setterReference.get());
		}
	}
	
	private final MethodReferenceCapturer methodSpy = new MethodReferenceCapturer();
	
	private Method captureMethod(SerializableFunction getter) {
		return this.methodSpy.findMethod(getter);
	}
	
	private Method captureMethod(SerializableBiConsumer setter) {
		return this.methodSpy.findMethod(setter);
	}
	
	/**
	 * Clones this object to create a new one with the given accessor as prefix of current one.
	 * Made to "slide" current instance with an accessor prefix. Used for embeddable objects with relation to make the relation being accessible
	 * from the "root" entity.
	 *
	 * @param accessor the prefix of the clone to be created
	 * @return a clones of this instance prefixed with the given accessor
	 * @param <E> the root entity type that owns the embeddable which has this relation
	 */
	public <E, CC extends Collection<E>> ManyToOneRelation<E, TRGT, TRGTID, CC> embedInto(Accessor<E, SRC> accessor) {
		AccessorChain<E, TRGT> slidedTargetProvider = new AccessorChain<>(accessor, targetProvider);
		ManyToOneRelation<E, TRGT, TRGTID, CC> result = new ManyToOneRelation<>(slidedTargetProvider, this::isSourceTablePerClassPolymorphic, this::getTargetMappingConfiguration);
		result.setRelationMode(this.getRelationMode());
		result.setNullable(this.isNullable());
		result.setFetchSeparately(this.isFetchSeparately());
		result.setColumnName(this.getColumnName());
		return result;
	}
	
	public static class MappedByConfiguration<SRC, TRGT, C2 extends Collection<SRC>> {
		
		/**
		 * Combiner of target entity with source entity
		 */
		@javax.annotation.Nullable
		private SerializableBiConsumer<TRGT, SRC> combiner;
		
		/**
		 * Source getter on target for bidirectionality (no consequence on database mapping).
		 */
		@javax.annotation.Nullable
		private SerializableFunction<TRGT, C2> accessor;
		
		/**
		 * Source setter on target for bidirectionality (no consequence on database mapping).
		 */
		@javax.annotation.Nullable
		private SerializableBiConsumer<TRGT, C2> mutator;
		
		/** Optional provider of collection instance to be used if collection value is null */
		@javax.annotation.Nullable
		private Supplier<C2> factory;
		
		@javax.annotation.Nullable
		public SerializableBiConsumer<TRGT, SRC> getCombiner() {
			return combiner;
		}
		
		public void setCombiner(@javax.annotation.Nullable SerializableBiConsumer<TRGT, SRC> combiner) {
			this.combiner = combiner;
		}
		
		@javax.annotation.Nullable
		public SerializableFunction<TRGT, C2> getAccessor() {
			return accessor;
		}
		
		public void setAccessor(@javax.annotation.Nullable SerializableFunction<TRGT, C2> accessor) {
			this.accessor = accessor;
		}
		
		@javax.annotation.Nullable
		public SerializableBiConsumer<TRGT, C2> getMutator() {
			return mutator;
		}
		
		public void setMutator(@javax.annotation.Nullable SerializableBiConsumer<TRGT, C2> mutator) {
			this.mutator = mutator;
		}
		
		@javax.annotation.Nullable
		public Supplier<C2> getFactory() {
			return factory;
		}
		
		public void setFactory(@javax.annotation.Nullable Supplier<C2> factory) {
			this.factory = factory;
		}
		
		public boolean isEmpty() {
			return accessor == null && mutator == null && factory == null && combiner == null;
		}
	}
}
