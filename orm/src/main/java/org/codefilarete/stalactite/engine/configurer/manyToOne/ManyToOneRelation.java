package org.codefilarete.stalactite.engine.configurer.manyToOne;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.function.BooleanSupplier;

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
import org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelation.MappedByConfiguration;
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
	
	public MappedByConfiguration getMappedByConfiguration() {
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
		Nullable<AccessorByMethodReference<TRGT, C>> getterReference = nullable(mappedByConfiguration.getReverseCollectionAccessor()).map(Accessors::accessorByMethodReference);
		Nullable<MutatorByMethodReference<TRGT, C>> setterReference = nullable(mappedByConfiguration.getReverseCollectionMutator()).map(Accessors::mutatorByMethodReference);
		if (getterReference.isAbsent() && setterReference.isAbsent()) {
			return null;
		} else if (getterReference.isPresent() && setterReference.isPresent()) {
			// we keep close to user demand : we keep its method references
			return new PropertyAccessor<>(getterReference.get(), setterReference.get());
		} else if (getterReference.isPresent() && setterReference.isAbsent()) {
			// we keep close to user demand : we keep its method reference ...
			// ... but we can't do it for mutator, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
			return new PropertyAccessor<>(getterReference.get(), new AccessorByMethod<TRGT, C>(captureMethod(mappedByConfiguration.getReverseCollectionAccessor())).toMutator());
		} else {
			// we keep close to user demand : we keep its method reference ...
			// ... but we can't do it for getter, so we use the most equivalent manner : a mutator based on setter method (fallback to property if not present)
			return new PropertyAccessor<>(new MutatorByMethod<TRGT, C>(captureMethod(mappedByConfiguration.getReverseCollectionMutator())).toAccessor(), setterReference.get());
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
		return result;
	}
}
