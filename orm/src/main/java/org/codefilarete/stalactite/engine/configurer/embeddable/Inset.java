package org.codefilarete.stalactite.engine.configurer.embeddable;

import java.lang.reflect.Method;

import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.reflection.ReadWriteAccessPoint;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.reflection.SerializableAccessor;
import org.codefilarete.reflection.SerializableMutator;
import org.codefilarete.reflection.SerializablePropertyAccessor;
import org.codefilarete.reflection.SerializablePropertyMutator;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.reflection.ValueAccessPointSet;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.embeddable.FluentEmbeddableMappingConfiguration;
import org.codefilarete.stalactite.engine.configurer.LambdaMethodUnsheller;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.tool.Reflections;

/**
 * Information storage of embedded mapping defined externally by an {@link EmbeddableMappingConfigurationProvider},
 * see {@link FluentEmbeddableMappingConfiguration#embed(org.codefilarete.reflection.SerializablePropertyAccessor, EmbeddableMappingConfigurationProvider)}
 *
 * @param <SRC>
 * @param <TRGT>
 * @see FluentEmbeddableMappingConfiguration#embed(org.codefilarete.reflection.SerializablePropertyAccessor, EmbeddableMappingConfigurationProvider) }
 * @see FluentEmbeddableMappingConfiguration#embed(org.codefilarete.reflection.SerializablePropertyMutator, EmbeddableMappingConfigurationProvider) }
 */
public class Inset<SRC, TRGT> {
	
	static <SRC, TRGT> Inset<SRC, TRGT> fromSetter(SerializablePropertyMutator<SRC, TRGT> targetSetter,
												   EmbeddableMappingConfigurationProvider<? extends TRGT> beanMappingBuilder,
												   LambdaMethodUnsheller lambdaMethodUnsheller) {
		Method insetAccessor = lambdaMethodUnsheller.captureLambdaMethod(targetSetter);
		return new Inset<>(insetAccessor,
				Accessors.readWriteAccessPoint(targetSetter),
				beanMappingBuilder);
	}
	
	static <SRC, TRGT> Inset<SRC, TRGT> fromGetter(SerializablePropertyAccessor<SRC, TRGT> targetGetter,
												   EmbeddableMappingConfigurationProvider<? extends TRGT> beanMappingBuilder,
												   LambdaMethodUnsheller lambdaMethodUnsheller) {
		Method insetAccessor = lambdaMethodUnsheller.captureLambdaMethod(targetGetter);
		return new Inset<>(insetAccessor,
				Accessors.readWriteAccessPoint(targetGetter),
				beanMappingBuilder);
	}
	
	private final Class<TRGT> embeddedClass;
	private final Method insetAccessor;
	/**
	 * Equivalent of {@link #insetAccessor} as a {@link ReadWriteAccessPoint}
	 */
	private final ReadWritePropertyAccessPoint<SRC, TRGT> accessor;
	private final ValueAccessPointMap<SRC, String, ValueAccessPoint<SRC>> overriddenColumnNames = new ValueAccessPointMap<>();
	private final ValueAccessPointMap<SRC, Size, ValueAccessPoint<SRC>> overriddenColumnSizes = new ValueAccessPointMap<>();
	private final ValueAccessPointSet<SRC, ValueAccessPoint<SRC>> excludedProperties = new ValueAccessPointSet<>();
	private final EmbeddableMappingConfigurationProvider<? extends TRGT> configurationProvider;
	private final ValueAccessPointMap<SRC, Column, ValueAccessPoint<SRC>> overriddenColumns = new ValueAccessPointMap<>();
	
	Inset(SerializablePropertyMutator<SRC, TRGT> targetSetter,
		  EmbeddableMappingConfigurationProvider<? extends TRGT> configurationProvider,
		  LambdaMethodUnsheller lambdaMethodUnsheller) {
		this.insetAccessor = lambdaMethodUnsheller.captureLambdaMethod(targetSetter);
		this.accessor = Accessors.readWriteAccessPoint(targetSetter);
		// looking for the target type because it's necessary to find its persister (and other objects)
		this.embeddedClass = Reflections.javaBeanTargetType(getInsetAccessor());
		this.configurationProvider = configurationProvider;
	}
	
	Inset(Method insetAccessor,
		  ReadWritePropertyAccessPoint<SRC, TRGT> accessor,
		  EmbeddableMappingConfigurationProvider<? extends TRGT> configurationProvider) {
		this.insetAccessor = insetAccessor;
		// looking for the target type because it's necessary to find its persister (and other objects)
		this.embeddedClass = Reflections.javaBeanTargetType(insetAccessor);
		this.accessor = accessor;
		this.configurationProvider = configurationProvider;
	}
	
	/**
	 * Equivalent of {@link #insetAccessor} as a {@link ReadWriteAccessPoint}
	 */
	public ReadWritePropertyAccessPoint<SRC, TRGT> getAccessor() {
		return accessor;
	}
	
	/**
	 * Equivalent of given getter or setter at construction time as a {@link Method}
	 */
	public Method getInsetAccessor() {
		return insetAccessor;
	}
	
	public Class<TRGT> getEmbeddedClass() {
		return embeddedClass;
	}
	
	public ValueAccessPointSet<SRC, ValueAccessPoint<SRC>> getExcludedProperties() {
		return this.excludedProperties;
	}
	
	public ValueAccessPointMap<SRC, String, ValueAccessPoint<SRC>> getOverriddenColumnNames() {
		return this.overriddenColumnNames;
	}
	
	public ValueAccessPointMap<SRC, Size, ValueAccessPoint<SRC>> getOverriddenColumnSizes() {
		return overriddenColumnSizes;
	}
	
	public ValueAccessPointMap<SRC, Column, ValueAccessPoint<SRC>> getOverriddenColumns() {
		return overriddenColumns;
	}
	
	public EmbeddableMappingConfigurationProvider<TRGT> getConfigurationProvider() {
		return (EmbeddableMappingConfigurationProvider<TRGT>) configurationProvider;
	}
	
	public void overrideName(SerializableAccessor methodRef, String columnName) {
		this.overriddenColumnNames.put(new AccessorByMethodReference(methodRef), columnName);
	}
	
	public void overrideName(SerializableMutator methodRef, String columnName) {
		this.overriddenColumnNames.put(new MutatorByMethodReference(methodRef), columnName);
	}
	
	public void overrideName(AccessorChain accessorChain, String columnName) {
		this.overriddenColumnNames.put(accessorChain, columnName);
	}
	
	public void overrideSize(SerializableAccessor methodRef, Size columnSize) {
		this.overriddenColumnSizes.put(new AccessorByMethodReference(methodRef), columnSize);
	}
	
	public void overrideSize(SerializableMutator methodRef, Size columnSize) {
		this.overriddenColumnSizes.put(new MutatorByMethodReference<>(methodRef), columnSize);
	}
	
	public void overrideSize(AccessorChain accessorChain, Size columnSize) {
		this.overriddenColumnSizes.put(accessorChain, columnSize);
	}
	
	public void override(SerializableAccessor methodRef, Column column) {
		this.overriddenColumns.put(new AccessorByMethodReference(methodRef), column);
	}
	
	public void override(SerializableMutator methodRef, Column column) {
		this.overriddenColumns.put(new MutatorByMethodReference(methodRef), column);
	}
	
	public void exclude(SerializableMutator methodRef) {
		this.excludedProperties.add(new MutatorByMethodReference(methodRef));
	}
	
	public void exclude(SerializableAccessor methodRef) {
		this.excludedProperties.add(new AccessorByMethodReference(methodRef));
	}
}
