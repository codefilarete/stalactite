package org.codefilarete.stalactite.engine.configurer.embeddable;

import java.lang.reflect.Method;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorByMethod;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.MutatorByMethod;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.reflection.ValueAccessPointSet;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfigurationProvider;
import org.codefilarete.stalactite.engine.configurer.LambdaMethodUnsheller;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.tool.Reflections;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * Information storage of embedded mapping defined externally by an {@link EmbeddableMappingConfigurationProvider},
 * see {@link FluentEmbeddableMappingConfigurationSupport#embed(SerializableFunction, EmbeddableMappingConfigurationProvider)}
 *
 * @param <SRC>
 * @param <TRGT>
 * @see FluentEmbeddableMappingConfigurationSupport#embed(SerializableFunction, EmbeddableMappingConfigurationProvider)}
 * @see FluentEmbeddableMappingConfigurationSupport#embed(SerializableBiConsumer, EmbeddableMappingConfigurationProvider)}
 */
public class Inset<SRC, TRGT> {
	
	static <SRC, TRGT> Inset<SRC, TRGT> fromSetter(SerializableBiConsumer<SRC, TRGT> targetSetter,
												   EmbeddableMappingConfigurationProvider<? extends TRGT> beanMappingBuilder,
												   LambdaMethodUnsheller lambdaMethodUnsheller) {
		Method insetAccessor = lambdaMethodUnsheller.captureLambdaMethod(targetSetter);
		return new Inset<>(insetAccessor,
				new PropertyAccessor<>(
						new MutatorByMethod<SRC, TRGT>(insetAccessor).toAccessor(),
						new MutatorByMethodReference<>(targetSetter)),
				beanMappingBuilder);
	}
	
	static <SRC, TRGT> Inset<SRC, TRGT> fromGetter(SerializableFunction<SRC, TRGT> targetGetter,
												   EmbeddableMappingConfigurationProvider<? extends TRGT> beanMappingBuilder,
												   LambdaMethodUnsheller lambdaMethodUnsheller) {
		Method insetAccessor = lambdaMethodUnsheller.captureLambdaMethod(targetGetter);
		return new Inset<>(insetAccessor,
				new PropertyAccessor<>(
						new AccessorByMethodReference<>(targetGetter),
						new AccessorByMethod<SRC, TRGT>(insetAccessor).toMutator()),
				beanMappingBuilder);
	}
	
	private final Class<TRGT> embeddedClass;
	private final Method insetAccessor;
	/**
	 * Equivalent of {@link #insetAccessor} as a {@link PropertyAccessor}
	 */
	private final Accessor<SRC, TRGT> accessor;
	private final ValueAccessPointMap<SRC, String> overriddenColumnNames = new ValueAccessPointMap<>();
	private final ValueAccessPointMap<SRC, Size> overriddenColumnSizes = new ValueAccessPointMap<>();
	private final ValueAccessPointSet<SRC> excludedProperties = new ValueAccessPointSet<>();
	private final EmbeddableMappingConfigurationProvider<? extends TRGT> configurationProvider;
	private final ValueAccessPointMap<SRC, Column> overriddenColumns = new ValueAccessPointMap<>();
	
	Inset(SerializableBiConsumer<SRC, TRGT> targetSetter,
		  EmbeddableMappingConfigurationProvider<? extends TRGT> configurationProvider,
		  LambdaMethodUnsheller lambdaMethodUnsheller) {
		this.insetAccessor = lambdaMethodUnsheller.captureLambdaMethod(targetSetter);
		this.accessor = new PropertyAccessor<>(
				new MutatorByMethod<SRC, TRGT>(insetAccessor).toAccessor(),
				new MutatorByMethodReference<>(targetSetter));
		// looking for the target type because it's necessary to find its persister (and other objects)
		this.embeddedClass = Reflections.javaBeanTargetType(getInsetAccessor());
		this.configurationProvider = configurationProvider;
	}
	
	Inset(Method insetAccessor,
		  Accessor<SRC, TRGT> accessor,
		  EmbeddableMappingConfigurationProvider<? extends TRGT> configurationProvider) {
		this.insetAccessor = insetAccessor;
		// looking for the target type because it's necessary to find its persister (and other objects)
		this.embeddedClass = Reflections.javaBeanTargetType(insetAccessor);
		this.accessor = accessor;
		this.configurationProvider = configurationProvider;
	}
	
	/**
	 * Equivalent of {@link #insetAccessor} as a {@link PropertyAccessor}
	 */
	public Accessor<SRC, TRGT> getAccessor() {
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
	
	public ValueAccessPointSet<SRC> getExcludedProperties() {
		return this.excludedProperties;
	}
	
	public ValueAccessPointMap<SRC, String> getOverriddenColumnNames() {
		return this.overriddenColumnNames;
	}
	
	public ValueAccessPointMap<SRC, Size> getOverriddenColumnSizes() {
		return overriddenColumnSizes;
	}
	
	public ValueAccessPointMap<SRC, Column> getOverriddenColumns() {
		return overriddenColumns;
	}
	
	public EmbeddableMappingConfigurationProvider<TRGT> getConfigurationProvider() {
		return (EmbeddableMappingConfigurationProvider<TRGT>) configurationProvider;
	}
	
	public void overrideName(SerializableFunction methodRef, String columnName) {
		this.overriddenColumnNames.put(new AccessorByMethodReference(methodRef), columnName);
	}
	
	public void overrideName(SerializableBiConsumer methodRef, String columnName) {
		this.overriddenColumnNames.put(new MutatorByMethodReference(methodRef), columnName);
	}
	
	public void overrideName(AccessorChain accessorChain, String columnName) {
		this.overriddenColumnNames.put(accessorChain, columnName);
	}
	
	public void overrideSize(SerializableFunction methodRef, Size columnSize) {
		this.overriddenColumnSizes.put(new AccessorByMethodReference(methodRef), columnSize);
	}
	
	public void overrideSize(SerializableBiConsumer methodRef, Size columnSize) {
		this.overriddenColumnSizes.put(new MutatorByMethodReference<>(methodRef), columnSize);
	}
	
	public void overrideSize(AccessorChain accessorChain, Size columnSize) {
		this.overriddenColumnSizes.put(accessorChain, columnSize);
	}
	
	public void override(SerializableFunction methodRef, Column column) {
		this.overriddenColumns.put(new AccessorByMethodReference(methodRef), column);
	}
	
	public void override(SerializableBiConsumer methodRef, Column column) {
		this.overriddenColumns.put(new MutatorByMethodReference(methodRef), column);
	}
	
	public void exclude(SerializableBiConsumer methodRef) {
		this.excludedProperties.add(new MutatorByMethodReference(methodRef));
	}
	
	public void exclude(SerializableFunction methodRef) {
		this.excludedProperties.add(new AccessorByMethodReference(methodRef));
	}
}
