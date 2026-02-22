package org.codefilarete.stalactite.dsl;

import org.codefilarete.stalactite.dsl.embeddable.FluentEmbeddableMappingBuilder;
import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.key.CompositeKeyMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.key.FluentCompositeKeyMappingBuilder;
import org.codefilarete.stalactite.dsl.subentity.FluentSubEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.subentity.SubEntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.subentity.SubEntityMappingConfigurationProvider;
import org.codefilarete.stalactite.engine.configurer.FluentCompositeKeyMappingConfigurationSupport;
import org.codefilarete.stalactite.engine.configurer.embeddable.FluentEmbeddableMappingConfigurationSupport;
import org.codefilarete.stalactite.engine.configurer.entity.FluentEntityMappingConfigurationSupport;
import org.codefilarete.stalactite.engine.configurer.FluentSubEntityMappingConfigurationSupport;
import org.codefilarete.stalactite.mapping.EmbeddedClassMapping;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * A utility class that offers static factory methods as entry points to start configuring an class persistence mapping.
 *
 * @author Guillaume Mary
 */
public final class FluentMappings {

	/**
	 * Starts a {@link FluentEntityMappingBuilder} for a given class.
	 *
	 * @param classToPersist the class to be persisted
	 * @param identifierType entity identifier type
	 * @param <T> any type to be persisted
	 * @param <I> the type of the identifier
	 * @return a new {@link FluentEntityMappingBuilder}
	 */
	@SuppressWarnings("squid:S1172")	// identifierType is used to sign result
	public static <T, I> FluentEntityMappingBuilder<T, I> entityBuilder(Class<T> classToPersist, Class<I> identifierType) {
		// Note that we don't use identifierType, but it's necessary to generic type of returned instance
		return new FluentEntityMappingConfigurationSupport<>(classToPersist);
	}

	/**
	 * Starts a {@link FluentSubEntityMappingBuilder} for a given class.
	 *
	 * @param classToPersist the class to be persisted
	 * @param <T> any type to be persisted
	 * @param <I> the type of the identifier
	 * @return a new {@link FluentSubEntityMappingBuilder}
	 * @see PolymorphismPolicy.TablePerClassPolymorphism#addSubClass(SubEntityMappingConfigurationProvider)
	 * @see PolymorphismPolicy.JoinTablePolymorphism#addSubClass(SubEntityMappingConfigurationProvider)
	 * @see PolymorphismPolicy.SingleTablePolymorphism#addSubClass(SubEntityMappingConfiguration, Object)
	 */
	public static <T, I> FluentSubEntityMappingBuilder<T, Object> subentityBuilder(Class<T> classToPersist) {
		return new FluentSubEntityMappingConfigurationSupport<>(classToPersist);
	}

	/**
	 * Starts a {@link FluentSubEntityMappingBuilder} for a given class.
	 *
	 * @param classToPersist the class to be persisted
	 * @param identifierType entity identifier type
	 * @param <T> any type to be persisted
	 * @param <I> the type of the identifier
	 * @return a new {@link FluentSubEntityMappingBuilder}
	 * @see PolymorphismPolicy.TablePerClassPolymorphism#addSubClass(SubEntityMappingConfigurationProvider)
	 * @see PolymorphismPolicy.JoinTablePolymorphism#addSubClass(SubEntityMappingConfigurationProvider)
	 * @see PolymorphismPolicy.SingleTablePolymorphism#addSubClass(SubEntityMappingConfiguration, Object)
	 */
	public static <T, I> FluentSubEntityMappingBuilder<T, I> subentityBuilder(Class<T> classToPersist, Class<I> identifierType) {
		// Note that we don't use identifierType, but it's necessary to generic type of returned instance
		return new FluentSubEntityMappingConfigurationSupport<>(classToPersist);
	}

	/**
	 * Starts a {@link FluentEmbeddableMappingBuilder} for a given class.
	 *
	 * @param persistedClass the class to be persisted by the {@link EmbeddedClassMapping}
	 * @param <T> any type to be persisted
	 * @return a new {@link FluentEmbeddableMappingBuilder}
	 */
	public static <T> FluentEmbeddableMappingBuilder<T> embeddableBuilder(Class<T> persistedClass) {
		return new FluentEmbeddableMappingConfigurationSupport<>(persistedClass);
	}

	/**
	 * Starts a {@link FluentCompositeKeyMappingBuilder} for a given class.
	 * To be used with {@link FluentEntityMappingBuilder#mapKey(SerializableFunction, CompositeKeyMappingConfigurationProvider, java.util.function.Consumer, java.util.function.Function)}
	 *
	 * @param persistedClass the class to be persisted by the {@link EmbeddedClassMapping}
	 * @param <T> any type to be persisted
	 * @return a new {@link FluentEmbeddableMappingBuilder}
	 */
	public static <T> FluentCompositeKeyMappingBuilder<T> compositeKeyBuilder(Class<T> persistedClass) {
		return new FluentCompositeKeyMappingConfigurationSupport<>(persistedClass);
	}

	private FluentMappings() {
		// tool class
	}
}
