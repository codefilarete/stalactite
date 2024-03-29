package org.codefilarete.stalactite.engine;

import org.codefilarete.stalactite.engine.configurer.FluentCompositeKeyMappingConfigurationSupport;
import org.codefilarete.stalactite.engine.configurer.FluentEmbeddableMappingConfigurationSupport;
import org.codefilarete.stalactite.engine.configurer.FluentEntityMappingConfigurationSupport;
import org.codefilarete.stalactite.engine.configurer.FluentSubEntityMappingConfigurationSupport;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.mapping.EmbeddedClassMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * Declares a simple entry point to start configuring a persistence mapping.
 * 
 * @author Guillaume Mary
 */
public final class MappingEase {
	
	/**
	 * Starts a {@link FluentEntityMappingBuilder} for a given class.
	 *
	 * @param classToPersist the class to be persisted by the {@link SimpleRelationalEntityPersister}
	 * 						 that will be created by {@link FluentEntityMappingBuilder#build(PersistenceContext)}
	 * @param identifierType entity identifier type
	 * @param <T> any type to be persisted
	 * @param <I> the type of the identifier
	 * @return a new {@link FluentEntityMappingBuilder}
	 */
	@SuppressWarnings("squid:S1172")	// identifierType is used to sign result
	public static <T, I> FluentEntityMappingBuilder<T, I> entityBuilder(Class<T> classToPersist, Class<I> identifierType) {
		return entityBuilder(classToPersist, identifierType, null);
	}
	
	public static <T, I> FluentEntityMappingBuilder<T, I> entityBuilder(Class<T> classToPersist, Class<I> identifierType, Table<?> targetTable) {
		return new FluentEntityMappingConfigurationSupport<>(classToPersist, targetTable);
	}
	
	public static <T, I> FluentSubEntityMappingBuilder<T, Object> subentityBuilder(Class<T> classToPersist) {
		return new FluentSubEntityMappingConfigurationSupport<>(classToPersist);
	}
	
	public static <T, I> FluentSubEntityMappingBuilder<T, I> subentityBuilder(Class<T> classToPersist, Class<I> identifierType) {
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
	 * To be used with {@link FluentEntityMappingBuilder#mapCompositeKey(SerializableFunction, CompositeKeyMappingConfigurationProvider)}
	 *
	 * @param persistedClass the class to be persisted by the {@link EmbeddedClassMapping}
	 * @param <T> any type to be persisted
	 * @return a new {@link FluentEmbeddableMappingBuilder}
	 */
	public static <T> FluentCompositeKeyMappingBuilder<T> compositeKeyBuilder(Class<T> persistedClass) {
		return new FluentCompositeKeyMappingConfigurationSupport<>(persistedClass);
	}
	
	private MappingEase() {
		// tool class
	}
}
