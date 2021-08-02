package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.engine.configurer.FluentEmbeddableMappingConfigurationSupport;
import org.gama.stalactite.persistence.engine.configurer.FluentEntityMappingConfigurationSupport;
import org.gama.stalactite.persistence.engine.configurer.FluentSubEntityMappingConfigurationSupport;
import org.gama.stalactite.persistence.engine.runtime.JoinedTablesPersister;
import org.gama.stalactite.persistence.mapping.EmbeddedClassMappingStrategy;

/**
 * Declares a simple entry point to start configuring a persistence mapping.
 * 
 * @author Guillaume Mary
 */
public final class MappingEase {
	
	/**
	 * Starts a {@link FluentEntityMappingBuilder} for a given class.
	 *
	 * @param classToPersist the class to be persisted by the {@link JoinedTablesPersister}
	 * 						 that will be created by {@link FluentEntityMappingBuilder#build(PersistenceContext)}
	 * @param identifierType entity identifier type
	 * @param <T> any type to be persisted
	 * @param <I> the type of the identifier
	 * @return a new {@link FluentEntityMappingBuilder}
	 */
	@SuppressWarnings("squid:S1172")	// identifierType is used to sign result
	public static <T, I> FluentEntityMappingBuilder<T, I> entityBuilder(Class<T> classToPersist, Class<I> identifierType) {
		return new FluentEntityMappingConfigurationSupport<>(classToPersist);
	}
	
	public static <T, I> FluentSubEntityMappingConfiguration<T, Object> subentityBuilder(Class<T> classToPersist) {
		return new FluentSubEntityMappingConfigurationSupport<>(classToPersist);
	}
	
	public static <T, I> FluentSubEntityMappingConfiguration<T, I> subentityBuilder(Class<T> classToPersist, Class<I> identifierType) {
		return new FluentSubEntityMappingConfigurationSupport<>(classToPersist);
	}
	
	/**
	 * Starts a {@link FluentEmbeddableMappingBuilder} for a given class.
	 *
	 * @param persistedClass the class to be persisted by the {@link EmbeddedClassMappingStrategy}
	 * @param <T> any type to be persisted
	 * @return a new {@link FluentEmbeddableMappingBuilder}
	 */
	public static <T> FluentEmbeddableMappingBuilder<T> embeddableBuilder(Class<T> persistedClass) {
		return new FluentEmbeddableMappingConfigurationSupport<>(persistedClass);
	}
	
	private MappingEase() {
		// tool class
	}
}
