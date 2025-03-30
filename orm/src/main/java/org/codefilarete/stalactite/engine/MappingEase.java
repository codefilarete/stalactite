package org.codefilarete.stalactite.engine;

import javax.annotation.Nullable;

import org.codefilarete.stalactite.engine.configurer.FluentCompositeKeyMappingConfigurationSupport;
import org.codefilarete.stalactite.engine.configurer.FluentEmbeddableMappingConfigurationSupport;
import org.codefilarete.stalactite.engine.configurer.FluentEntityMappingConfigurationSupport;
import org.codefilarete.stalactite.engine.configurer.FluentSubEntityMappingConfigurationSupport;
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
	 * @param classToPersist the class to be persisted
	 * @param identifierType entity identifier type
	 * @param <T> any type to be persisted
	 * @param <I> the type of the identifier
	 * @return a new {@link FluentEntityMappingBuilder}
	 */
	@SuppressWarnings("squid:S1172")	// identifierType is used to sign result
	public static <T, I> FluentEntityMappingBuilder<T, I> entityBuilder(Class<T> classToPersist, Class<I> identifierType) {
		return entityBuilder(classToPersist, identifierType, (Table) null);
	}
	
	/**
	 * Starts a {@link FluentEntityMappingBuilder} for a given class.
	 *
	 * @param classToPersist the class to be persisted
	 * @param identifierType entity identifier type
	 * @param targetTableName table name to store the entity in
	 * @param <T> any type to be persisted
	 * @param <I> the type of the identifier
	 * @return a new {@link FluentEntityMappingBuilder}
	 */
	public static <T, I> FluentEntityMappingBuilder<T, I> entityBuilder(Class<T> classToPersist, Class<I> identifierType, @Nullable String targetTableName) {
		// Note that we don't use identifierType, but it's necessary to generic type of returned instance
		return new FluentEntityMappingConfigurationSupport<>(classToPersist, targetTableName);
	}
	
	/**
	 * Starts a {@link FluentEntityMappingBuilder} for a given class.
	 *
	 * @param classToPersist the class to be persisted
	 * @param identifierType entity identifier type
	 * @param targetTable table name to store the entity in, mapped property will be added to it as columns
	 * @param <T> any type to be persisted
	 * @param <I> the type of the identifier
	 * @return a new {@link FluentEntityMappingBuilder}
	 */
	public static <T, I> FluentEntityMappingBuilder<T, I> entityBuilder(Class<T> classToPersist, Class<I> identifierType, @Nullable Table<?> targetTable) {
		// Note that we don't use identifierType, but it's necessary to generic type of returned instance
		return new FluentEntityMappingConfigurationSupport<>(classToPersist, targetTable);
	}
	
	/**
	 * Starts a {@link FluentSubEntityMappingBuilder} for a given class.
	 *
	 * @param classToPersist the class to be persisted
	 * @param <T> any type to be persisted
	 * @param <I> the type of the identifier
	 * @return a new {@link FluentSubEntityMappingBuilder}
	 * @see org.codefilarete.stalactite.engine.PolymorphismPolicy.TablePerClassPolymorphism#addSubClass(SubEntityMappingConfigurationProvider)
	 * @see org.codefilarete.stalactite.engine.PolymorphismPolicy.JoinTablePolymorphism#addSubClass(SubEntityMappingConfigurationProvider)
	 * @see org.codefilarete.stalactite.engine.PolymorphismPolicy.SingleTablePolymorphism#addSubClass(SubEntityMappingConfiguration, Object) 
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
	 * @see org.codefilarete.stalactite.engine.PolymorphismPolicy.TablePerClassPolymorphism#addSubClass(SubEntityMappingConfigurationProvider)
	 * @see org.codefilarete.stalactite.engine.PolymorphismPolicy.JoinTablePolymorphism#addSubClass(SubEntityMappingConfigurationProvider)
	 * @see org.codefilarete.stalactite.engine.PolymorphismPolicy.SingleTablePolymorphism#addSubClass(SubEntityMappingConfiguration, Object)
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
	 * To be used with {@link FluentEntityMappingBuilder#mapCompositeKey(SerializableFunction, CompositeKeyMappingConfigurationProvider, java.util.function.Consumer, java.util.function.Function)}
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
