package org.codefilarete.stalactite.dsl.property;

import java.util.Map;
import java.util.function.Supplier;

import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * API to define a Map relation
 * 
 * @author Guillaume Mary
 */
public interface MapOptions<K, V, M extends Map<K, V>> {
	
	MapOptions<K, V, M> reverseJoinColumn(String columnName);
	
	MapOptions<K, V, M> keyColumn(String columnName);
	
	MapOptions<K, V, M> keySize(Size columnSize);
	
	MapOptions<K, V, M> valueColumn(String columnName);
	
	MapOptions<K, V, M> valueSize(Size columnSize);
	
	MapOptions<K, V, M> initializeWith(Supplier<? extends M> collectionFactory);
	
	MapOptions<K, V, M> onTable(String tableName);
	
	MapOptions<K, V, M> onTable(Table table);
	
	EntityInMapOptions<K, V, M> withKeyMapping(EntityMappingConfigurationProvider<K, ?> mappingConfigurationProvider);
	
	/**
	 * Indicates mapping to be used for {@link Map} keys when it's a bean type.
	 * Bean properties will be part of table primary key.
	 * 
	 * @param mappingConfigurationProvider properties mapping for key type
	 */
	EmbeddableInMapOptions<K> withKeyMapping(EmbeddableMappingConfigurationProvider<K> mappingConfigurationProvider);
	
	EntityInMapOptions<K, V, M> withValueMapping(EntityMappingConfigurationProvider<V, ?> mappingConfigurationProvider);
	
	/**
	 * Indicates mapping to be used for {@link Map} values when it's a bean type.
	 * Bean properties will be part of table primary key.
	 * 
	 * @param mappingConfigurationProvider properties mapping for value type
	 */
	EmbeddableInMapOptions<V> withValueMapping(EmbeddableMappingConfigurationProvider<V> mappingConfigurationProvider);
	
	MapOptions<K, V, M> fetchSeparately();
	
	interface EntityInMapOptions<K, V, M extends Map<K, V>> extends CascadeOptions {
		
		/**
		 * Defines how cascading is applied to key entities or value entities of a {@link Map} relation.
		 * If given mode is a writable one ({@link RelationMode#ASSOCIATION_ONLY}, {@link RelationMode#ALL_ORPHAN_REMOVAL}, {@link RelationMode#ALL})
		 * then association records are also considered writable (keys and values). As a consequence, giving {@link RelationMode#READ_ONLY} makes
		 * them read-only too.
		 *
		 * @param relationMode a {@link RelationMode}
		 * @return a proxy letting caller chain the result with some methods of {@link MapOptions} to create its configuration fluently
		 */
		@Override
		EntityInMapOptions<K, V, M> cascading(RelationMode relationMode);
		
	}
	
	interface EmbeddableInMapOptions<E> {
		
		/**
		 * Allows to set column name while embedded configuration is given by {@link #withKeyMapping(EmbeddableMappingConfigurationProvider)}
		 *
		 * @param getter property accessor for which column name must be overridden
		 * @param columnName column name to be used by override
		 */
		EmbeddableInMapOptions<E> overrideName(SerializableFunction<E, ?> getter, String columnName);
		
		/**
		 * Allows to set column name while embedded configuration is given by {@link #withKeyMapping(EmbeddableMappingConfigurationProvider)}
		 *
		 * @param setter property accessor for which column name must be overridden
		 * @param columnName column name to be used by override
		 */
		EmbeddableInMapOptions<E> overrideName(SerializableBiConsumer<E, ?> setter, String columnName);
		
		/**
		 * Allows to set column name while embedded configuration is given by {@link #withKeyMapping(EmbeddableMappingConfigurationProvider)}
		 *
		 * @param getter property accessor for which column name must be overridden
		 * @param columnSize column name to be used by override
		 */
		EmbeddableInMapOptions<E> overrideSize(SerializableFunction<E, ?> getter, Size columnSize);
		
		/**
		 * Allows to set column name while embedded configuration is given by {@link #withKeyMapping(EmbeddableMappingConfigurationProvider)}
		 *
		 * @param setter property accessor for which column name must be overridden
		 * @param columnSize column name to be used by override
		 */
		EmbeddableInMapOptions<E> overrideSize(SerializableBiConsumer<E, ?> setter, Size columnSize);
		
	}
}