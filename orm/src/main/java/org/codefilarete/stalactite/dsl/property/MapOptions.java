package org.codefilarete.stalactite.dsl.property;

import java.util.Map;
import java.util.function.Supplier;

import org.codefilarete.stalactite.dsl.entity.EntityMappingConfigurationProvider;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfigurationProvider;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * API to define a Map relation
 * 
 * @author Guillaume Mary
 */
public interface MapOptions<K, V, M extends Map<K, V>> {
	
	MapOptions<K, V, M> withReverseJoinColumn(String columnName);
	
	MapOptions<K, V, M> keyColumn(String columnName);
	
	MapOptions<K, V, M> valueColumn(String columnName);
	
	MapOptions<K, V, M> withMapFactory(Supplier<? extends M> collectionFactory);
	
	MapOptions<K, V, M> onTable(String tableName);
	
	MapOptions<K, V, M> onTable(Table table);
	
	KeyAsEntityMapOptions<K, V, M> withKeyMapping(EntityMappingConfigurationProvider<K, ?> mappingConfigurationProvider);
	
	/**
	 * Indicates mapping to be used for {@link Map} keys when it's a bean type.
	 * Bean properties will be part of table primary key.
	 * 
	 * @param mappingConfigurationProvider properties mapping for key type
	 */
	MapOptions<K, V, M> withKeyMapping(EmbeddableMappingConfigurationProvider<K> mappingConfigurationProvider);
	
	ValueAsEntityMapOptions<K, V, M> withValueMapping(EntityMappingConfigurationProvider<V, ?> mappingConfigurationProvider);
	
	/**
	 * Indicates mapping to be used for {@link Map} values when it's a bean type.
	 * Bean properties will be part of table primary key.
	 * 
	 * @param mappingConfigurationProvider properties mapping for value type
	 */
	MapOptions<K, V, M> withValueMapping(EmbeddableMappingConfigurationProvider<V> mappingConfigurationProvider);
	
	/**
	 * Allows to set column name while embedded configuration is given by {@link #withKeyMapping(EmbeddableMappingConfigurationProvider)}
	 * @param getter property accessor for which column name must be overridden
	 * @param columnName column name to be used by override
	 */
	<IN> MapOptions<K, V, M> overrideKeyColumnName(SerializableFunction<K, IN> getter, String columnName);
	
	/**
	 * Allows to set column name while embedded configuration is given by {@link #withKeyMapping(EmbeddableMappingConfigurationProvider)}
	 * @param setter property accessor for which column name must be overridden
	 * @param columnName column name to be used by override
	 */
	<IN> MapOptions<K, V, M> overrideKeyColumnName(SerializableBiConsumer<K, IN> setter, String columnName);
	
	/**
	 * Allows to set column name while embedded configuration is given by {@link #withValueMapping(EmbeddableMappingConfigurationProvider)}
	 * @param getter property accessor for which column name must be overridden
	 * @param columnName column name to be used by override
	 */
	<IN> MapOptions<K, V, M> overrideValueColumnName(SerializableFunction<K, IN> getter, String columnName);
	
	/**
	 * Allows to set column name while embedded configuration is given by {@link #withValueMapping(EmbeddableMappingConfigurationProvider)}
	 * @param setter property accessor for which column name must be overridden
	 * @param columnName column name to be used by override
	 */
	<IN> MapOptions<K, V, M> overrideValueColumnName(SerializableBiConsumer<K, IN> setter, String columnName);
	
	MapOptions<K, V, M> fetchSeparately();
	
	interface KeyAsEntityMapOptions<K, V, M extends Map<K, V>> {
		
		/**
		 * Defines how cascading is applied to key entries and key entities of a {@link Map} relation.
		 * If given mode is a writable one ({@link RelationMode#ASSOCIATION_ONLY}, {@link RelationMode#ALL_ORPHAN_REMOVAL}, {@link RelationMode#ALL})
		 * then association records are also considered writable (keys and values). As a consequence, giving {@link RelationMode#READ_ONLY} makes
		 * them read-only too.
		 * 
		 * @param relationMode a {@link RelationMode}
		 * @return a proxy letting caller chain the result with some methods of {@link MapOptions} to create its configuration fluently 
		 */
		MapOptions<K, V, M> cascading(RelationMode relationMode);
		
	}
	
	interface ValueAsEntityMapOptions<K, V, M extends Map<K, V>> {
		
		/**
		 * Defines how cascading is applied to value entities of a {@link Map} relation.
		 * Note that as a difference with {@link KeyAsEntityMapOptions#cascading(RelationMode)} this method doesn't drive
		 * association records : it only acts on value entities by managing their cascade on their table, but not in the
		 * association table.
		 *
		 * @param relationMode a {@link RelationMode}
		 * @return a proxy letting caller chain the result with some methods of {@link MapOptions} to create its configuration fluently 
		 */
		MapOptions<K, V, M> cascading(RelationMode relationMode);
		
	}
}