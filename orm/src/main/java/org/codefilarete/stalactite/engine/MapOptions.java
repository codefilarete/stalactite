package org.codefilarete.stalactite.engine;

import java.util.Map;
import java.util.function.Supplier;

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
	
	MapOptions<K, V, M> withKeyColumn(String columnName);
	
	MapOptions<K, V, M> withValueColumn(String columnName);
	
	MapOptions<K, V, M> withMapFactory(Supplier<? extends M> collectionFactory);
	
	MapOptions<K, V, M> withTable(String tableName);
	
	MapOptions<K, V, M> withTable(Table table);
	
	MapOptions<K, V, M> withKeyMapping(EntityMappingConfigurationProvider<K, ?> mappingConfigurationProvider);
	
	/**
	 * Indicates mapping to be used for {@link Map} keys when it's a bean type.
	 * Bean properties will be part of table primary key.
	 * 
	 * @param mappingConfigurationProvider properties mapping for key type
	 */
	MapOptions<K, V, M> withKeyMapping(EmbeddableMappingConfigurationProvider<K> mappingConfigurationProvider);
	
	MapOptions<K, V, M> withValueMapping(EntityMappingConfigurationProvider<V, ?> mappingConfigurationProvider);
	
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
	
}