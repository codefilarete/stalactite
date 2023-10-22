package org.codefilarete.stalactite.engine;

import java.util.Map;
import java.util.function.Supplier;

import org.codefilarete.stalactite.sql.ddl.structure.Table;

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
	
	MapOptions<K, V, M> withKeyMapping(EmbeddableMappingConfigurationProvider<K> mappingConfigurationProvider);
	
	MapOptions<K, V, M> withValueMapping(EntityMappingConfigurationProvider<V, ?> mappingConfigurationProvider);
	
	MapOptions<K, V, M> withValueMapping(EmbeddableMappingConfigurationProvider<V> mappingConfigurationProvider);
	
	MapOptions<K, V, M> fetchSeparately();
	
}