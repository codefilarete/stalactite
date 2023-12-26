package org.codefilarete.stalactite.engine.configurer.map;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorChain.ValueInitializerOnNullValue;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.configurer.map.KeyValueRecordMapping.KeyValueRecordIdMapping;
import org.codefilarete.stalactite.mapping.EmbeddedClassMapping;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.collection.Maps.ChainingMap;

/**
 * Builder of {@link KeyValueRecordMapping} to let one choose between single-column entry element and composite one.
 * Managing those cases is necessary due to that single-value entry element cannot be set from database
 * through a Mutator and declared by the Map&gt;Accessor, Column&lt;
 *
 * @author Guillaume Mary
 */
class KeyValueRecordMappingBuilder<K, V, I, T extends Table<T>, LEFTTABLE extends Table<LEFTTABLE>> {
	
	private static <K> ChainingMap<ReversibleAccessor, Column> chainWithKeyAccessor(EmbeddedClassMapping<K, ?> entryKeyMapping) {
		ChainingMap<ReversibleAccessor, Column> result = new ChainingMap<>();
		entryKeyMapping.getPropertyToColumn().forEach((keyPropertyAccessor, column) -> {
			AccessorChain key = new AccessorChain(KeyValueRecord.KEY_ACCESSOR, keyPropertyAccessor);
			key.setNullValueHandler(new ValueInitializerOnNullValue((accessor, inputType) -> {
				if (accessor == KeyValueRecord.KEY_ACCESSOR) {
					return entryKeyMapping.getClassToPersist();
				}
				return ValueInitializerOnNullValue.giveValueType(accessor, inputType);
			}));
			result.add(key, column);
		});
		return result;
	}
	
	private static <V> ChainingMap<ReversibleAccessor, Column> chainWithValueAccessor(EmbeddedClassMapping<V, ?> entryKeyMapping) {
		ChainingMap<ReversibleAccessor, Column> result = new ChainingMap<>();
		entryKeyMapping.getPropertyToColumn().forEach((keyPropertyAccessor, column) -> {
			AccessorChain key = new AccessorChain(KeyValueRecord.VALUE_ACCESSOR, keyPropertyAccessor);
			key.setNullValueHandler(new ValueInitializerOnNullValue((accessor, inputType) -> {
				if (accessor == KeyValueRecord.VALUE_ACCESSOR) {
					return entryKeyMapping.getClassToPersist();
				}
				return ValueInitializerOnNullValue.giveValueType(accessor, inputType);
			}));
			result.add(key, column);
		});
		return result;
	}
	
	private final T associationTable;
	private final IdentifierAssembler<I, LEFTTABLE> sourceIdentifierAssembler;
	private final Map<Column<LEFTTABLE, Object>, Column<T, Object>> primaryKeyForeignColumnMapping;
	private Column<T, K> keyColumn;
	private Column<T, V> valueColumn;
	private EmbeddedClassMapping<K, T> entryKeyMapping;
	private EmbeddedClassMapping<V, T> entryValueMapping;
	
	KeyValueRecordMappingBuilder(
			T associationTable,
			IdentifierAssembler<I, LEFTTABLE> sourceIdentifierAssembler,
			Map<Column<LEFTTABLE, Object>, Column<T, Object>> primaryKeyForeignColumnMapping) {
		this.associationTable = associationTable;
		this.sourceIdentifierAssembler = sourceIdentifierAssembler;
		this.primaryKeyForeignColumnMapping = primaryKeyForeignColumnMapping;
	}
	
	void withEntryKeyIsSingleProperty(Column<T, K> keyColumn) {
		this.keyColumn = keyColumn;
	}
	
	void withEntryValueIsSingleProperty(Column<T, V> valueColumn) {
		this.valueColumn = valueColumn;
	}
	
	void withEntryKeyIsComplexType(EmbeddedClassMapping<K, T> entryKeyMapping) {
		this.entryKeyMapping = entryKeyMapping;
	}
	
	void withEntryValueIsComplexType(EmbeddedClassMapping<V, T> entryValueMapping) {
		this.entryValueMapping = entryValueMapping;
	}
	
	KeyValueRecordMapping<K, V, I, T> build() {
		Map<ReversibleAccessor, Column> propertiesMapping = new HashMap<>();
		KeyValueRecordIdMapping<K, I, T> idMapping = null;
		if (keyColumn != null) {
			propertiesMapping.put(KeyValueRecord.KEY_ACCESSOR, keyColumn);
			idMapping = new KeyValueRecordIdMapping<>(
					associationTable,
					(row, columnedRow) -> columnedRow.getValue(keyColumn, row),
					(Function<K, Map<Column<T, Object>, Object>>) k -> (Map) Maps.forHashMap(Column.class, Object.class).add(keyColumn, k),
					sourceIdentifierAssembler,
					primaryKeyForeignColumnMapping);
		} else if (entryKeyMapping != null) {
			propertiesMapping.putAll(chainWithKeyAccessor(entryKeyMapping));
			idMapping = new KeyValueRecordIdMapping<>(
					associationTable,
					entryKeyMapping,
					sourceIdentifierAssembler,
					primaryKeyForeignColumnMapping);
		}
		if (valueColumn != null) {
			propertiesMapping.put(KeyValueRecord.VALUE_ACCESSOR, valueColumn);
		} else if (entryValueMapping != null) {
			propertiesMapping.putAll(chainWithValueAccessor(entryValueMapping));
		}
		
		return new KeyValueRecordMapping<K, V, I, T>(associationTable, (Map) propertiesMapping, idMapping);
	}
}
