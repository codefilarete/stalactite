package org.codefilarete.stalactite.engine.configurer.map;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.AccessorChain.ValueInitializerOnNullValue;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.ComposedIdMapping;
import org.codefilarete.stalactite.mapping.EmbeddedBeanMapping;
import org.codefilarete.stalactite.mapping.EmbeddedClassMapping;
import org.codefilarete.stalactite.mapping.IdAccessor;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.collection.Maps.ChainingMap;

/**
 * Mapping strategy dedicated to {@link KeyValueRecord}. Very close to {@link org.codefilarete.stalactite.engine.configurer.AssociationRecordMapping}
 * in its principle.
 *
 * @param <K> embedded key bean type
 * @param <V> embedded value bean type
 * @param <I> source identifier type
 * @param <T> relation table type
 * @author Guillaume Mary
 */
class KeyValueRecordMapping<K, V, I, T extends Table<T>> extends ClassMapping<KeyValueRecord<K, V, I>, RecordId<K, I>, T> {
	
	/**
	 * Builder of {@link KeyValueRecordMapping} to let one choose between single-column entry element and composite one.
	 * Managing those cases is necessary due to that single-value entry element cannot be set from database
	 * through a Mutator and declared by the Map&gt;Accessor, Column&lt;
	 * 
	 * @author Guillaume Mary
	 */
	static class KeyValueRecordMappingBuilder<K, V, I, T extends Table<T>, LEFTTABLE extends Table<LEFTTABLE>> {
		
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
			this.primaryKeyForeignColumnMapping = (Map) primaryKeyForeignColumnMapping;
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
			KeyValueRecordIdMapping<K, V, I, T> idMapping = null;
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
	
	KeyValueRecordMapping(T targetTable,
						  Map<? extends ReversibleAccessor<KeyValueRecord<K, V, I>, Object>, Column<T, Object>> propertyToColumn,
						  KeyValueRecordIdMapping<K, V, I, T> idMapping) {
		super((Class) KeyValueRecord.class, targetTable, propertyToColumn, idMapping);
		
	}
	
	/**
	 * {@link IdMapping} for {@link KeyValueRecord} : a composed id made of
	 * - {@link KeyValueRecord#getId()}
	 * - {@link KeyValueRecord#getKey()}
	 * - {@link KeyValueRecord#getValue()}
	 */
	@VisibleForTesting
	static class KeyValueRecordIdMapping<K, V, I, T extends Table<T>> extends ComposedIdMapping<KeyValueRecord<K, V, I>, RecordId<K, I>> {
		
		private KeyValueRecordIdMapping(
				RecordIdAssembler recordIdAssembler) {
			super(new KeyValueRecordIdAccessor<>(),
					new AlreadyAssignedIdentifierManager<>((Class<RecordId<K, I>>) (Class) RecordId.class,
							KeyValueRecord::markAsPersisted,
							KeyValueRecord::isPersisted),
					recordIdAssembler);
		}
		
		public <LEFTTABLE extends Table<LEFTTABLE>> KeyValueRecordIdMapping(
				T targetTable,
				BiFunction<Row, ColumnedRow, K> entryKeyAssembler,
				Function<K, Map<Column<T, Object>, Object>> entryKeyColumnValueProvider,
				IdentifierAssembler<I, LEFTTABLE> sourceIdentifierAssembler,
				Map<Column<LEFTTABLE, Object>, Column<T, Object>> primaryKey2ForeignKeyMapping) {
			this(new RecordIdAssembler<>(targetTable, entryKeyAssembler, entryKeyColumnValueProvider, sourceIdentifierAssembler, primaryKey2ForeignKeyMapping));
		}
		
		public <LEFTTABLE extends Table<LEFTTABLE>> KeyValueRecordIdMapping(
				T targetTable,
				EmbeddedBeanMapping<K, T> entryKeyMapping,
				IdentifierAssembler<I, LEFTTABLE> sourceIdentifierAssembler,
				Map<Column<LEFTTABLE, Object>, Column<T, Object>> primaryKey2ForeignKeyMapping) {
			this(new RecordIdAssembler<>(targetTable, entryKeyMapping, sourceIdentifierAssembler, primaryKey2ForeignKeyMapping));
		}
		
		/**
		 * Override because {@link ComposedIdMapping} is based on null identifier to determine newness, which is always false for {@link KeyValueRecord}
		 * because they always have one. We delegate its computation to the entity.
		 *
		 * @param entity any non-null entity
		 * @return true or false based on {@link KeyValueRecord#isNew()}
		 */
		@Override
		public boolean isNew(KeyValueRecord<K, V, I> entity) {
			return entity.isNew();
		}
		
		private static class KeyValueRecordIdAccessor<K, V, I> implements IdAccessor<KeyValueRecord<K, V, I>, RecordId<K, I>> {
			
			@Override
			public RecordId<K, I> getId(KeyValueRecord<K, V, I> associationRecord) {
				return associationRecord.getId();
			}
			
			@Override
			public void setId(KeyValueRecord<K, V, I> associationRecord, RecordId<K, I> identifier) {
				associationRecord.setId(new RecordId<>(identifier.getId(), associationRecord.getKey()));
				associationRecord.setKey(identifier.getKey());
			}
		}
		
		/**
		 * Identifier assembler for {@link RecordId}
		 *
		 * @param <K> embedded key bean type
		 * @param <ID> source identifier type
		 */
		@VisibleForTesting
		static class RecordIdAssembler<K, ID, T extends Table<T>> extends ComposedIdentifierAssembler<RecordId<K, ID>, T> {
			
			private final BiFunction<Row, ColumnedRow, K> entryKeyAssembler;
			private final Function<K, Map<Column<T, Object>, Object>> entryKeyColumnValueProvider;
			private final IdentifierAssembler<ID, ?> sourceIdentifierAssembler;
			private final Map<Column<?, Object>, Column<T, Object>> primaryKey2ForeignKeyMapping;
			
			/**
			 * Constructor mapping given values to fields
			 * 
			 * @param targetTable association table on which value should be inserted an read
			 * @param entryKeyAssembler builder of K instances from {@link Column} found in a {@link Row}
			 * @param entryKeyColumnValueProvider provides {@link Column} values to be inserted / updated from a K instance
			 * @param sourceIdentifierAssembler assemble / disassemble ID instances from / to {@link Column}s
			 * @param primaryKey2ForeignKeyMapping used to remap {@link Column}s of sourceIdentifierAssembler to association table since it gives it for LEFTTABLE
			 * @param <LEFTTABLE> table type of source declaring the relation
			 */
			@VisibleForTesting
			<LEFTTABLE extends Table<LEFTTABLE>> RecordIdAssembler(
					T targetTable,
					BiFunction<Row, ColumnedRow, K> entryKeyAssembler,
					Function<K, Map<Column<T, Object>, Object>> entryKeyColumnValueProvider,
					IdentifierAssembler<ID, LEFTTABLE> sourceIdentifierAssembler,
					Map<? extends Column<LEFTTABLE, Object>, ? extends Column<T, Object>> primaryKey2ForeignKeyMapping
			) {
				super(targetTable);
				this.entryKeyAssembler = entryKeyAssembler;
				this.entryKeyColumnValueProvider = entryKeyColumnValueProvider;
				this.sourceIdentifierAssembler = sourceIdentifierAssembler;
				this.primaryKey2ForeignKeyMapping = (Map) primaryKey2ForeignKeyMapping;
			}
			
			/**
			 * Constructor for cases where K is a simple (embedded) bean.
			 *
			 * @param targetTable association table on which value should be inserted an read
			 * @param entryKeyMapping persistence mapping of K instance
			 * @param sourceIdentifierAssembler assemble / disassemble ID instances from / to {@link Column}s
			 * @param primaryKey2ForeignKeyMapping used to remap {@link Column}s of sourceIdentifierAssembler to association table since it gives it for LEFTTABLE
			 * @param <LEFTTABLE> table type of source declaring the relation
			 */
			@VisibleForTesting
			<LEFTTABLE extends Table<LEFTTABLE>> RecordIdAssembler(
					T targetTable,
					EmbeddedBeanMapping<K, T> entryKeyMapping,
					IdentifierAssembler<ID, LEFTTABLE> sourceIdentifierAssembler,
					Map<? extends Column<LEFTTABLE, Object>, ? extends Column<T, Object>> primaryKey2ForeignKeyMapping
					) {
				this(targetTable, (row, columnedRow) -> entryKeyMapping.copyTransformerWithAliases(columnedRow).transform(row), entryKeyMapping::getInsertValues, sourceIdentifierAssembler, primaryKey2ForeignKeyMapping);
			}
			
			/**
			 * Constructor for cases where entry key an entity referencing another table. Then K type becomes its identifier.
			 *
			 * @param targetTable association table on which value should be inserted an read
			 * @param rightTableIdentifierAssembler entity identifier mapping (K type mapping)
			 * @param sourceIdentifierAssembler assemble / disassemble ID instances from / to {@link Column}s
			 * @param primaryKey2ForeignKeyMapping used to remap {@link Column}s of sourceIdentifierAssembler to association table since it gives it for LEFTTABLE
			 * @param <LEFTTABLE> table type of source declaring the relation
			 */
			@VisibleForTesting
			<LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>> RecordIdAssembler(
					T targetTable,
					IdentifierAssembler<K, RIGHTTABLE> rightTableIdentifierAssembler,
					IdentifierAssembler<ID, LEFTTABLE> sourceIdentifierAssembler,
					Map<? extends Column<LEFTTABLE, Object>, ? extends Column<T, Object>> primaryKey2ForeignKeyMapping,
					Map<Column<RIGHTTABLE, Object>, Column<T, Object>> rightTable2EntryKeyMapping
					) {
				this(targetTable, rightTableIdentifierAssembler::assemble, k -> {
					Map<Column<RIGHTTABLE, Object>, Object> keyColumnValues = rightTableIdentifierAssembler.getColumnValues(k);
					return Maps.innerJoin(rightTable2EntryKeyMapping, keyColumnValues);
					
				}, sourceIdentifierAssembler, primaryKey2ForeignKeyMapping);
			}
			
			@Override
			public RecordId<K, ID> assemble(Row row, ColumnedRow rowAliaser) {
				ID id = sourceIdentifierAssembler.assemble(row, rowAliaser);
				K entryKey = entryKeyAssembler.apply(row, rowAliaser);
				
				return new RecordId<>(id, entryKey);
			}
			
			@Override
			public RecordId<K, ID> assemble(Function<Column<?, ?>, Object> columnValueProvider) {
				// never called because we override assemble(Row, ColumnedRow)
				return null;
			}
			
			@Override
			public Map<Column<T, Object>, Object> getColumnValues(RecordId<K, ID> record) {
				Map<Column<?, Object>, Object> sourceColumnValues = (Map) sourceIdentifierAssembler.getColumnValues(record.getId());
				Map<Column<T, Object>, Object> idColumnValues = Maps.innerJoin(primaryKey2ForeignKeyMapping, sourceColumnValues);
				Map<Column<T, Object>, Object> entryKeyColumnValues = entryKeyColumnValueProvider.apply(record.getKey());
				Map<Column<T, Object>, Object> result = new HashMap<>();
				result.putAll(idColumnValues);
				result.putAll(entryKeyColumnValues);
				return result;
			}
		}
	}
}