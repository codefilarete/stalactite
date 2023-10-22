package org.codefilarete.stalactite.engine.configurer.map;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.ComposedIdMapping;
import org.codefilarete.stalactite.mapping.EmbeddedClassMapping;
import org.codefilarete.stalactite.mapping.IdAccessor;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.collection.Maps;

/**
 * Mapping strategy dedicated to {@link KeyValueRecord}. Very close to {@link org.codefilarete.stalactite.engine.configurer.AssociationRecordMapping}
 * in its principle.
 */
class KeyValueRecordMapping<K, V, I, T extends Table<T>> extends ClassMapping<KeyValueRecord<K, V, I>, KeyValueRecord<K, V, I>, T> {
	
	<LEFTTABLE extends Table<LEFTTABLE>> KeyValueRecordMapping(T targetTable,
															   Column<T, K> keyColumn,
															   Column<T, V> valueColumn,
															   IdentifierAssembler<I, LEFTTABLE> sourceIdentifierAssembler,
															   Map<Column<LEFTTABLE, Object>, Column<T, Object>> primaryKeyForeignColumnMapping) {
		super((Class) KeyValueRecord.class,
				targetTable,
				(Map) Maps.forHashMap(ReversibleAccessor.class, Column.class)
						.add(KeyValueRecord.KEY_ACCESSOR, keyColumn)
						.add(KeyValueRecord.VALUE_ACCESSOR, valueColumn),
				new KeyValueRecordIdMapping<>(targetTable, keyColumn, valueColumn, sourceIdentifierAssembler, primaryKeyForeignColumnMapping));
	}
	
	<LEFTTABLE extends Table<LEFTTABLE>> KeyValueRecordMapping(T targetTable,
															   EmbeddedClassMapping<KeyValueRecord<K, V, I>, T> keyMapping,
															   IdentifierAssembler<I, LEFTTABLE> sourceIdentifierAssembler,
															   Map<Column<LEFTTABLE, Object>, Column<T, Object>> primaryKeyForeignColumnMapping) {
		super((Class) KeyValueRecord.class,
				targetTable,
				keyMapping.getPropertyToColumn(),
				new KeyValueRecordIdMapping<>(targetTable, keyMapping, sourceIdentifierAssembler, primaryKeyForeignColumnMapping));
	}
	
	/**
	 * {@link IdMapping} for {@link KeyValueRecord} : a composed id made of
	 * - {@link KeyValueRecord#getId()}
	 * - {@link KeyValueRecord#getKey()}
	 * - {@link KeyValueRecord#getValue()}
	 */
	private static class KeyValueRecordIdMapping<K, V, I, T extends Table<T>> extends ComposedIdMapping<KeyValueRecord<K, V, I>, KeyValueRecord<K, V, I>> {
		
		public <LEFTTABLE extends Table<LEFTTABLE>> KeyValueRecordIdMapping(
				T targetTable,
				Column<T, K> keyColumn,
				Column<T, V> valueColumn,
				IdentifierAssembler<I, LEFTTABLE> sourceIdentifierAssembler,
				Map<Column<LEFTTABLE, Object>, Column<T, Object>> primaryKeyForeignColumnMapping) {
			super(new KeyValueRecordIdAccessor<>(),
					new AlreadyAssignedIdentifierManager<>((Class<KeyValueRecord<K, V, I>>) (Class) KeyValueRecord.class,
							KeyValueRecord::markAsPersisted,
							KeyValueRecord::isPersisted),
					new DefaultKeyValueRecordIdentifierAssembler<>(targetTable, keyColumn, valueColumn, sourceIdentifierAssembler, primaryKeyForeignColumnMapping));
		}
		
		public <LEFTTABLE extends Table<LEFTTABLE>> KeyValueRecordIdMapping(
				T targetTable,
				EmbeddedClassMapping<KeyValueRecord<K, V, I>, T> keyClassMapping,
				IdentifierAssembler<I, LEFTTABLE> sourceIdentifierAssembler,
				Map<Column<LEFTTABLE, Object>, Column<T, Object>> primaryKeyForeignColumnMapping) {
			super(new KeyValueRecordIdAccessor<>(),
					new AlreadyAssignedIdentifierManager<>((Class<KeyValueRecord<K, V, I>>) (Class) KeyValueRecord.class,
							KeyValueRecord::markAsPersisted,
							KeyValueRecord::isPersisted),
					new KeyValueRecordIdentifierAssembler<>(targetTable, keyClassMapping, sourceIdentifierAssembler, primaryKeyForeignColumnMapping));
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
		
		private static class KeyValueRecordIdAccessor<K, V, I> implements IdAccessor<KeyValueRecord<K, V, I>, KeyValueRecord<K, V, I>> {
			
			@Override
			public KeyValueRecord<K, V, I> getId(KeyValueRecord<K, V, I> associationRecord) {
				return associationRecord;
			}
			
			@Override
			public void setId(KeyValueRecord<K, V, I> associationRecord, KeyValueRecord<K, V, I> identifier) {
				associationRecord.setId(identifier.getId());
				associationRecord.setKey(identifier.getKey());
				associationRecord.setValue(identifier.getValue());
			}
		}
		
		/**
		 * Identifier assembler when {@link KeyValueRecord} is persisted according to a default behavior :
		 * - identifier is saved in table primary key
		 * - element value is saved in elementColumn
		 *
		 * @param <K> embedded key bean type
		 * @param <V> embedded value bean type
		 * @param <ID> source identifier type
		 */
		private static class DefaultKeyValueRecordIdentifierAssembler<K, V, ID, T extends Table<T>> extends ComposedIdentifierAssembler<KeyValueRecord<K, V, ID>, T> {
			
			private final Column<T, K> keyColumn;
			private final Column<T, V> valueColumn;
			private final IdentifierAssembler<ID, ?> sourceIdentifierAssembler;
			private final Map<Column<?, Object>, Column<T, Object>> primaryKeyForeignColumnMapping;
			
			private <LEFTTABLE extends Table<LEFTTABLE>> DefaultKeyValueRecordIdentifierAssembler(T targetTable,
																								  Column<T, K> keyColumn,
																								  Column<T, V> valueColumn,
																								  IdentifierAssembler<ID, LEFTTABLE> sourceIdentifierAssembler,
																								  Map<Column<LEFTTABLE, Object>, Column<T, Object>> primaryKeyForeignColumnMapping) {
				super(targetTable);
				this.keyColumn = keyColumn;
				this.valueColumn = valueColumn;
				this.sourceIdentifierAssembler = sourceIdentifierAssembler;
				this.primaryKeyForeignColumnMapping = (Map) primaryKeyForeignColumnMapping;
			}
			
			@Override
			public KeyValueRecord<K, V, ID> assemble(Function<Column<?, ?>, Object> columnValueProvider) {
				ID id = sourceIdentifierAssembler.assemble(sourceColumn -> {
					Column<T, Object> targetColumn = primaryKeyForeignColumnMapping.get(sourceColumn);
					return columnValueProvider.apply(targetColumn);
				});
				K leftValue = (K) columnValueProvider.apply(keyColumn);
				V rightValue = (V) columnValueProvider.apply(valueColumn);
				// we should not return an id if any (both expected in fact) value is null
				if (id == null || leftValue == null || rightValue == null) {
					return null;
				} else {
					return new KeyValueRecord<>(id, leftValue, rightValue);
				}
			}
			
			@Override
			public Map<Column<T, Object>, Object> getColumnValues(KeyValueRecord<K, V, ID> id) {
				Map<Column<?, Object>, Object> sourceColumnValues = (Map) sourceIdentifierAssembler.getColumnValues(id.getId());
				Map<Column<T, Object>, Object> idColumnValues = Maps.innerJoin(primaryKeyForeignColumnMapping, sourceColumnValues);
				Map<Column<T, Object>, Object> result = new HashMap<>();
				result.put((Column<T, Object>) keyColumn, id.getKey());
				result.put((Column<T, Object>) valueColumn, id.getValue());
				result.putAll(idColumnValues);
				return result;
			}
		}
		
		/**
		 * Identifier assembler for cases where user gave a configuration to persist embedded beans (default way is not used)
		 *
		 * @param <K> embedded key bean type
		 * @param <V> embedded value bean type
		 * @param <ID> source identifier type
		 */
		private static class KeyValueRecordIdentifierAssembler<K, V, ID, T extends Table<T>> extends ComposedIdentifierAssembler<KeyValueRecord<K, V, ID>, T> {
			
			private final EmbeddedClassMapping<KeyValueRecord<K, V, ID>, T> recordClassMapping;
			private final IdentifierAssembler<ID, ?> sourceIdentifierAssembler;
			private final Map<Column<?, Object>, Column<T, Object>> primaryKeyForeignColumnMapping;
			
			private <LEFTTABLE extends Table<LEFTTABLE>> KeyValueRecordIdentifierAssembler(
					T targetTable,
					EmbeddedClassMapping<KeyValueRecord<K, V, ID>, T> recordClassMapping,
					IdentifierAssembler<ID, LEFTTABLE> sourceIdentifierAssembler,
					Map<Column<LEFTTABLE, Object>, Column<T, Object>> primaryKeyForeignColumnMapping) {
				super(targetTable);
				this.recordClassMapping = recordClassMapping;
				this.sourceIdentifierAssembler = sourceIdentifierAssembler;
				this.primaryKeyForeignColumnMapping = (Map) primaryKeyForeignColumnMapping;
			}
			
			@Override
			public KeyValueRecord<K, V, ID> assemble(Row row, ColumnedRow rowAliaser) {
				return recordClassMapping.copyTransformerWithAliases(rowAliaser).transform(row);
			}
			
			@Override
			public KeyValueRecord<K, V, ID> assemble(Function<Column<?, ?>, Object> columnValueProvider) {
				// never called because we override assemble(Row, ColumnedRow)
				return null;
			}
			
			@Override
			public Map<Column<T, Object>, Object> getColumnValues(KeyValueRecord<K, V, ID> id) {
				Map<Column<?, Object>, Object> sourceColumnValues = (Map) sourceIdentifierAssembler.getColumnValues(id.getId());
				Map<Column<T, Object>, Object> idColumnValues = Maps.innerJoin(primaryKeyForeignColumnMapping, sourceColumnValues);
				Map<Column<T, Object>, Object> result = new HashMap<>();
				result.putAll(idColumnValues);
				result.putAll(recordClassMapping.getInsertValues(id));
				return result;
			}
		}
	}
}