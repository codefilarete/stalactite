package org.codefilarete.stalactite.engine.configurer.map;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.ComposedIdMapping;
import org.codefilarete.stalactite.mapping.EmbeddedBeanMapping;
import org.codefilarete.stalactite.mapping.IdAccessor;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Maps;

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
public class KeyValueRecordMapping<K, V, I, T extends Table<T>> extends ClassMapping<KeyValueRecord<K, V, I>, RecordId<K, I>, T> {
	
	@VisibleForTesting
	public KeyValueRecordMapping(T targetTable,
						  Map<? extends ReversibleAccessor<KeyValueRecord<K, V, I>, ?>, Column<T, ?>> propertyToColumn,
						  KeyValueRecordIdMapping<K, I, T> idMapping) {
		super((Class) KeyValueRecord.class,
				targetTable,
				propertyToColumn,
				// cast because idMapping has KeyValueRecord<K, ?, I> as generics instead of KeyValueRecord<K, V, I>
				(ComposedIdMapping<KeyValueRecord<K, V, I>, RecordId<K, I>>) (ComposedIdMapping) idMapping);
		
	}
	
	/**
	 * {@link IdMapping} for {@link KeyValueRecord} : a composed id made of
	 * - {@link KeyValueRecord#getId()}
	 * - {@link KeyValueRecord#getKey()}
	 */
	@VisibleForTesting
	public static class KeyValueRecordIdMapping<K, I, T extends Table<T>> extends ComposedIdMapping<KeyValueRecord<K, ?, I>, RecordId<K, I>> {
		
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
				Function<ColumnedRow, K> entryKeyAssembler,
				Function<K, Map<Column<T, ?>, ?>> entryKeyColumnValueProvider,
				IdentifierAssembler<I, LEFTTABLE> sourceIdentifierAssembler,
				Map<Column<LEFTTABLE, ?>, Column<T, ?>> primaryKey2ForeignKeyMapping) {
			this(new RecordIdAssembler<>(targetTable, entryKeyAssembler, entryKeyColumnValueProvider, sourceIdentifierAssembler, primaryKey2ForeignKeyMapping));
		}
		
		public <LEFTTABLE extends Table<LEFTTABLE>> KeyValueRecordIdMapping(
				T targetTable,
				EmbeddedBeanMapping<K, T> entryKeyMapping,
				IdentifierAssembler<I, LEFTTABLE> sourceIdentifierAssembler,
				Map<Column<LEFTTABLE, ?>, Column<T, ?>> primaryKey2ForeignKeyMapping) {
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
		public boolean isNew(KeyValueRecord<K, ?, I> entity) {
			return entity.isNew();
		}
		
		public static class KeyValueRecordIdAccessor<K, I> implements IdAccessor<KeyValueRecord<K, ?, I>, RecordId<K, I>> {
			
			@Override
			public RecordId<K, I> getId(KeyValueRecord<K, ?, I> associationRecord) {
				return associationRecord.getId();
			}
			
			@Override
			public void setId(KeyValueRecord<K, ?, I> associationRecord, RecordId<K, I> identifier) {
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
			
			private final Function<ColumnedRow, K> entryKeyAssembler;
			private final Function<K, Map<Column<T, ?>, ?>> entryKeyColumnValueProvider;
			private final IdentifierAssembler<ID, ?> sourceIdentifierAssembler;
			private final Map<Column<?, ?>, Column<T, ?>> primaryKey2ForeignKeyMapping;
			
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
					Function<ColumnedRow, K> entryKeyAssembler,
					Function<K, Map<Column<T, ?>, ?>> entryKeyColumnValueProvider,
					IdentifierAssembler<ID, LEFTTABLE> sourceIdentifierAssembler,
					Map<? extends Column<LEFTTABLE, ?>, ? extends Column<T, ?>> primaryKey2ForeignKeyMapping
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
					Map<? extends Column<LEFTTABLE, ?>, ? extends Column<T, ?>> primaryKey2ForeignKeyMapping
					) {
				this(targetTable, entryKeyMapping::transform, entryKeyMapping::getInsertValues, sourceIdentifierAssembler, primaryKey2ForeignKeyMapping);
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
					Map<? extends Column<LEFTTABLE, ?>, ? extends Column<T, ?>> primaryKey2ForeignKeyMapping,
					Map<Column<RIGHTTABLE, ?>, Column<T, ?>> rightTable2EntryKeyMapping
					) {
				this(targetTable, rightTableIdentifierAssembler::assemble, k -> {
					Map<Column<RIGHTTABLE, ?>, ?> keyColumnValues = rightTableIdentifierAssembler.getColumnValues(k);
					return Maps.innerJoin(rightTable2EntryKeyMapping, keyColumnValues);
					
				}, sourceIdentifierAssembler, primaryKey2ForeignKeyMapping);
			}
			
			@Override
			public RecordId<K, ID> assemble(ColumnedRow row) {
				ID id = sourceIdentifierAssembler.assemble(row);
				K entryKey = entryKeyAssembler.apply(row);
				
				return id == null ? null : new RecordId<>(id, entryKey);
			}
			
			@Override
			public Map<Column<T, ?>, ?> getColumnValues(RecordId<K, ID> record) {
				Map<Column<?, ?>, Object> sourceColumnValues = (Map) sourceIdentifierAssembler.getColumnValues(record.getId());
				Map<Column<T, ?>, Object> idColumnValues = Maps.innerJoin(primaryKey2ForeignKeyMapping, sourceColumnValues);
				Map<Column<T, ?>, ?> entryKeyColumnValues = entryKeyColumnValueProvider.apply(record.getKey());
				Map<Column<T, ?>, Object> result = new HashMap<>();
				result.putAll(idColumnValues);
				result.putAll(entryKeyColumnValues);
				return result;
			}
		}
	}
}