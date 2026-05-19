package org.codefilarete.stalactite.engine.configurer.elementcollection;

import java.util.HashMap;
import java.util.Map;

import org.codefilarete.reflection.ReadWriteAccessPoint;
import org.codefilarete.stalactite.mapping.ComposedIdMapping;
import org.codefilarete.stalactite.mapping.DefaultEntityMapping;
import org.codefilarete.stalactite.mapping.EmbeddedClassMapping;
import org.codefilarete.stalactite.mapping.IdAccessor;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.collection.Maps;

/**
 * Class mapping dedicated to {@link IndexedElementRecord}. Very close to {@link ElementRecordMapping}
 * in its principle.
 */
public class IndexedElementRecordMapping<C, I, T extends Table<T>, ER extends IndexedElementRecord<C, I>> extends DefaultEntityMapping<ER, ER, T> {
	
	public <LEFTTABLE extends Table<LEFTTABLE>> IndexedElementRecordMapping(T targetTable,
	                                                                        Column<T, C> elementColumn,
	                                                                        Column<T, Integer> indexColumn,
	                                                                        IdentifierAssembler<I, LEFTTABLE> sourceIdentifierAssembler,
	                                                                        Map<Column<LEFTTABLE, ?>, Column<T, ?>> foreignKeyColumnMapping) {
		super((Class) IndexedElementRecord.class,
				targetTable,
				(Map) Maps.forHashMap(ReadWriteAccessPoint.class, Column.class)
						.add(ElementRecord.ELEMENT_ACCESSOR, elementColumn)
						.add(IndexedElementRecord.INDEX_ACCESSOR, indexColumn),
				new IndexedElementRecordIdMapping<>(targetTable, elementColumn, indexColumn, sourceIdentifierAssembler, foreignKeyColumnMapping));
	}
	
	public <LEFTTABLE extends Table<LEFTTABLE>> IndexedElementRecordMapping(T targetTable,
	                                                                        EmbeddedClassMapping<ER, T> embeddableMapping,
	                                                                        Column<T, Integer> indexColumn,
	                                                                        IdentifierAssembler<I, LEFTTABLE> sourceIdentifierAssembler,
	                                                                        Map<Column<LEFTTABLE, ?>, Column<T, ?>> foreignKeyColumnMapping) {
		super((Class) IndexedElementRecord.class,
				targetTable,
				(Map) Maps.putAll(Maps.forHashMap(ReadWriteAccessPoint.class, Column.class)
						.add(IndexedElementRecord.INDEX_ACCESSOR, indexColumn), embeddableMapping.getPropertyToColumn()),
				new IndexedElementRecordIdMapping<>(targetTable, embeddableMapping, indexColumn, sourceIdentifierAssembler, foreignKeyColumnMapping));
	}
	
	/**
	 * {@link IdMapping} for {@link IndexedElementRecord} : a composed id made of :
	 * - {@link IndexedElementRecord#getId()}
	 * - {@link IndexedElementRecord#getIndex()}
	 * - {@link IndexedElementRecord#getElement()}
	 */
	public static class IndexedElementRecordIdMapping<C, I, T extends Table<T>, ER extends IndexedElementRecord<C, I>> extends ComposedIdMapping<ER, ER> {
		
		public <LEFTTABLE extends Table<LEFTTABLE>> IndexedElementRecordIdMapping(
				T targetTable,
				Column<T, C> elementColumn,
				Column<T, Integer> indexColumn,
				IdentifierAssembler<I, LEFTTABLE> sourceIdentifierAssembler,
				Map<Column<LEFTTABLE, ?>, Column<T, ?>> foreignKeyColumnMapping) {
			super(new IndexedElementRecordIdAccessor<>(),
					new AlreadyAssignedIdentifierManager<>((Class<ER>) (Class) IndexedElementRecord.class,
							IndexedElementRecord::markAsPersisted,
							IndexedElementRecord::isPersisted),
					new DefaultIndexedElementRecordIdentifierAssembler<>(targetTable, elementColumn, indexColumn, sourceIdentifierAssembler, foreignKeyColumnMapping));
		}
		
		public <LEFTTABLE extends Table<LEFTTABLE>> IndexedElementRecordIdMapping(
				T targetTable,
				EmbeddedClassMapping<ER, T> elementMapping,
				Column<T, Integer> indexColumn,
				IdentifierAssembler<I, LEFTTABLE> sourceIdentifierAssembler,
				Map<Column<LEFTTABLE, ?>, Column<T, ?>> foreignKeyColumnMapping) {
			super(new IndexedElementRecordIdAccessor<>(),
					new AlreadyAssignedIdentifierManager<>((Class<ER>) (Class) IndexedElementRecord.class,
							IndexedElementRecord::markAsPersisted,
							IndexedElementRecord::isPersisted),
					new IndexedElementRecordIdentifierAssembler<>(targetTable, elementMapping, indexColumn, sourceIdentifierAssembler, foreignKeyColumnMapping));
		}
		
		/**
		 * Override because {@link ComposedIdMapping} is based on null identifier to determine newness, which is always false for {@link IndexedElementRecord}
		 * because they always have one. We delegate its computation to the entity.
		 *
		 * @param entity any non-null entity
		 * @return true or false based on {@link IndexedElementRecord#isNew()}
		 */
		@Override
		public boolean isNew(ER entity) {
			return entity.isNew();
		}
		
		private static class IndexedElementRecordIdAccessor<C, I, ER extends IndexedElementRecord<C, I>> implements IdAccessor<ER, ER> {
			
			@Override
			public ER getId(ER associationRecord) {
				return associationRecord;
			}
			
			@Override
			public void setId(ER associationRecord, ER identifier) {
				associationRecord.setId(identifier.getId());
				associationRecord.setIndex(identifier.getIndex());
				associationRecord.setElement(identifier.getElement());
			}
		}
		
		/**
		 * Identifier assembler when {@link IndexedElementRecord} is persisted according to a default behavior :
		 * - identifier is saved in table primary key
		 * - element value is saved in elementColumn
		 *
		 * @param <TRGT> embedded bean type
		 * @param <SRCID>   source identifier type
		 */
		public static class DefaultIndexedElementRecordIdentifierAssembler<TRGT, SRCID, T extends Table<T>, ER extends IndexedElementRecord<TRGT, SRCID>> extends ComposedIdentifierAssembler<ER, T> {
			
			private final Column<T, TRGT> elementColumn;
			private final Column<T, Integer> indexColumn;
			private final IdentifierAssembler<SRCID, ?> sourceIdentifierAssembler;
			private final Map<Column<?, ?>, Column<T, ?>> foreignKeyColumnMapping;
			
			private <LEFTTABLE extends Table<LEFTTABLE>> DefaultIndexedElementRecordIdentifierAssembler(T targetTable,
			                                                                                            Column<T, TRGT> elementColumn,
			                                                                                            Column<T, Integer> indexColumn,
			                                                                                            IdentifierAssembler<SRCID, LEFTTABLE> sourceIdentifierAssembler,
			                                                                                            Map<Column<LEFTTABLE, ?>, Column<T, ?>> foreignKeyColumnMapping) {
				super(targetTable);
				this.elementColumn = elementColumn;
				this.indexColumn = indexColumn;
				this.sourceIdentifierAssembler = sourceIdentifierAssembler;
				this.foreignKeyColumnMapping = (Map) foreignKeyColumnMapping;
			}
			
			public Column<T, TRGT> getElementColumn() {
				return elementColumn;
			}
			
			public IdentifierAssembler<SRCID, ?> getSourceIdentifierAssembler() {
				return sourceIdentifierAssembler;
			}
			
			@Override
			public ER assemble(ColumnedRow columnValueProvider) {
				SRCID leftValue = sourceIdentifierAssembler.assemble(new ColumnedRow() {
					@Override
					public <E> E get(Selectable<E> sourceColumn) {
						Column<T, E> targetColumn = (Column<T, E>) foreignKeyColumnMapping.get(sourceColumn);
						return columnValueProvider.get(targetColumn);
					}
				});
				TRGT rightValue = columnValueProvider.get(elementColumn);
				// we should not return an id if any (both expected in fact) value is null
				if (leftValue == null || rightValue == null) {
					return null;
				} else {
					return (ER) new IndexedElementRecord<>(leftValue, rightValue, columnValueProvider.get(indexColumn));
				}
			}
			
			@Override
			public Map<Column<T, ?>, Object> getColumnValues(ER id) {
				Map<Column<?, ?>, Object> sourceColumnValues = (Map) sourceIdentifierAssembler.getColumnValues(id.getId());
				Map<Column<T, ?>, Object> idColumnValues = Maps.innerJoin(foreignKeyColumnMapping, sourceColumnValues);
				Map<Column<T, ?>, Object> result = new HashMap<>();
				result.put(indexColumn, id.getIndex());
				result.putAll(idColumnValues);
				return result;
			}
		}
		
		/**
		 * Identifier assembler for cases where user gave a configuration to persist embedded beans (default way is not used)
		 *
		 * @param <TRGT> embedded bean type
		 * @param <SRCID>   source identifier type
		 */
		public static class IndexedElementRecordIdentifierAssembler<TRGT, SRCID, T extends Table<T>, ER extends IndexedElementRecord<TRGT, SRCID>> extends ComposedIdentifierAssembler<ER, T> {
			
			private final EmbeddedClassMapping<ER, T> elementMapping;
			private final IdentifierAssembler<SRCID, ?> sourceIdentifierAssembler;
			private final Map<Column<?, ?>, Column<T, ?>> foreignKeyColumnMapping;
			private Column<T, ?> indexColumn;
			
			private <LEFTTABLE extends Table<LEFTTABLE>> IndexedElementRecordIdentifierAssembler(
					T targetTable,
					EmbeddedClassMapping<ER, T> elementMapping,
					Column<T, ?> indexColumn,
					IdentifierAssembler<SRCID, LEFTTABLE> sourceIdentifierAssembler,
					Map<Column<LEFTTABLE, ?>, Column<T, ?>> foreignKeyColumnMapping) {
				super(targetTable);
				this.elementMapping = elementMapping;
				this.sourceIdentifierAssembler = sourceIdentifierAssembler;
				this.foreignKeyColumnMapping = (Map) foreignKeyColumnMapping;
				this.indexColumn = indexColumn;
			}
			
			public EmbeddedClassMapping<ER, T> getElementMapping() {
				return elementMapping;
			}
			
			@Override
			public ER assemble(ColumnedRow row) {
				return elementMapping.transform(row);
			}
			
			@Override
			public Map<Column<T, ?>, Object> getColumnValues(ER id) {
				Map<Column<?, ?>, Object> sourceColumnValues = (Map) sourceIdentifierAssembler.getColumnValues(id.getId());
				Map<Column<T, ?>, Object> idColumnValues = Maps.innerJoin(foreignKeyColumnMapping, sourceColumnValues);
				Map<Column<T, ?>, Object> result = new HashMap<>();
				result.putAll(idColumnValues);
				result.put(indexColumn, id.getIndex());
				return result;
			}
		}
	}
}
