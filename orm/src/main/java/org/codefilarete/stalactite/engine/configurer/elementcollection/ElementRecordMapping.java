package org.codefilarete.stalactite.engine.configurer.elementcollection;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.ComposedIdMapping;
import org.codefilarete.stalactite.mapping.EmbeddedClassMapping;
import org.codefilarete.stalactite.mapping.IdAccessor;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.collection.Maps;

/**
 * Class mapping dedicated to {@link ElementRecord}. Very close to {@link org.codefilarete.stalactite.engine.configurer.AssociationRecordMapping}
 * in its principle.
 */
public class ElementRecordMapping<C, I, T extends Table<T>> extends ClassMapping<ElementRecord<C, I>, ElementRecord<C, I>, T> {
	
	@VisibleForTesting
	public <LEFTTABLE extends Table<LEFTTABLE>> ElementRecordMapping(T targetTable,
															  Column<T, C> elementColumn,
															  IdentifierAssembler<I, LEFTTABLE> sourceIdentifierAssembler,
															  Map<Column<LEFTTABLE, ?>, Column<T, ?>> primaryKeyForeignColumnMapping) {
		super((Class) ElementRecord.class,
				targetTable,
				(Map) Maps.forHashMap(ReversibleAccessor.class, Column.class)
						.add(ElementRecord.ELEMENT_ACCESSOR, elementColumn),
				new ElementRecordIdMapping<>(targetTable, elementColumn, sourceIdentifierAssembler, primaryKeyForeignColumnMapping));
	}
	
	<LEFTTABLE extends Table<LEFTTABLE>> ElementRecordMapping(T targetTable,
															  EmbeddedClassMapping<ElementRecord<C, I>, T> embeddableMapping,
															  IdentifierAssembler<I, LEFTTABLE> sourceIdentifierAssembler,
															  Map<Column<LEFTTABLE, ?>, Column<T, ?>> primaryKeyForeignColumnMapping) {
		super((Class) ElementRecord.class,
				targetTable,
				embeddableMapping.getPropertyToColumn(),
				new ElementRecordIdMapping<>(targetTable, embeddableMapping, sourceIdentifierAssembler, primaryKeyForeignColumnMapping));
	}
	
	/**
	 * {@link IdMapping} for {@link ElementRecord} : a composed id made of :
	 * - {@link ElementRecord#getId()}
	 * - {@link ElementRecord#getElement()}
	 */
	public static class ElementRecordIdMapping<C, I, T extends Table<T>> extends ComposedIdMapping<ElementRecord<C, I>, ElementRecord<C, I>> {
		
		public <LEFTTABLE extends Table<LEFTTABLE>> ElementRecordIdMapping(
				T targetTable,
				Column<T, C> elementColumn,
				IdentifierAssembler<I, LEFTTABLE> sourceIdentifierAssembler,
				Map<Column<LEFTTABLE, ?>, Column<T, ?>> primaryKeyForeignColumnMapping) {
			super(new ElementRecordIdMapping.ElementRecordIdAccessor<>(),
					new AlreadyAssignedIdentifierManager<>((Class<ElementRecord<C, I>>) (Class) ElementRecord.class,
							ElementRecord::markAsPersisted,
							ElementRecord::isPersisted),
					new ElementRecordIdMapping.DefaultElementRecordIdentifierAssembler<>(targetTable, elementColumn, sourceIdentifierAssembler, primaryKeyForeignColumnMapping));
		}
		
		public <LEFTTABLE extends Table<LEFTTABLE>> ElementRecordIdMapping(
				T targetTable,
				EmbeddedClassMapping<ElementRecord<C, I>, T> elementMapping,
				IdentifierAssembler<I, LEFTTABLE> sourceIdentifierAssembler,
				Map<Column<LEFTTABLE, ?>, Column<T, ?>> primaryKeyForeignColumnMapping) {
			super(new ElementRecordIdMapping.ElementRecordIdAccessor<>(),
					new AlreadyAssignedIdentifierManager<>((Class<ElementRecord<C, I>>) (Class) ElementRecord.class,
							ElementRecord::markAsPersisted,
							ElementRecord::isPersisted),
					new ElementRecordIdentifierAssembler<>(targetTable, elementMapping, sourceIdentifierAssembler, primaryKeyForeignColumnMapping));
		}
		
		/**
		 * Override because {@link ComposedIdMapping} is based on null identifier to determine newness, which is always false for {@link ElementRecord}
		 * because they always have one. We delegate its computation to the entity.
		 *
		 * @param entity any non-null entity
		 * @return true or false based on {@link ElementRecord#isNew()}
		 */
		@Override
		public boolean isNew(ElementRecord<C, I> entity) {
			return entity.isNew();
		}
		
		private static class ElementRecordIdAccessor<C, I> implements IdAccessor<ElementRecord<C, I>, ElementRecord<C, I>> {
			
			@Override
			public ElementRecord<C, I> getId(ElementRecord<C, I> associationRecord) {
				return associationRecord;
			}
			
			@Override
			public void setId(ElementRecord<C, I> associationRecord, ElementRecord<C, I> identifier) {
				associationRecord.setId(identifier.getId());
				associationRecord.setElement(identifier.getElement());
			}
		}
		
		/**
		 * Identifier assembler when {@link ElementRecord} is persisted according to a default behavior :
		 * - identifier is saved in table primary key
		 * - element value is saved in elementColumn
		 *
		 * @param <TRGT> embedded bean type
		 * @param <ID> source identifier type
		 */
		public static class DefaultElementRecordIdentifierAssembler<TRGT, ID, T extends Table<T>> extends ComposedIdentifierAssembler<ElementRecord<TRGT, ID>, T> {
			
			private final Column<T, TRGT> elementColumn;
			private final IdentifierAssembler<ID, ?> sourceIdentifierAssembler;
			private final Map<Column<?, ?>, Column<T, ?>> primaryKeyForeignColumnMapping;
			
			private <LEFTTABLE extends Table<LEFTTABLE>> DefaultElementRecordIdentifierAssembler(T targetTable,
																								 Column<T, TRGT> elementColumn,
																								 IdentifierAssembler<ID, LEFTTABLE> sourceIdentifierAssembler,
																								 Map<Column<LEFTTABLE, ?>, Column<T, ?>> primaryKeyForeignColumnMapping) {
				super(targetTable);
				this.elementColumn = elementColumn;
				this.sourceIdentifierAssembler = sourceIdentifierAssembler;
				this.primaryKeyForeignColumnMapping = (Map) primaryKeyForeignColumnMapping;
			}
			
			public Column<T, TRGT> getElementColumn() {
				return elementColumn;
			}
			
			public IdentifierAssembler<ID, ?> getSourceIdentifierAssembler() {
				return sourceIdentifierAssembler;
			}
			
			@Override
			public ElementRecord<TRGT, ID> assemble(ColumnedRow columnValueProvider) {
				ID leftValue = sourceIdentifierAssembler.assemble(new ColumnedRow() {
					@Override
					public <E> E get(Selectable<E> sourceColumn) {
						Column<T, E> targetColumn = (Column<T, E>) primaryKeyForeignColumnMapping.get(sourceColumn);
						return columnValueProvider.get(targetColumn);
					}
				});
				TRGT rightValue = columnValueProvider.get(elementColumn);
				// we should not return an id if any (both expected in fact) value is null
				if (leftValue == null || rightValue == null) {
					return null;
				} else {
					return new ElementRecord<>(leftValue, rightValue);
				}
			}
			
			@Override
			public Map<Column<T, ?>, Object> getColumnValues(ElementRecord<TRGT, ID> id) {
				Map<Column<?, ?>, Object> sourceColumnValues = (Map) sourceIdentifierAssembler.getColumnValues(id.getId());
				Map<Column<T, ?>, Object> idColumnValues = Maps.innerJoin(primaryKeyForeignColumnMapping, sourceColumnValues);
				Map<Column<T, ?>, Object> result = new HashMap<>();
				result.put(elementColumn, id.getElement());
				result.putAll(idColumnValues);
				return result;
			}
		}
		
		/**
		 * Identifier assembler for cases where user gave a configuration to persist embedded beans (default way is not used)
		 *
		 * @param <TRGT> embedded bean type
		 * @param <ID> source identifier type
		 */
		public static class ElementRecordIdentifierAssembler<TRGT, ID, T extends Table<T>> extends ComposedIdentifierAssembler<ElementRecord<TRGT, ID>, T> {
			
			private final EmbeddedClassMapping<ElementRecord<TRGT, ID>, T> elementMapping;
			private final IdentifierAssembler<ID, ?> sourceIdentifierAssembler;
			private final Map<Column<?, ?>, Column<T, ?>> primaryKeyForeignColumnMapping;
			
			private <LEFTTABLE extends Table<LEFTTABLE>> ElementRecordIdentifierAssembler(
					T targetTable,
					EmbeddedClassMapping<ElementRecord<TRGT, ID>, T> elementMapping,
					IdentifierAssembler<ID, LEFTTABLE> sourceIdentifierAssembler,
					Map<Column<LEFTTABLE, ?>, Column<T, ?>> primaryKeyForeignColumnMapping) {
				super(targetTable);
				this.elementMapping = elementMapping;
				this.sourceIdentifierAssembler = sourceIdentifierAssembler;
				this.primaryKeyForeignColumnMapping = (Map) primaryKeyForeignColumnMapping;
			}
			
			public EmbeddedClassMapping<ElementRecord<TRGT, ID>, T> getElementMapping() {
				return elementMapping;
			}
			
			@Override
			public ElementRecord<TRGT, ID> assemble(ColumnedRow row) {
				return elementMapping.transform(row);
			}
			
			@Override
			public Map<Column<T, ?>, Object> getColumnValues(ElementRecord<TRGT, ID> id) {
				Map<Column<?, ?>, Object> sourceColumnValues = (Map) sourceIdentifierAssembler.getColumnValues(id.getId());
				Map<Column<T, ?>, Object> idColumnValues = Maps.innerJoin(primaryKeyForeignColumnMapping, sourceColumnValues);
				Map<Column<T, ?>, Object> result = new HashMap<>();
				result.putAll(idColumnValues);
				result.putAll(elementMapping.getInsertValues(id));
				return result;
			}
		}
	}
}
