package org.codefilarete.stalactite.engine.configurer;

import java.util.HashMap;
import java.util.Map;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.runtime.AssociationRecord;
import org.codefilarete.stalactite.engine.runtime.AssociationTable;
import org.codefilarete.stalactite.mapping.DefaultEntityMapping;
import org.codefilarete.stalactite.mapping.ComposedIdMapping;
import org.codefilarete.stalactite.mapping.IdAccessor;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.SingleIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Maps;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * @author Guillaume Mary
 */
public class AssociationRecordMapping<
		ASSOCIATIONTABLE extends AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, LEFTID, RIGHTID>,
		LEFTTABLE extends Table<LEFTTABLE>,
		RIGHTTABLE extends Table<RIGHTTABLE>,
		LEFTID,
		RIGHTID>
		extends DefaultEntityMapping<AssociationRecord, AssociationRecord, ASSOCIATIONTABLE> {
	
	/**
	 * Computes a mapping from the {@link AssociationRecord} class to the association table's column.
	 * Handles both single-key and composite-key identifiers.
	 * 
	 * @param identifierColumnMapping identifier column mapping
	 * @param identifierAssembler identifier assembler
	 * @param valueAccessor value accessor
	 * @param valueMutator value mutator
	 * @return a mapping between the association record property and the association table's column.
	 * @param <ASSOCIATIONTABLE> association table type
	 * @param <TARGETTABLE> target table type, which can be the left or right one
	 * @param <ID> identifier type (simple type or complex one)
	 */
	private static <ASSOCIATIONTABLE extends Table<ASSOCIATIONTABLE>, TARGETTABLE extends Table<TARGETTABLE>, ID>
	Map<ReversibleAccessor<AssociationRecord, Object>, Column<ASSOCIATIONTABLE, Object>> mapping(
			Map<Column<TARGETTABLE, ?>, Column<ASSOCIATIONTABLE, ?>> identifierColumnMapping,
			IdentifierAssembler<ID, TARGETTABLE> identifierAssembler,
			SerializableFunction<AssociationRecord, Object> valueAccessor,
			SerializableBiConsumer<AssociationRecord, Object> valueMutator) {
		Map<ReversibleAccessor<AssociationRecord, Object>, Column<ASSOCIATIONTABLE, Object>> result = new HashMap<>();
		if (identifierAssembler instanceof SingleIdentifierAssembler) {
			ReversibleAccessor<AssociationRecord, Object> reversibleAccessor = PropertyAccessor.fromMethodReference(valueAccessor, valueMutator);
			Column<TARGETTABLE, ID> column = ((SingleIdentifierAssembler<ID, TARGETTABLE>) identifierAssembler).getColumn();
			Column<ASSOCIATIONTABLE, ID> associationtableColumn = (Column<ASSOCIATIONTABLE, ID>) identifierColumnMapping.get(column);
			result.put(reversibleAccessor, (Column<ASSOCIATIONTABLE, Object>) associationtableColumn);
		} else if (identifierAssembler instanceof DefaultComposedIdentifierAssembler) {
			DefaultComposedIdentifierAssembler<ID, TARGETTABLE> leftIdentifierAssembler1 = (DefaultComposedIdentifierAssembler<ID, TARGETTABLE>) identifierAssembler;
			Map<ReversibleAccessor<ID, ?>, Column<TARGETTABLE, ?>> mapping = leftIdentifierAssembler1.getMapping();
			mapping.forEach((accessor, column) -> {
				AccessorChain<AssociationRecord, Object> propertyAccessor = new AccessorChain<>(Accessors.accessor(valueAccessor), accessor);
				propertyAccessor.setNullValueHandler(new AccessorChain.ValueInitializerOnNullValue((accessor1, aClass) -> Reflections.newInstance(leftIdentifierAssembler1.getDefaultConstructor())));
				result.put(propertyAccessor, (Column<ASSOCIATIONTABLE, Object>) identifierColumnMapping.get(column));
			});
		}
		return result;
	}

	public AssociationRecordMapping(ASSOCIATIONTABLE targetTable,
									IdentifierAssembler<LEFTID, LEFTTABLE> leftIdentifierAssembler,
									IdentifierAssembler<RIGHTID, RIGHTTABLE> rightIdentifierAssembler) {
		this(targetTable, leftIdentifierAssembler, rightIdentifierAssembler, targetTable.getLeftIdentifierColumnMapping(), targetTable.getRightIdentifierColumnMapping());
	}
	
	private AssociationRecordMapping(ASSOCIATIONTABLE targetTable,
									IdentifierAssembler<LEFTID, LEFTTABLE> leftIdentifierAssembler,
									IdentifierAssembler<RIGHTID, RIGHTTABLE> rightIdentifierAssembler,
									Map<Column<LEFTTABLE, ?>, Column<ASSOCIATIONTABLE, ?>> leftIdentifierColumnMapping,
									Map<Column<RIGHTTABLE, ?>, Column<ASSOCIATIONTABLE, ?>> rightIdentifierColumnMapping
									) {
		super(AssociationRecord.class,
				targetTable,
				Maps.putAll(
						mapping(leftIdentifierColumnMapping, leftIdentifierAssembler, AssociationRecord::getLeft, AssociationRecord::setLeft),
						mapping(rightIdentifierColumnMapping, rightIdentifierAssembler, AssociationRecord::getRight, AssociationRecord::setRight)),
				new ComposedIdMapping<AssociationRecord, AssociationRecord>(
						new IdAccessor<AssociationRecord, AssociationRecord>() {
							@Override
							public AssociationRecord getId(AssociationRecord associationRecord) {
								return associationRecord;
							}
							
							@Override
							public void setId(AssociationRecord associationRecord, AssociationRecord identifier) {
								associationRecord.setLeft(identifier.getLeft());
								associationRecord.setRight(identifier.getRight());
							}
						}, new AlreadyAssignedIdentifierManager<>(AssociationRecord.class, AssociationRecord::markAsPersisted, AssociationRecord::isPersisted),
						new ComposedIdentifierAssembler<AssociationRecord, ASSOCIATIONTABLE>(targetTable) {
							@Override
							public AssociationRecord assemble(ColumnedRow columnValueProvider) {
								LEFTID leftid = leftIdentifierAssembler.assemble(new ColumnedRow() {
									@Override
									public <E> E get(Selectable<E> column) {
										return (E) columnValueProvider.get(leftIdentifierColumnMapping.get(column));
									}
								});
								RIGHTID rightid = rightIdentifierAssembler.assemble(new ColumnedRow() {
									@Override
									public <E> E get(Selectable<E> column) {
										return (E) columnValueProvider.get(rightIdentifierColumnMapping.get(column));
									}
								});
								// we should not return an id if any (both expected in fact) value is null
								if (leftid == null || rightid == null) {
									return null;
								} else {
									return new AssociationRecord(leftid, rightid);
								}
							}
							
							@Override
							public Map<Column<ASSOCIATIONTABLE, ?>, Object> getColumnValues(AssociationRecord id) {
								Map<Column<LEFTTABLE, ?>, ?> leftValues = leftIdentifierAssembler.getColumnValues((LEFTID) id.getLeft());
								Map<Column<RIGHTTABLE, ?>, ?> rightValues = rightIdentifierAssembler.getColumnValues((RIGHTID) id.getRight());
								Map<Column<ASSOCIATIONTABLE, ?>, Object> result = new HashMap<>();
								leftValues.forEach((key, value) -> result.put(leftIdentifierColumnMapping.get(key), value));
								rightValues.forEach((key, value) -> result.put(rightIdentifierColumnMapping.get(key), value));
								return result;
							}
						}) {
					
					@Override
					public boolean isNew(AssociationRecord entity) {
						return !entity.isPersisted();
					}
				});
	}
}
