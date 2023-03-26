package org.codefilarete.stalactite.engine.configurer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationRecord;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationTable;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.ComposedIdMapping;
import org.codefilarete.stalactite.mapping.IdAccessor;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Maps;

/**
 * @author Guillaume Mary
 */
public class IndexedAssociationRecordMapping<
		ASSOCIATIONTABLE extends IndexedAssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, LEFTID, RIGHTID>,
		LEFTTABLE extends Table<LEFTTABLE>,
		RIGHTTABLE extends Table<RIGHTTABLE>,
		LEFTID,
		RIGHTID>
		extends ClassMapping<IndexedAssociationRecord, IndexedAssociationRecord, ASSOCIATIONTABLE> {
	
	
	public IndexedAssociationRecordMapping(ASSOCIATIONTABLE targetTable,
										   IdentifierAssembler<LEFTID, LEFTTABLE> leftIdentifierAssembler,
										   IdentifierAssembler<RIGHTID, RIGHTTABLE> rightIdentifierAssembler,
										   Map<Column<LEFTTABLE, Object>, Column<ASSOCIATIONTABLE, Object>> leftIdentifierColumnMapping,
										   Map<Column<RIGHTTABLE, Object>, Column<ASSOCIATIONTABLE, Object>> rightIdentifierColumnMapping) {
		super(IndexedAssociationRecord.class,
				targetTable,
				Maps.forHashMap((Class<ReversibleAccessor<IndexedAssociationRecord, Object>>) (Class) ReversibleAccessor.class,
								(Class<Column<ASSOCIATIONTABLE, Object>>) (Class) Column.class)
						.add((ReversibleAccessor) IndexedAssociationRecord.INDEX_ACCESSOR, (Column) targetTable.getIndexColumn()),
				new ComposedIdMapping<IndexedAssociationRecord, IndexedAssociationRecord>(
						new IdAccessor<IndexedAssociationRecord, IndexedAssociationRecord>() {
							@Override
							public IndexedAssociationRecord getId(IndexedAssociationRecord associationRecord) {
								return associationRecord;
							}
							
							@Override
							public void setId(IndexedAssociationRecord associationRecord, IndexedAssociationRecord identifier) {
								associationRecord.setLeft(identifier.getLeft());
								associationRecord.setRight(identifier.getRight());
								associationRecord.setIndex(identifier.getIndex());
							}
						},
						new AlreadyAssignedIdentifierManager<>(IndexedAssociationRecord.class, IndexedAssociationRecord::markAsPersisted, IndexedAssociationRecord::isPersisted),
						new ComposedIdentifierAssembler<IndexedAssociationRecord, ASSOCIATIONTABLE>(targetTable) {
							@Override
							public IndexedAssociationRecord assemble(Function<Column<?, ?>, Object> columnValueProvider) {
								LEFTID leftid = leftIdentifierAssembler.assemble(columnValueProvider);
								RIGHTID rightid = rightIdentifierAssembler.assemble(columnValueProvider);
								// we should not return an id if any (both expected in fact) value is null
								if (leftid == null || rightid == null) {
									return null;
								} else {
									return new IndexedAssociationRecord(leftid, rightid, (int) columnValueProvider.apply(targetTable.getIndexColumn()));
								}
							}
							
							@Override
							public Map<Column<ASSOCIATIONTABLE, Object>, Object> getColumnValues(IndexedAssociationRecord id) {
								Map<Column<LEFTTABLE, Object>, Object> leftValues = leftIdentifierAssembler.getColumnValues((LEFTID) id.getLeft());
								Map<Column<RIGHTTABLE, Object>, Object> rightValues = rightIdentifierAssembler.getColumnValues((RIGHTID) id.getRight());
								Map<Column<ASSOCIATIONTABLE, Object>, Object> result = new HashMap<>();
								Map<Column<LEFTTABLE, Object>, Column<ASSOCIATIONTABLE, Object>> leftIdentifierColumnMapping1 = leftIdentifierColumnMapping;
								Map<Column<RIGHTTABLE, Object>, Column<ASSOCIATIONTABLE, Object>> rightIdentifierColumnMapping1 = rightIdentifierColumnMapping;
								leftValues.forEach((key, value) -> result.put(leftIdentifierColumnMapping1.get(key), value));
								rightValues.forEach((key, value) -> result.put(rightIdentifierColumnMapping1.get(key), value));
								// because main mapping forbids to update primary key (see EmbeddedClassMapping), but index is part of it and will be updated,
								// we need to add it to the mapping
								result.put((Column) targetTable.getIndexColumn(), id.getIndex());
								
								return result;
							}
						}) {
					@Override
					public boolean isNew(IndexedAssociationRecord entity) {
						return !entity.isPersisted();
					}
				});
	}
	
	@Override
	public Set<Column<ASSOCIATIONTABLE, Object>> getUpdatableColumns() {
		return Arrays.asHashSet((Column) getTargetTable().getIndexColumn());
	}
}
