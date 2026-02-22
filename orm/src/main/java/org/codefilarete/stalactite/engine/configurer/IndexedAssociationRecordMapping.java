package org.codefilarete.stalactite.engine.configurer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationRecord;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationTable;
import org.codefilarete.stalactite.mapping.DefaultEntityMapping;
import org.codefilarete.stalactite.mapping.ComposedIdMapping;
import org.codefilarete.stalactite.mapping.IdAccessor;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
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
		extends DefaultEntityMapping<IndexedAssociationRecord, IndexedAssociationRecord, ASSOCIATIONTABLE> {
	
	
	public IndexedAssociationRecordMapping(ASSOCIATIONTABLE targetTable,
										   IdentifierAssembler<LEFTID, LEFTTABLE> leftIdentifierAssembler,
										   IdentifierAssembler<RIGHTID, RIGHTTABLE> rightIdentifierAssembler,
										   Map<Column<LEFTTABLE, ?>, Column<ASSOCIATIONTABLE, ?>> leftIdentifierColumnMapping,
										   Map<Column<RIGHTTABLE, ?>, Column<ASSOCIATIONTABLE, ?>> rightIdentifierColumnMapping) {
		super(IndexedAssociationRecord.class,
				targetTable,
				Maps.forHashMap((Class<ReversibleAccessor<IndexedAssociationRecord, Object>>) (Class) ReversibleAccessor.class,
								(Class<Column<ASSOCIATIONTABLE, ?>>) (Class) Column.class)
						.add((ReversibleAccessor) IndexedAssociationRecord.INDEX_ACCESSOR, targetTable.getIndexColumn()),
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
							public IndexedAssociationRecord assemble(ColumnedRow columnValueProvider) {
								LEFTID leftid = leftIdentifierAssembler.assemble(new ColumnedRow() {
									@Override
									public <E> E get(Selectable<E> column) {
										Column<ASSOCIATIONTABLE, ?> column1 = leftIdentifierColumnMapping.get(column);
										return (E) columnValueProvider.get(column1);
									}
								});
								RIGHTID rightid = rightIdentifierAssembler.assemble(new ColumnedRow() {
									@Override
									public <E> E get(Selectable<E> column) {
										Column<ASSOCIATIONTABLE, ?> column1 = rightIdentifierColumnMapping.get(column);
										return (E) columnValueProvider.get(column1);
									}
								});
								// we should not return an id if any (both expected in fact) value is null
								if (leftid == null || rightid == null) {
									return null;
								} else {
									return new IndexedAssociationRecord(leftid, rightid, columnValueProvider.get(targetTable.getIndexColumn()));
								}
							}
							
							@Override
							public Map<Column<ASSOCIATIONTABLE, ?>, Object> getColumnValues(IndexedAssociationRecord id) {
								Map<Column<LEFTTABLE, ?>, ?> leftValues = leftIdentifierAssembler.getColumnValues((LEFTID) id.getLeft());
								Map<Column<RIGHTTABLE, ?>, ?> rightValues = rightIdentifierAssembler.getColumnValues((RIGHTID) id.getRight());
								Map<Column<ASSOCIATIONTABLE, ?>, Object> result = new HashMap<>();
								leftValues.forEach((key, value) -> result.put(leftIdentifierColumnMapping.get(key), value));
								rightValues.forEach((key, value) -> result.put(rightIdentifierColumnMapping.get(key), value));
								// because main mapping forbids to update primary key (see EmbeddedClassMapping), but index is part of it and will be updated,
								// we need to add it to the mapping
								result.put(targetTable.getIndexColumn(), id.getIndex());
								
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
	public Set<Column<ASSOCIATIONTABLE, ?>> getUpdatableColumns() {
		return Arrays.asHashSet(getTargetTable().getIndexColumn());
	}
}
