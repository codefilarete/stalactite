package org.codefilarete.stalactite.engine.configurer;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.runtime.AssociationRecord;
import org.codefilarete.stalactite.engine.runtime.AssociationTable;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.mapping.ComposedIdMapping;
import org.codefilarete.stalactite.mapping.IdAccessor;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * @author Guillaume Mary
 */
public class AssociationRecordMapping<
		ASSOCIATIONTABLE extends AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, LEFTID, RIGHTID>,
		LEFTTABLE extends Table<LEFTTABLE>,
		RIGHTTABLE extends Table<RIGHTTABLE>,
		LEFTID,
		RIGHTID>
		extends ClassMapping<AssociationRecord, AssociationRecord, ASSOCIATIONTABLE> {
	
	public AssociationRecordMapping(ASSOCIATIONTABLE targetTable,
									IdentifierAssembler<LEFTID, LEFTTABLE> leftIdentifierAssembler,
									IdentifierAssembler<RIGHTID, RIGHTTABLE> rightIdentifierAssembler,
									Map<Column<LEFTTABLE, ?>, Column<ASSOCIATIONTABLE, ?>> leftIdentifierColumnMapping,
									Map<Column<RIGHTTABLE, ?>, Column<ASSOCIATIONTABLE, ?>> rightIdentifierColumnMapping
									) {
		super(AssociationRecord.class,
				targetTable,
				new HashMap<>(),
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
							public AssociationRecord assemble(Function<Column<?, ?>, Object> columnValueProvider) {
								LEFTID leftid = leftIdentifierAssembler.assemble(columnValueProvider);
								RIGHTID rightid = rightIdentifierAssembler.assemble(columnValueProvider);
								// we should not return an id if any (both expected in fact) value is null
								if (leftid == null || rightid == null) {
									return null;
								} else {
									return new AssociationRecord(leftid, rightid);
								}
							}
							
							@Override
							public Map<Column<ASSOCIATIONTABLE, ?>, Object> getColumnValues(AssociationRecord id) {
								Map<Column<LEFTTABLE, ?>, Object> leftValues = leftIdentifierAssembler.getColumnValues((LEFTID) id.getLeft());
								Map<Column<RIGHTTABLE, ?>, Object> rightValues = rightIdentifierAssembler.getColumnValues((RIGHTID) id.getRight());
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
