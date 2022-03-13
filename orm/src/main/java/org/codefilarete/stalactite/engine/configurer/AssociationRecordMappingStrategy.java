package org.codefilarete.stalactite.engine.configurer;

import javax.annotation.Nonnull;
import java.util.Map;

import org.codefilarete.stalactite.engine.runtime.AssociationRecord;
import org.codefilarete.stalactite.engine.runtime.AssociationTable;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.mapping.ClassMappingStrategy;
import org.codefilarete.stalactite.mapping.ComposedIdMappingStrategy;
import org.codefilarete.stalactite.mapping.IdAccessor;
import org.codefilarete.stalactite.sql.ddl.structure.Column;

/**
 * @author Guillaume Mary
 */
class AssociationRecordMappingStrategy extends ClassMappingStrategy<AssociationRecord, AssociationRecord, AssociationTable> {
	
	public AssociationRecordMappingStrategy(AssociationTable targetTable) {
		super(AssociationRecord.class, targetTable, (Map) Maps
						.forHashMap(ReversibleAccessor.class, Column.class)
						.add(AssociationRecord.LEFT_ACCESSOR, targetTable.getOneSideKeyColumn())
						.add(AssociationRecord.RIGHT_ACCESSOR, targetTable.getManySideKeyColumn())
				,
				new ComposedIdMappingStrategy<AssociationRecord, AssociationRecord>(new IdAccessor<AssociationRecord, AssociationRecord>() {
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
						new ComposedIdentifierAssembler<AssociationRecord>(targetTable) {
							@Override
							protected AssociationRecord assemble(Map<Column, Object> primaryKeyElements) {
								Object leftValue = primaryKeyElements.get(targetTable.getOneSideKeyColumn());
								Object rightValue = primaryKeyElements.get(targetTable.getManySideKeyColumn());
								// we should not return an id if any (both expected in fact) value is null
								if (leftValue == null || rightValue == null) {
									return null;
								} else {
									return new AssociationRecord(leftValue, rightValue);
								}
							}
							
							@Override
							public Map<Column, Object> getColumnValues(@Nonnull AssociationRecord id) {
								return Maps.asMap(targetTable.getOneSideKeyColumn(), id.getLeft())
										.add(targetTable.getManySideKeyColumn(), id.getRight());
							}
						}) {
					
					@Override
					public boolean isNew(@Nonnull AssociationRecord entity) {
						return !entity.isPersisted();
					}
				});
	}
}
