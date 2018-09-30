package org.gama.stalactite.persistence.engine;

import javax.annotation.Nonnull;
import java.util.Map;

import org.gama.lang.collection.Maps;
import org.gama.reflection.IReversibleAccessor;
import org.gama.stalactite.persistence.id.assembly.ComposedIdentifierAssembler;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.ComposedIdMappingStrategy;
import org.gama.stalactite.persistence.mapping.IdAccessor;
import org.gama.stalactite.persistence.structure.Column;

/**
 * @author Guillaume Mary
 */
class AssociationRecordMappingStrategy extends ClassMappingStrategy<AssociationRecord, AssociationRecord, AssociationTable> {
	
	public AssociationRecordMappingStrategy(AssociationTable targetTable) {
		super(AssociationRecord.class, targetTable, Maps
						.asMap(AssociationRecord.LEFT_ACCESSOR, targetTable.getOneSideKeyColumn())
						.add((IReversibleAccessor) AssociationRecord.RIGHT_ACCESSOR, targetTable.getManySideKeyColumn())
				,
				new ComposedIdMappingStrategy<>(new IdAccessor<AssociationRecord, AssociationRecord>() {
					@Override
					public AssociationRecord getId(AssociationRecord associationRecord) {
						return associationRecord;
					}
					
					@Override
					public void setId(AssociationRecord associationRecord, AssociationRecord identifier) {
						associationRecord.setLeft(identifier.getLeft());
						associationRecord.setRight(identifier.getRight());
					}
				}, new AlreadyAssignedIdentifierManager<>(AssociationRecord.class), new ComposedIdentifierAssembler<AssociationRecord>(targetTable) {
					@Override
					protected AssociationRecord assemble(Map<Column, Object> primaryKeyElements) {
						return new AssociationRecord(
								primaryKeyElements.get(targetTable.getOneSideKeyColumn()),
								primaryKeyElements.get(targetTable.getManySideKeyColumn())
						);
					}
					
					@Override
					public Map<Column, Object> getColumnValues(@Nonnull AssociationRecord id) {
						return Maps.asMap(targetTable.getOneSideKeyColumn(), id.getLeft())
								.add(targetTable.getManySideKeyColumn(), id.getRight());
					}
				}));
	}
}
