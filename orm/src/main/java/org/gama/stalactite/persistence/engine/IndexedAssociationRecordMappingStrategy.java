package org.gama.stalactite.persistence.engine;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.gama.lang.collection.Arrays;
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
class IndexedAssociationRecordMappingStrategy extends ClassMappingStrategy<IndexedAssociationRecord, IndexedAssociationRecord, IndexedAssociationTable> {
	
	public IndexedAssociationRecordMappingStrategy(IndexedAssociationTable targetTable) {
		super(IndexedAssociationRecord.class, targetTable, Maps
						.asMap(IndexedAssociationRecord.LEFT_ACCESSOR, targetTable.getOneSideKeyColumn())
						.add(IndexedAssociationRecord.RIGHT_ACCESSOR, targetTable.getManySideKeyColumn())
						.add((IReversibleAccessor) IndexedAssociationRecord.INDEX_ACCESSOR, (Column) targetTable.getIndexColumn())
				,
				new ComposedIdMappingStrategy<>(new IdAccessor<IndexedAssociationRecord, IndexedAssociationRecord>() {
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
				}, new AlreadyAssignedIdentifierManager<>(IndexedAssociationRecord.class, c -> {}, c -> false), new ComposedIdentifierAssembler<IndexedAssociationRecord>(targetTable) {
					@Override
					protected IndexedAssociationRecord assemble(Map<Column, Object> primaryKeyElements) {
						return new IndexedAssociationRecord(
								primaryKeyElements.get(targetTable.getOneSideKeyColumn()),
								primaryKeyElements.get(targetTable.getManySideKeyColumn()),
								(int) primaryKeyElements.get(targetTable.getIndexColumn()));
					}
					
					@Override
					public Map<Column, Object> getColumnValues(@Nonnull IndexedAssociationRecord id) {
						return Maps.asMap(targetTable.getOneSideKeyColumn(), id.getLeft())
								.add(targetTable.getManySideKeyColumn(), id.getRight())
								.add((Column) targetTable.getIndexColumn(), id.getIndex())
								.add(targetTable.getIndexColumn(), id.getIndex());
					}
				}));
		// because main mapping forbids to update primary key (see EmbeddedBeanMappingStrategy), but index is part of it and will be updated,
		// we need to add it to the mapping
		getMainMappingStrategy().addSilentColumnInserter((Column) targetTable.getIndexColumn(), (Function<IndexedAssociationRecord, Object>) IndexedAssociationRecord.INDEX_ACCESSOR::get);
		getMainMappingStrategy().addSilentColumnUpdater((Column) targetTable.getIndexColumn(), (Function<IndexedAssociationRecord, Object>) IndexedAssociationRecord.INDEX_ACCESSOR::get);
	}
	
	@Override
	public Set<Column<IndexedAssociationTable, Object>> getUpdatableColumns() {
		return Arrays.asHashSet((Column) getTargetTable().getIndexColumn());
	}
}
