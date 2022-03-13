package org.codefilarete.stalactite.engine.configurer;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;

import org.codefilarete.stalactite.engine.runtime.IndexedAssociationRecord;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationTable;
import org.codefilarete.tool.collection.Arrays;
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
class IndexedAssociationRecordMappingStrategy extends ClassMappingStrategy<IndexedAssociationRecord, IndexedAssociationRecord, IndexedAssociationTable> {
	
	public IndexedAssociationRecordMappingStrategy(IndexedAssociationTable targetTable) {
		super(IndexedAssociationRecord.class, targetTable, Maps
						.asMap(IndexedAssociationRecord.LEFT_ACCESSOR, targetTable.getOneSideKeyColumn())
						.add(IndexedAssociationRecord.RIGHT_ACCESSOR, targetTable.getManySideKeyColumn())
						.add((ReversibleAccessor) IndexedAssociationRecord.INDEX_ACCESSOR, (Column) targetTable.getIndexColumn())
				,
				new ComposedIdMappingStrategy<IndexedAssociationRecord, IndexedAssociationRecord>(new IdAccessor<IndexedAssociationRecord, IndexedAssociationRecord>() {
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
				}, new AlreadyAssignedIdentifierManager<>(IndexedAssociationRecord.class, IndexedAssociationRecord::markAsPersisted, IndexedAssociationRecord::isPersisted),
						new ComposedIdentifierAssembler<IndexedAssociationRecord>(targetTable) {
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
						}) {
					@Override
					public boolean isNew(@Nonnull IndexedAssociationRecord entity) {
						return !entity.isPersisted();
					}
				});
		// because main mapping forbids to update primary key (see EmbeddedClassMappingStrategy), but index is part of it and will be updated,
		// we need to add it to the mapping
		ShadowColumnValueProvider<IndexedAssociationRecord, Integer, IndexedAssociationTable> indexValueProvider =
				new ShadowColumnValueProvider<>(targetTable.getIndexColumn(), IndexedAssociationRecord.INDEX_ACCESSOR::get);
		getMainMappingStrategy().addShadowColumnInsert(indexValueProvider);
		getMainMappingStrategy().addShadowColumnUpdate(indexValueProvider);
	}
	
	@Override
	public Set<Column<IndexedAssociationTable, Object>> getUpdatableColumns() {
		return Arrays.asHashSet((Column) getTargetTable().getIndexColumn());
	}
}
