package org.codefilarete.stalactite.engine.runtime;

import java.util.Queue;
import java.util.Set;

import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger;
import org.codefilarete.stalactite.mapping.AbstractTransformer;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Row;

/**
 * @author Guillaume Mary
 */
class FirstPhaseRelationLoader<E, ID, T extends Table> implements EntityMerger<E, T> {
	
	protected final Column<Table, ID> primaryKey;
	protected final IdMapping<E, ID> idMapping;
	private final SelectExecutor<E, ID> selectExecutor;
	protected final ThreadLocal<Queue<Set<RelationIds<Object, Object, Object>>>> relationIdsHolder;
	
	public FirstPhaseRelationLoader(IdMapping<E, ID> subEntityIdMapping,
									Column<Table, ID> primaryKey,
									SelectExecutor<E, ID> selectExecutor,
									ThreadLocal<Queue<Set<RelationIds<Object, Object, Object>>>> relationIdsHolder) {
		this.primaryKey = primaryKey;
		this.idMapping = subEntityIdMapping;
		this.selectExecutor = selectExecutor;
		this.relationIdsHolder = relationIdsHolder;
	}
	
	@Override
	public AbstractTransformer<E> copyTransformerWithAliases(ColumnedRow columnedRow) {
		return new AbstractTransformer<E>(null, columnedRow) {
			
			// this is not invoked
			@Override
			public AbstractTransformer<E> copyWithAliases(ColumnedRow columnedRow) {
				throw new UnsupportedOperationException("this instance is not expected to be copied :"
						+ " row transformation algorithm as changed, please fix it or fix this method");
			}
			
			@Override
			public void applyRowToBean(Row row, E bean) {
				fillCurrentRelationIds(row, bean, columnedRow);
			}
		};
	}
	
	@Override
	public Set<Column<T, Object>> getSelectableColumns() {
		return (Set) Arrays.asHashSet(primaryKey);
	}
	
	protected void fillCurrentRelationIds(Row row, E bean, ColumnedRow columnedRow) {
		Set<RelationIds<Object, E, ID>> relationIds = ((Queue<Set<RelationIds<Object, E, ID>>>) (Queue) relationIdsHolder.get()).peek();
		relationIds.add(new RelationIds<>(selectExecutor, idMapping.getIdAccessor()::getId, bean, (ID) columnedRow.getValue(primaryKey, row)));
	}
}
