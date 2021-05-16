package org.gama.stalactite.persistence.engine.runtime;

import java.util.Queue;
import java.util.Set;

import org.gama.lang.collection.Arrays;
import org.gama.stalactite.persistence.engine.ISelectExecutor;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.EntityMerger;
import org.gama.stalactite.persistence.mapping.AbstractTransformer;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.persistence.mapping.IdMappingStrategy;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.result.Row;

/**
 * @author Guillaume Mary
 */
class FirstPhaseRelationLoader<E, ID, T extends Table> implements EntityMerger<E, T> {
	
	protected final Column<Table, ID> primaryKey;
	protected final IdMappingStrategy<E, ID> idMappingStrategy;
	private final ISelectExecutor<E, ID> selectExecutor;
	protected final ThreadLocal<Queue<Set<RelationIds<Object, Object, Object>>>> relationIdsHolder;
	
	public FirstPhaseRelationLoader(IdMappingStrategy<E, ID> subEntityIdMappingStrategy,
									Column<Table, ID> primaryKey,
									ISelectExecutor<E, ID> selectExecutor,
									ThreadLocal<Queue<Set<RelationIds<Object, Object, Object>>>> relationIdsHolder) {
		this.primaryKey = primaryKey;
		this.idMappingStrategy = subEntityIdMappingStrategy;
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
		relationIds.add(new RelationIds<>(selectExecutor, idMappingStrategy.getIdAccessor()::getId, bean, (ID) columnedRow.getValue(primaryKey, row)));
	}
}
