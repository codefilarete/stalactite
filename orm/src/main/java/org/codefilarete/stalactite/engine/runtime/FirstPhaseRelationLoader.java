package org.codefilarete.stalactite.engine.runtime;

import java.util.Queue;
import java.util.Set;

import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger;
import org.codefilarete.stalactite.mapping.AbstractTransformer;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.result.Row;

/**
 * @author Guillaume Mary
 */
class FirstPhaseRelationLoader<C, I> implements EntityMerger<C> {
	
	protected final IdMapping<C, I> idMapping;
	private final SelectExecutor<C, I> selectExecutor;
	protected final ThreadLocal<Queue<Set<RelationIds<Object, C, I>>>> relationIdsHolder;
	
	public FirstPhaseRelationLoader(IdMapping<C, I> subEntityIdMapping,
									SelectExecutor<C, I> selectExecutor,
									ThreadLocal<Queue<Set<RelationIds<Object, C, I>>>> relationIdsHolder) {
		this.idMapping = subEntityIdMapping;
		this.selectExecutor = selectExecutor;
		this.relationIdsHolder = relationIdsHolder;
	}
	
	@Override
	public AbstractTransformer<C> copyTransformerWithAliases(ColumnedRow columnedRow) {
		return new AbstractTransformer<C>(null, columnedRow) {
			
			// this is not invoked
			@Override
			public AbstractTransformer<C> copyWithAliases(ColumnedRow columnedRow) {
				throw new UnsupportedOperationException("this instance is not expected to be copied :"
						+ " row transformation algorithm as changed, please fix it or fix this method");
			}
			
			@Override
			public void applyRowToBean(Row row, C bean) {
				fillCurrentRelationIds(row, bean, columnedRow);
			}
		};
	}
	
	@Override
	public Set<Selectable<Object>> getSelectableColumns() {
		return (Set) idMapping.getIdentifierAssembler().getColumns();
	}
	
	protected void fillCurrentRelationIds(Row row, C bean, ColumnedRow columnedRow) {
		Set<RelationIds<Object, C, I>> relationIds = relationIdsHolder.get().peek();
		I id = idMapping.getIdentifierAssembler().assemble(row, columnedRow);
		relationIds.add(new RelationIds<>(selectExecutor, idMapping.getIdAccessor()::getId, bean, id));
	}
}
