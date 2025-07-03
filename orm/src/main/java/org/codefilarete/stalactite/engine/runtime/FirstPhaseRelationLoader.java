package org.codefilarete.stalactite.engine.runtime;

import java.util.Queue;
import java.util.Set;

import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger;
import org.codefilarete.stalactite.mapping.AbstractTransformer;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.result.ColumnedRow;

/**
 * @author Guillaume Mary
 */
public class FirstPhaseRelationLoader<C, I> implements EntityMerger<C> {
	
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
	public AbstractTransformer<C> getRowTransformer() {
		return new AbstractTransformer<C>((Class) null) {
			
			@Override
			public void applyRowToBean(ColumnedRow row, C bean) {
				fillCurrentRelationIds(row, bean);
			}
		};
	}
	
	@Override
	public Set<Selectable<?>> getSelectableColumns() {
		return (Set) idMapping.getIdentifierAssembler().getColumns();
	}
	
	protected void fillCurrentRelationIds(ColumnedRow row, C bean) {
		Set<RelationIds<Object, C, I>> relationIds = relationIdsHolder.get().peek();
		I id = idMapping.getIdentifierAssembler().assemble(row);
		relationIds.add(new RelationIds<>(selectExecutor, idMapping.getIdAccessor()::getId, bean, id));
	}
}
