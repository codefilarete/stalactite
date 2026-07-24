package org.codefilarete.stalactite.engine.configurer.resolver.separatefetch;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.runtime.load.EntityMerger;
import org.codefilarete.stalactite.mapping.AbstractTransformer;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.sql.result.ColumnedRow;

public class FirstPhaseRelationLoader<SRC, TRGTID> implements EntityMerger<SRC> {
	
	private final Function<ColumnedRow, TRGTID> idMapping;
	private final ThreadLocal<RelationStorage<SRC, TRGTID>> relationIdsHolder;
	private final Set<Selectable<?>> selectableColumns;
	
	public FirstPhaseRelationLoader(Function<ColumnedRow, TRGTID> idMapping,
	                                Set<Selectable<?>> idColumns,
	                                ThreadLocal<RelationStorage<SRC, TRGTID>> relationIdsHolder) {
		this.idMapping = idMapping;
		this.relationIdsHolder = relationIdsHolder;
		this.selectableColumns = new HashSet<>(idColumns);
	}
	
	@Override
	public AbstractTransformer<SRC> getRowTransformer() {
		return new AbstractTransformer<SRC>((Class) null) {
			
			@Override
			public void applyRowToBean(ColumnedRow row, SRC bean) {
				fillCurrentRelationIds(row, bean);
			}
		};
	}
	
	@Override
	public Set<Selectable<?>> getSelectableColumns() {
		return selectableColumns;
	}
	
	protected void fillCurrentRelationIds(ColumnedRow row, SRC bean) {
		RelationStorage<SRC, TRGTID> relationIds = relationIdsHolder.get();
		TRGTID id = idMapping.apply(row);
		relationIds.add(bean, id);
	}
}
