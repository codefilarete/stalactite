package org.codefilarete.stalactite.engine.configurer.resolver.separatefetch;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.runtime.load.EntityMerger;
import org.codefilarete.stalactite.mapping.AbstractTransformer;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.result.ColumnedRow;

public class FirstPhaseIndexedRelationLoader<SRC, TRGTID> implements EntityMerger<SRC> {
	
	private final Function<ColumnedRow, TRGTID> idMapping;
	private final ThreadLocal<IndexedRelationStorage<SRC, TRGTID>> relationIdsHolder;
	private final Column<?, Integer> indexColumn;
	private final Set<Selectable<?>> selectableColumns;
	
	public FirstPhaseIndexedRelationLoader(Function<ColumnedRow, TRGTID> idMapping,
	                                       Set<Selectable<?>> idColumns,
	                                       Column<?, Integer> indexColumn,
	                                       ThreadLocal<IndexedRelationStorage<SRC, TRGTID>> relationIdsHolder) {
		this.idMapping = idMapping;
		this.relationIdsHolder = relationIdsHolder;
		this.indexColumn = indexColumn;
		this.selectableColumns = new HashSet<>(idColumns);
		this.selectableColumns.add(indexColumn);
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
		IndexedRelationStorage<SRC, TRGTID> relationIds = relationIdsHolder.get();
		TRGTID id = idMapping.apply(row);
		relationIds.add(bean, id, row.get(indexColumn));
	}
}
