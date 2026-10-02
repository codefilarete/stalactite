package org.codefilarete.stalactite.engine.configurer.resolver.separatefetch;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.runtime.load.EntityMerger;
import org.codefilarete.stalactite.mapping.AbstractTransformer;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.result.ColumnedRow;

/**
 * Class aimed at collecting target entity ids for each source entity, in order to load them in a second phase.
 * Target identifiers are collected from {@link java.sql.ResultSet} rows, represented by {@link ColumnedRow}, and stored
 * in a {@link ThreadLocal} variable, thus they are available in a second phase (by the same thread), after the source
 * entities have been loaded.
 * 
 * @param <SRC>
 * @param <TRGTID>
 */
public class FirstPhaseIndexedManyRelationLoader<SRC, TRGTID> implements EntityMerger<SRC> {
	
	private final Function<ColumnedRow, TRGTID> idMapping;
	private final ThreadLocalStorage<SRC, Map<Integer, TRGTID>, IndexedRelationStorage<SRC, TRGTID>> relationIdsHolder;
	private final Column<?, Integer> indexColumn;
	private final Set<Selectable<?>> selectableColumns;
	
	public FirstPhaseIndexedManyRelationLoader(Function<ColumnedRow, TRGTID> idMapping,
											   Set<? extends Selectable<?>> idColumns,
											   ThreadLocalStorage<SRC, Map<Integer, TRGTID>, IndexedRelationStorage<SRC, TRGTID>> relationIdsHolder,
											   Column<?, Integer> indexColumn) {
		this.idMapping = idMapping;
		this.relationIdsHolder = relationIdsHolder;
		this.indexColumn = indexColumn;
		this.selectableColumns = new HashSet<>(idColumns);
	}
	
	public ThreadLocalStorage<SRC, Map<Integer, TRGTID>, IndexedRelationStorage<SRC, TRGTID>> getRelationIdsHolder() {
		return relationIdsHolder;
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
		TRGTID id = idMapping.apply(row);
		if (id != null) {
			relationIdsHolder.getStorage().add(bean, id, row.get(indexColumn));
		}
	}
}
