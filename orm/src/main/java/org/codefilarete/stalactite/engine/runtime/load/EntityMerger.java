package org.codefilarete.stalactite.engine.runtime.load;

import java.util.Set;

import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.RowTransformer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Contract to merge a row to some bean property (no bean creation, only property completion)
 */
public interface EntityMerger<E, T extends Table> {
	
	RowTransformer<E> copyTransformerWithAliases(ColumnedRow columnedRow);
	
	Set<Column<T, Object>> getSelectableColumns();
	
	/**
	 * Adapter of a {@link EntityMapping} as a {@link EntityMerger}.
	 * Implemented as a simple wrapper of a {@link EntityMapping} because methods are very close.
	 *
	 * @param <E> entity type
	 * @param <T> table type
	 */
	class EntityMergerAdapter<E, T extends Table> implements EntityMerger<E, T> {
		
		private final EntityMapping<E, ?, T> delegate;
		
		public EntityMergerAdapter(EntityMapping<E, ?, T> delegate) {
			this.delegate = delegate;
		}
		
		@Override
		public RowTransformer<E> copyTransformerWithAliases(ColumnedRow columnedRow) {
			return delegate.copyTransformerWithAliases(columnedRow);
		}
		
		@Override
		public Set<Column<T, Object>> getSelectableColumns() {
			return delegate.getSelectableColumns();
		}
	}
	
}
