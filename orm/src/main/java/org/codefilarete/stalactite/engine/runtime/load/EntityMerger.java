package org.codefilarete.stalactite.engine.runtime.load;

import java.util.Set;

import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.RowTransformer;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * Contract to merge a row to some bean property (no bean creation, only property completion)
 */
public interface EntityMerger<C> {
	
	RowTransformer<C> getRowTransformer();
	
	Set<Selectable<?>> getSelectableColumns();
	
	/**
	 * Adapter of a {@link EntityMapping} as a {@link EntityMerger}.
	 * Implemented as a simple wrapper of a {@link EntityMapping} because methods are very close.
	 *
	 * @param <C> entity type
	 * @param <T> table type
	 */
	class EntityMergerAdapter<C, T extends Table<T>> implements EntityMerger<C> {
		
		private Set<Column<T, ?>> selectableColumns;
		private RowTransformer<C> rowTransformer;
		
		public EntityMergerAdapter(EntityMapping<C, ?, T> delegate) {
			// We create a full clone of the current selectable columns to capture them and avoid later changes, which can hardly change except when
			// shadow columns are added to the DefaultEntityDefaultEntityMapping: this happens in one-to-many mapped association and many side is a table-per-class case.
			// This is a bit of a hack, but it globally makes sense, even if doing it only fixes the above case (no problem with other polymorphic
			// cases, which is normal because their algorithms are different)
			this.selectableColumns = new KeepOrderSet<>(delegate.getSelectableColumns());
			this.rowTransformer = delegate.getRowTransformer();
		}
		
		@Override
		public RowTransformer<C> getRowTransformer() {
			return rowTransformer;
		}
		
		@Override
		public Set<Selectable<?>> getSelectableColumns() {
			return (Set) selectableColumns;
		}
	}
	
}
