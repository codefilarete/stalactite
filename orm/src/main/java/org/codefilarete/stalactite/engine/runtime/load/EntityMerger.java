package org.codefilarete.stalactite.engine.runtime.load;

import java.util.Set;

import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.RowTransformer;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

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
		
		private final EntityMapping<C, ?, T> delegate;
		
		public EntityMergerAdapter(EntityMapping<C, ?, T> delegate) {
			this.delegate = delegate;
		}
		
		@Override
		public RowTransformer<C> getRowTransformer() {
			return delegate.getRowTransformer();
		}
		
		@Override
		public Set<Selectable<?>> getSelectableColumns() {
			return (Set) delegate.getSelectableColumns();
		}
	}
	
}
