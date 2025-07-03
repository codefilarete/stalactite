package org.codefilarete.stalactite.engine.runtime.load;

import java.util.Set;

import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.RowTransformer;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;

/**
 * Contract to deserialize a database row to a bean
 */
public interface EntityInflater<C, I> {
	
	Class<C> getEntityType();
	
	I giveIdentifier(ColumnedRow row);
	
	RowTransformer<C> getRowTransformer();
	
	Set<Selectable<?>> getSelectableColumns();
	
	/**
	 * Adapter of a {@link EntityMapping} as a {@link EntityInflater}.
	 * Implemented as a simple wrapper of a {@link EntityMapping} because methods are very close.
	 *
	 * @param <C> entity type
	 * @param <I> identifier type
	 * @param <T> table type
	 */
	class EntityMappingAdapter<C, I, T extends Table<T>> implements EntityInflater<C, I> {
		
		private final EntityMapping<C, I, T> delegate;
		
		public EntityMappingAdapter(EntityMapping<C, I, T> delegate) {
			this.delegate = delegate;
		}
		
		@Override
		public Class<C> getEntityType() {
			return this.delegate.getClassToPersist();
		}
		
		@Override
		public I giveIdentifier(ColumnedRow row) {
			return this.delegate.getIdMapping().getIdentifierAssembler().assemble(row);
		}
		
		@Override
		public RowTransformer<C> getRowTransformer() {
			return this.delegate.getRowTransformer();
		}
		
		@Override
		public Set<Selectable<?>> getSelectableColumns() {
			return (Set) this.delegate.getSelectableColumns();
		}
	}
	
}
