package org.codefilarete.stalactite.engine.runtime.load;

import java.util.Set;

import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.RowTransformer;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Row;

/**
 * Contract to deserialize a database row to a bean
 */
public interface EntityInflater<C, I> {
	
	Class<C> getEntityType();
	
	I giveIdentifier(Row row, ColumnedRow columnedRow);
	
	RowTransformer<C> copyTransformerWithAliases(ColumnedRow columnedRow);
	
	Set<Selectable<Object>> getSelectableColumns();
	
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
		public I giveIdentifier(Row row, ColumnedRow columnedRow) {
			return this.delegate.getIdMapping().getIdentifierAssembler().assemble(row, columnedRow);
		}
		
		@Override
		public RowTransformer<C> copyTransformerWithAliases(ColumnedRow columnedRow) {
			return this.delegate.copyTransformerWithAliases(columnedRow);
		}
		
		@Override
		public Set<Selectable<Object>> getSelectableColumns() {
			return (Set) this.delegate.getSelectableColumns();
		}
	}
	
}
