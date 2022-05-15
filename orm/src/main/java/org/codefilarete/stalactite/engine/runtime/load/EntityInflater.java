package org.codefilarete.stalactite.engine.runtime.load;

import java.util.Set;

import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.RowTransformer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Row;

/**
 * Contract to deserialize a database row to a bean
 */
public interface EntityInflater<E, I, T extends Table> {
	
	Class<E> getEntityType();
	
	I giveIdentifier(Row row, ColumnedRow columnedRow);
	
	RowTransformer<E> copyTransformerWithAliases(ColumnedRow columnedRow);
	
	Set<Column<T, Object>> getSelectableColumns();
	
	/**
	 * Adapter of a {@link EntityMapping} as a {@link EntityInflater}.
	 * Implemented as a simple wrapper of a {@link EntityMapping} because methods are very close.
	 *
	 * @param <E> entity type
	 * @param <I> identifier type
	 * @param <T> table type
	 */
	class EntityMappingAdapter<E, I, T extends Table> implements EntityInflater<E, I, T> {
		
		private final EntityMapping<E, I, T> delegate;
		
		public EntityMappingAdapter(EntityMapping<E, I, T> delegate) {
			this.delegate = delegate;
		}
		
		@Override
		public Class<E> getEntityType() {
			return this.delegate.getClassToPersist();
		}
		
		@Override
		public I giveIdentifier(Row row, ColumnedRow columnedRow) {
			return this.delegate.getIdMapping().getIdentifierAssembler().assemble(row, columnedRow);
		}
		
		@Override
		public RowTransformer<E> copyTransformerWithAliases(ColumnedRow columnedRow) {
			return this.delegate.copyTransformerWithAliases(columnedRow);
		}
		
		@Override
		public Set<Column<T, Object>> getSelectableColumns() {
			return this.delegate.getSelectableColumns();
		}
	}
	
}
