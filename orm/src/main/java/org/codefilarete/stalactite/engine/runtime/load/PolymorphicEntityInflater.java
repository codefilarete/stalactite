package org.codefilarete.stalactite.engine.runtime.load;

import java.util.Set;

import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.RowTransformer;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Row;

/**
 * Contract to deserialize a database row to a polymorphic entity.
 * 
 * 
 * 
 * @param <E> parent entity type
 * @param <D> sub entity type
 * @param <I> identifier type
 * @param <T> table type
 * @author Guillaume Mary
 */
public class PolymorphicEntityInflater<E, D extends E, I, T extends Table<T>> implements EntityInflater<D, I> {
	
	private final ConfiguredRelationalPersister<E, I> mainPersister;
	
	private final ConfiguredRelationalPersister<D, I> subPersister;
	
	public PolymorphicEntityInflater(ConfiguredRelationalPersister<E, I> mainPersister,
									 ConfiguredRelationalPersister<D, I> subPersister) {
		this.mainPersister = mainPersister;
		this.subPersister = subPersister;
	}
	
	@Override
	public Class<D> getEntityType() {
		return subPersister.getClassToPersist();
	}
	
	@Override
	public I giveIdentifier(Row row, ColumnedRow columnedRow) {
		return mainPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(row, columnedRow);
	}
	
	@Override
	public RowTransformer<D> copyTransformerWithAliases(ColumnedRow columnedRow) {
		return subPersister.getMapping().copyTransformerWithAliases(columnedRow);
	}
	
	@Override
	public Set<Selectable<?>> getSelectableColumns() {
		return (Set) subPersister.getMapping().getSelectableColumns();
	}
}
