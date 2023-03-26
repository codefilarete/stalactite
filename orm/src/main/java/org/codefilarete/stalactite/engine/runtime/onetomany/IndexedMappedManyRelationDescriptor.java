package org.codefilarete.stalactite.engine.runtime.onetomany;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Container to store information of a one-to-many indexed mapped relation (by a column on the reverse side)
 * 
 * @author Guillaume Mary
 */
public class IndexedMappedManyRelationDescriptor<SRC, TRGT, C extends Collection<TRGT>, SRCID> extends MappedManyRelationDescriptor<SRC, TRGT, C, SRCID> {
	
	/** Column that stores index value, owned by reverse side table (table of targetPersister) */
	private final Column<Table, Integer> indexingColumn;
	
	/**
	 * 
	 * @param collectionGetter collection accessor
	 * @param collectionSetter collection setter
	 * @param collectionFactory collection factory
	 * @param reverseSetter setter on the owning side for source bean, optional
	 */
	public IndexedMappedManyRelationDescriptor(Function<SRC, C> collectionGetter,
											   BiConsumer<SRC, C> collectionSetter,
											   Supplier<C> collectionFactory,
											   @Nullable BiConsumer<TRGT, SRC> reverseSetter,
											   Key<?, SRCID> reverseColumn,
											   Column<? extends Table, Integer> indexingColumn) {
		super(collectionGetter, collectionSetter, collectionFactory, reverseSetter, reverseColumn);
		this.indexingColumn = (Column<Table, Integer>) indexingColumn;
	}
	
	public Column<Table, Integer> getIndexingColumn() {
		return indexingColumn;
	}
}
