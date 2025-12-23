package org.codefilarete.stalactite.dsl.property;

import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface ElementCollectionOptions<C, O, S extends Collection<O>> extends CollectionOptions<C, O, S> {
	
	@Override
	ElementCollectionOptions<C, O, S> initializeWith(Supplier<? extends S> collectionFactory);
	
	ElementCollectionOptions<C, O, S> elementColumnName(String columnName);
	
	ElementCollectionOptions<C, O, S> elementColumnSize(Size columnSize);
	
	/**
	 * Sets reverse column name (foreign key one)
	 */
	@Override
	ElementCollectionOptions<C, O, S> reverseJoinColumn(String name);
	
	@Override
	ElementCollectionOptions<C, O, S> onTable(Table table);
	
	@Override
	ElementCollectionOptions<C, O, S> onTable(String tableName);
	
}
