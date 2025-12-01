package org.codefilarete.stalactite.dsl.property;

import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface ElementCollectionOptions<C, O, S extends Collection<O>> {
	
	ElementCollectionOptions<C, O, S> withCollectionFactory(Supplier<? extends S> collectionFactory);
	
	ElementCollectionOptions<C, O, S> elementColumn(String columnName);
	
	/**
	 * Sets reverse column name (foreign key one)
	 */
	ElementCollectionOptions<C, O, S> reverseJoinColumn(String name);

	ElementCollectionOptions<C, O, S> onTable(Table table);
	
	ElementCollectionOptions<C, O, S> onTable(String tableName);
	
}
