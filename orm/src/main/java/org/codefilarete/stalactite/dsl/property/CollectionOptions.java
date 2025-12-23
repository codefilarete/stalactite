package org.codefilarete.stalactite.dsl.property;

import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface CollectionOptions<C, O, S extends Collection<O>> {
	
	CollectionOptions<C, O, S> initializeWith(Supplier<? extends S> collectionFactory);
	
	/**
	 * Sets reverse column name (foreign key one)
	 */
	CollectionOptions<C, O, S> reverseJoinColumn(String name);
	
	CollectionOptions<C, O, S> onTable(Table table);
	
	CollectionOptions<C, O, S> onTable(String tableName);
	
}