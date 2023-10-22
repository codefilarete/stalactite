package org.codefilarete.stalactite.engine;

import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface ElementCollectionOptions<C, O, S extends Collection<O>> {
	
	ElementCollectionOptions<C, O, S> withCollectionFactory(Supplier<? extends S> collectionFactory);
	
	/**
	 * Sets reverse column name (foreign key one)
	 */
	ElementCollectionOptions<C, O, S> withReverseJoinColumn(String name);

	ElementCollectionOptions<C, O, S> withTable(Table table);
	
	ElementCollectionOptions<C, O, S> withTable(String tableName);
	
}
