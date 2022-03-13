package org.codefilarete.stalactite.persistence.engine;

import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.stalactite.persistence.sql.ddl.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface ElementCollectionOptions<C, O, S extends Collection<O>> {
	
	ElementCollectionOptions<C, O, S> withCollectionFactory(Supplier<? extends S> collectionFactory);
	
	ElementCollectionOptions<C, O, S> mappedBy(String name);

	ElementCollectionOptions<C, O, S> withTable(Table table);
	
	ElementCollectionOptions<C, O, S> withTable(String tableName);
	
}
