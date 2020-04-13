package org.gama.stalactite.persistence.engine;

import java.util.Collection;
import java.util.function.Supplier;

import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface ElementCollectionOptions<C, O, S extends Collection<O>> {
	
	ElementCollectionOptions<C, O, S> withCollectionFactory(Supplier<? extends S> collectionFactory);
	
	ElementCollectionOptions<C, O, S> mappedBy(String name);

	ElementCollectionOptions<C, O, S> withTable(Table table);
	
	ElementCollectionOptions<C, O, S> withTable(String tableName);
	
}
