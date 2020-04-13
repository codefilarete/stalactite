package org.gama.stalactite.persistence.engine;

import java.util.Collection;
import java.util.function.Supplier;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface ElementCollectionOptions<C, O, S extends Collection<O>> {
	
	<IN> ElementCollectionOptions<C, O, S> overrideName(SerializableFunction<C, IN> getter, String columnName);
	
	<IN> ElementCollectionOptions<C, O, S> overrideName(SerializableBiConsumer<C, IN> setter, String columnName);
	
	ElementCollectionOptions<C, O, S> withCollectionFactory(Supplier<? extends S> collectionFactory);
//	
//	<IN> ElementCollectionOptions<C> override(SerializableFunction<C, IN> function, Column<Table, IN> targetColumn);
//	
	ElementCollectionOptions<C, O, S> mappedBy(String name);

	ElementCollectionOptions<C, O, S> withTable(Table table);
	
	ElementCollectionOptions<C, O, S> withTable(String tableName);
	
}
