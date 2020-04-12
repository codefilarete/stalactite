package org.gama.stalactite.persistence.engine;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

/**
 * @author Guillaume Mary
 */
public interface CollectionOptions<C> {
	
	<IN> CollectionOptions<C> overrideName(SerializableFunction<C, IN> getter, String columnName);
	
	<IN> CollectionOptions<C> overrideName(SerializableBiConsumer<C, IN> setter, String columnName);
//	
//	<IN> CollectionOptions<C> override(SerializableFunction<C, IN> function, Column<Table, IN> targetColumn);
//	
//	<IN> CollectionOptions<C> withTable(String name);
	
//	<IN> CollectionOptions<C> withReverseColumnName(String name);
//	
//	<IN> CollectionOptions<C> withTable(Table table);
//	
//	<I, T extends Table> CollectionOptions<C> withTable(T table, Column<T, I> reverseColumn);
//	
	
}
