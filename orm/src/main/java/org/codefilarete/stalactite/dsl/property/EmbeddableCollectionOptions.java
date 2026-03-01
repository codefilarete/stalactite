package org.codefilarete.stalactite.dsl.property;

import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.reflection.SerializableAccessor;
import org.codefilarete.reflection.SerializableMutator;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Interface to manage the configuration of beans as value in {@link Collection} of elements.
 * @author Guillaume Mary
 */
public interface EmbeddableCollectionOptions<C, O, S extends Collection<O>> extends CollectionOptions<C, O, S> {
	
	<IN> EmbeddableCollectionOptions<C, O, S> overrideName(SerializableAccessor<O, IN> getter, String columnName);
	
	<IN> EmbeddableCollectionOptions<C, O, S> overrideName(SerializableMutator<O, IN> setter, String columnName);
	
	<IN> EmbeddableCollectionOptions<C, O, S> overrideSize(SerializableAccessor<O, IN> getter, Size columnSize);
	
	<IN> EmbeddableCollectionOptions<C, O, S> overrideSize(SerializableMutator<O, IN> setter, Size columnSize);
	
	@Override
	EmbeddableCollectionOptions<C, O, S> initializeWith(Supplier<? extends S> collectionFactory);
	
	/**
	 * Sets reverse column name (foreign key one)
	 */
	@Override
	EmbeddableCollectionOptions<C, O, S> reverseJoinColumn(String name);
	
	@Override
	EmbeddableCollectionOptions<C, O, S> indexed();
	
	@Override
	EmbeddableCollectionOptions<C, O, S> indexedBy(String columnName);
	
	@Override
	EmbeddableCollectionOptions<C, O, S> onTable(Table table);
	
	@Override
	EmbeddableCollectionOptions<C, O, S> onTable(String tableName);
	
}
