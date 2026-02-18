package org.codefilarete.stalactite.dsl.property;

import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface CollectionOptions<C, O, S extends Collection<O>> {
	
	/**
	 * Provides a factory to be used to create the collection when needed. The resulting collection instance will be
	 * assigned to the property.
	 *
	 * @param collectionFactory a {@code Supplier} that provides a new instance of the collection
	 * @return the current instance for method chaining
	 */
	CollectionOptions<C, O, S> initializeWith(Supplier<? extends S> collectionFactory);
	
	/**
	 * Sets the reverse column name (the foreign key one)
	 * 
	 * @param name the column name
	 * @return the current instance for method chaining   
	 */
	CollectionOptions<C, O, S> reverseJoinColumn(String name);
	
	/**
	 * Asks to persist the collection elements index.
	 * By default, the column name that keeps track of it is named {@value org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy#DEFAULT_INDEX_COLUMN_NAME}.
	 * 
	 * @see #indexedBy(String)
	 * @return the current instance for method chaining
	 */
	CollectionOptions<C, O, S> indexed();
	
	/**
	 * Asks to persist the collection elements index by specifying the column name.
	 *
	 * @see #indexed()
	 * @return the current instance for method chaining
	 */
	CollectionOptions<C, O, S> indexedBy(String columnName);
	
	/**
	 * Specifies the table on which the collection should be persisted.
	 * 
	 * @param table the {@code Table} instance representing the database table to persist the collection
	 * @return the current instance for method chaining
	 */
	CollectionOptions<C, O, S> onTable(Table table);
	
	/**
	 * Specifies the table name on which the collection should be persisted.
	 * 
	 * @param tableName the table name representing the database table to persist the collection
	 * @return the current instance for method chaining
	 */
	CollectionOptions<C, O, S> onTable(String tableName);
}
