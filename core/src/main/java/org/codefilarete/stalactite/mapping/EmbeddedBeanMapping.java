package org.codefilarete.stalactite.mapping;

import java.util.Set;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Contract for embedding a bean in another
 * 
 * @author Guillaume Mary
 */
public interface EmbeddedBeanMapping<C, T extends Table<T>> extends Mapping<C, T> {
	
	/**
	 * Gives the columns implied in the persistence. Used as a reference for CRUD operations.
	 * Result is not expected to change between calls and should be constant
	 *  
	 * @return a non-null set of columns to be written and read
	 */
	Set<Column<T, ?>> getColumns();
}
