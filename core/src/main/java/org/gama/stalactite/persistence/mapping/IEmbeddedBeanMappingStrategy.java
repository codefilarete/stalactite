package org.gama.stalactite.persistence.mapping;

import javax.annotation.Nonnull;
import java.util.Set;

import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Contract for embedding beans in another
 * 
 * @author Guillaume Mary
 */
public interface IEmbeddedBeanMappingStrategy<C, T extends Table> extends IMappingStrategy<C, T> {
	
	/**
	 * Gives the columns implied in the persistence. Used as a reference for CRUD operations.
	 * Result is not expected to change between calls and should be constant
	 * 
	 * @return a non null set of columns to be written and read
	 */
	@Nonnull
	Set<Column<T, Object>> getColumns();
}
