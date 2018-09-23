package org.gama.stalactite.persistence.mapping;

import javax.annotation.Nonnull;
import java.util.Set;

import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Mapper for embedded beans in another. For instance a simple @OneToOne can be considered as an embedded bean.
 * 
 * @author Guillaume Mary
 */
public interface IEmbeddedBeanMapping<C, T extends Table> extends IMappingStrategy<C, T> {
	
	/**
	 * Gives the columns implied in the persistence. Used as a reference for CRUD operations.
	 * Result is not expected to change between calls and should be constant (unless you have special use case).
	 * 
	 * @return a non null set of columns to be written and read
	 */
	@Nonnull
	Set<Column<T, Object>> getColumns();
}
