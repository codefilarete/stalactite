package org.codefilarete.stalactite.engine;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.sql.ddl.structure.Column;

public interface JoinColumnNamingStrategy {
	
	/**
	 * Expected to generate the column name for given method definition which maps a property.
	 * Will be called several times with same {@link AccessorDefinition} but different target column in case of composed
	 * key on source entity : one call for each column composing the key
	 *
	 * @param accessorDefinition a representation of the method (getter or setter) that gives the property to be persisted
	 * @param targetColumn target column on reverse side : the right side primary key for one-to-one, reverse column for one-to-many relation owned by reverse side
	 */
	String giveName(AccessorDefinition accessorDefinition, Column<?, ?> targetColumn);
	
	String DEFAULT_JOIN_COLUMN_SUFFIX = "Id";
	
	/**
	 * Adds {@value #DEFAULT_JOIN_COLUMN_SUFFIX} to the {@link ColumnNamingStrategy#DEFAULT} naming strategy 
	 */
	JoinColumnNamingStrategy JOIN_DEFAULT = (accessor, targetColumn) -> ColumnNamingStrategy.DEFAULT.giveName(accessor) + DEFAULT_JOIN_COLUMN_SUFFIX;
	
}
