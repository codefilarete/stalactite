package org.codefilarete.stalactite.dsl.naming;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.tool.Strings;

public interface JoinColumnNamingStrategy {
	
	/**
	 * Expected to generate the column name for given method definition which maps a property.
	 * Will be called several times with same {@link AccessorDefinition} but different target column in case of composed
	 * key on source entity : one call for each column composing the key
	 *
	 * @param propertyAccessor a representation of the method (getter or setter) that gives the property to be persisted
	 * @param targetColumn target column on reverse side : the right side primary key for one-to-one, reverse column for one-to-many relation owned by reverse side
	 */
	default String giveName(ValueAccessPoint<?>  propertyAccessor, Column<?, ?> targetColumn) {
		return giveName(AccessorDefinition.giveDefinition(propertyAccessor), targetColumn);
	}
	
	String giveName(AccessorDefinition accessorDefinition, Column<?, ?> targetColumn);
	
	/**
	 * Adds given column {@link Column#getName} capitalized to the {@link ColumnNamingStrategy#DEFAULT} naming strategy 
	 */
	JoinColumnNamingStrategy JOIN_DEFAULT = (accessor, targetColumn) -> ColumnNamingStrategy.DEFAULT.giveName(accessor) + Strings.capitalize(targetColumn.getName());
	
	JoinColumnNamingStrategy HIBERNATE_DEFAULT = (accessor, targetColumn) -> ColumnNamingStrategy.DEFAULT.giveName(accessor) + '_' + targetColumn.getName();
	
}
