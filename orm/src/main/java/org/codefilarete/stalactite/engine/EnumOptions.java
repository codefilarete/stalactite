package org.codefilarete.stalactite.engine;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Enum mapping options
 * 
 * @author Guillaume Mary
 */
public interface EnumOptions extends PropertyOptions {
	
	EnumOptions byName();
	
	EnumOptions byOrdinal();
	
	@Override
	EnumOptions mandatory();
	
	@Override
	EnumOptions setByConstructor();
	
	@Override
	EnumOptions readonly();
	
	@Override
	EnumOptions columnName(String name);
	
	@Override
	<O> EnumOptions column(Column<? extends Table, O> column);
	
	@Override
	EnumOptions fieldName(String name);
}
