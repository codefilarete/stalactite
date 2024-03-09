package org.codefilarete.stalactite.engine;

import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.tool.function.Converter;

/**
 * Enum mapping options
 * 
 * @author Guillaume Mary
 */
public interface EnumOptions<E extends Enum<E>> extends PropertyOptions<E> {
	
	EnumOptions<E> byName();
	
	EnumOptions<E> byOrdinal();
	
	@Override
	EnumOptions<E> mandatory();
	
	@Override
	EnumOptions<E> setByConstructor();
	
	@Override
	EnumOptions<E> readonly();
	
	@Override
	EnumOptions<E> columnName(String name);
	
	@Override
	EnumOptions<E> column(Column<? extends Table, ? extends E> column);
	
	@Override
	EnumOptions<E> fieldName(String name);
	
	@Override
	EnumOptions<E> readConverter(Converter<E, E> converter);
	
	@Override
	EnumOptions<E> writeConverter(Converter<E, E> converter);
	
	@Override
	<V> PropertyOptions<E> sqlBinder(ParameterBinder<V> parameterBinder);
}
