package org.codefilarete.stalactite.dsl.property;

import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.tool.function.Converter;

/**
 * @author Guillaume Mary
 */
public interface ColumnOptions<O> extends PropertyOptions<O> {
	
	/**
	 * Marks the property as mandatory, which makes the mapped column not nullable: does not make a null checking at runtime.
	 * Note that using this method on an identifier one as no purpose because identifiers are already mandatory.
	 */
	@Override
	ColumnOptions<O> mandatory();
	
	@Override
	ColumnOptions<O> setByConstructor();
	
	@Override
	ColumnOptions<O> readonly();
	
	@Override
	ColumnOptions<O> columnName(String name);
	
	@Override
	ColumnOptions<O> columnSize(Size size);
	
	@Override
	ColumnOptions<O> column(Column<? extends Table, ? extends O> column);
	
	@Override
	ColumnOptions<O> fieldName(String name);
	
	@Override
	ColumnOptions<O> readConverter(Converter<O, O> converter);
	
	@Override
	ColumnOptions<O> writeConverter(Converter<O, O> converter);
	
	@Override
	<V> PropertyOptions<O> sqlBinder(ParameterBinder<V> parameterBinder);
	
}
