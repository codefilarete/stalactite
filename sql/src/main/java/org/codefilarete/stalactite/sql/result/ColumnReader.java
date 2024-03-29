package org.codefilarete.stalactite.sql.result;

import java.sql.ResultSet;
import java.util.function.Function;

import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;

/**
 * Small contract for {@link ResultSet} reading.
 * May seem a duplicate of {@link ResultSetReader} but this class expect to be more tied to some columns, or autonomous
 * to compose a bean, because its main method {@link #read(ResultSet)} doesn't get any column name as argument : caller doesn't force it to read
 * any particular column.
 * 
 * @author Guillaume Mary
 */
public interface ColumnReader<C> {
	
	/**
	 * Reads current {@link ResultSet} row to generate a bean
	 * @param resultSet a {@link ResultSet} positioned at a row (started, not closed)
	 * @return a bean
	 */
	C read(ResultSet resultSet);
	
	ColumnReader<C> copyWithAliases(Function<String, String> columnMapping);
}
