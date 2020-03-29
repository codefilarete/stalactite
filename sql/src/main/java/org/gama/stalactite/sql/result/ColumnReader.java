package org.gama.stalactite.sql.result;

import javax.annotation.Nonnull;
import java.sql.ResultSet;
import java.util.function.Function;

/**
 * Small contract for {@link ResultSet} reading.
 * May seem a duplicate of {@link org.gama.stalactite.sql.binder.ResultSetReader} but this class expect to be more tied to some columns, or autonomous
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
	C read(@Nonnull ResultSet resultSet);
	
	ColumnReader<C> copyWithAliases(Function<String, String> columnMapping);
}
