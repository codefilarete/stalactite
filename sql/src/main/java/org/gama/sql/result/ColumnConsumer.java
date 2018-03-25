package org.gama.sql.result;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.BiConsumer;

import org.gama.lang.function.ThrowingBiConsumer;
import org.gama.sql.binder.ResultSetReader;

/**
 * A simple wire between a column name of a {@link ResultSet} and a function (setter) of a bean through a {@link BiConsumer}.
 * For example: <br/>
 * <code>new ColumnConsumer("isActive", DefaultResultSetReaders.BOOLEAN_READER, MyClass::setActive)</code>
 * 
 * @author Guillaume Mary
 */
public class ColumnConsumer<T, I> implements ThrowingBiConsumer<T, ResultSet, SQLException> {
	
	private final String columnName;
	private final ResultSetReader<I> reader;
	private final BiConsumer<T, I> consumer;
	
	public ColumnConsumer(String columnName, ResultSetReader<I> reader, BiConsumer<T, I> consumer) {
		this.columnName = columnName;
		this.reader = reader;
		this.consumer = consumer;
	}
	
	public String getColumnName() {
		return columnName;
	}
	
	public ResultSetReader<I> getReader() {
		return reader;
	}
	
	public BiConsumer<T, I> getConsumer() {
		return consumer;
	}
	
	/**
	 * Applies the column value from the {@link ResultSet} onto the instance through the given consumer (at construction time)
	 * 
	 * @param instance a bean, not null
	 * @param resultSet the source of data
	 * @throws SQLException in case of error during {@link ResultSet} read
	 */
	@Override
	public void accept(T instance, ResultSet resultSet) throws SQLException {
		consumer.accept(instance, reader.get(resultSet, columnName));
	}
}
