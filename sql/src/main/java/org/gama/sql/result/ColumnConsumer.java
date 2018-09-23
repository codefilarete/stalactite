package org.gama.sql.result;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.gama.sql.binder.ResultSetReader;

/**
 * A simple wire between a (or more) column name of a {@link ResultSet} and a function (setter) of a bean through a {@link BiConsumer}.
 * For example: <br/>
 * <code>new ColumnConsumer("isActive", DefaultResultSetReaders.BOOLEAN_READER, MyClass::setActive)</code>
 * 
 * @param <C> assembled bean type
 * @param <I> bean identifier type
 * @author Guillaume Mary
 */
public class ColumnConsumer<C, I> implements ResultSetRowAssembler<C> {
	
	private final ColumnReader<I> reader;
	private final BiConsumer<C, I> consumer;
	
	/**
	 * General constructor which accept {@link SingleColumnReader} or {@link MultipleColumnsReader} 
	 * 
	 * @param reader any {@link SingleColumnReader} or {@link MultipleColumnsReader}
	 * @param consumer a consumer that can create a bean from its identifier
	 */
	public ColumnConsumer(ColumnReader<I> reader, BiConsumer<C, I> consumer) {
		this.reader = reader;
		this.consumer = consumer;
	}
	
	/**
	 * Detailed constructor for a {@link SingleColumnReader} case
	 * 
	 * @param columnName the column name to be read
	 * @param reader column type reader
	 * @param consumer a consumer that can create a bean from its identifier
	 */
	public ColumnConsumer(String columnName, ResultSetReader<I> reader, BiConsumer<C, I> consumer) {
		this(new SingleColumnReader<>(columnName, reader), consumer);
	}
	
	/**
	 * @return the given reader at construction time
	 */
	public ColumnReader<I> getReader() {
		return reader;
	}
	
	/**
	 * @return the given consumer at construction time
	 */
	public BiConsumer<C, I> getConsumer() {
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
	public void assemble(C instance, ResultSet resultSet) throws SQLException {
		consumer.accept(instance, reader.read(resultSet));
	}
	
	/**
	 * Makes a copy of this instance with column translation.
	 * Made to reuse this instance on another kind of {@link ResultSet} on which column differs by their column names.
	 *
	 * @param columnMapping a {@link Function} that gives a new column name for a asked one
	 * 						Can be implemented with a switch/case, a prefix/suffix concatenation, etc
	 * @return a new instance, kind of clone of this
	 */
	public ColumnConsumer<C, I> copyWithAliases(Function<String, String> columnMapping) {
		return new ColumnConsumer<>(reader.copyWithAliases(columnMapping), consumer);
	}
}
