package org.gama.stalactite.sql.result;

import java.sql.ResultSet;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.gama.stalactite.sql.binder.ResultSetReader;

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
	private final BiConsumer<C, I> propertySetter;
	
	/**
	 * General constructor which accept {@link SingleColumnReader} or {@link MultipleColumnsReader} 
	 * 
	 * @param reader any {@link SingleColumnReader} or {@link MultipleColumnsReader}
	 * @param propertySetter a consumer that is expected to set a property of a bean with value given by reader
	 */
	public ColumnConsumer(ColumnReader<I> reader, BiConsumer<C, I> propertySetter) {
		this.reader = reader;
		this.propertySetter = propertySetter;
	}
	
	/**
	 * Detailed constructor for a {@link SingleColumnReader} case
	 * 
	 * @param columnName the column name to be read
	 * @param reader column type reader
	 * @param propertySetter a consumer that is expected to set a property of a bean with value given by reader
	 */
	public ColumnConsumer(String columnName, ResultSetReader<I> reader, BiConsumer<C, I> propertySetter) {
		this(new SingleColumnReader<>(columnName, reader), propertySetter);
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
	public BiConsumer<C, I> getPropertySetter() {
		return propertySetter;
	}
	
	/**
	 * Applies the column value from the {@link ResultSet} onto the instance through the given consumer (at construction time)
	 * 
	 * @param instance a bean, not null
	 * @param resultSet the source of data
	 */
	@Override
	public void assemble(C instance, ResultSet resultSet) {
		propertySetter.accept(instance, reader.read(resultSet));
	}
	
	@Override
	public ColumnConsumer<C, I> copyWithAliases(Function<String, String> columnMapping) {
		return new ColumnConsumer<>(reader.copyWithAliases(columnMapping), propertySetter);
	}
}
