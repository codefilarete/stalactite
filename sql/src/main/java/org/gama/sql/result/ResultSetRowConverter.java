package org.gama.sql.result;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.gama.lang.bean.IConverter;
import org.gama.sql.binder.ResultSetReader;

/**
 * A class aimed at creating flat (no graph) beans from a {@link ResultSet} row.
 * Will read a main/root column that determines if a bean can be created from it (is null ?), then applies some more "bean filler" that are
 * defined with {@link ColumnConsumer}.
 * 
 * @param <I> the type of the bean key (Input)
 * @param <T> the type of the bean
 *     
 * @author Guillaume Mary
 */
public class ResultSetRowConverter<I, T> implements IConverter<ResultSet, T, SQLException> {
	
	private final String columnName;
	private final ResultSetReader<I> reader;
	private final Function<I, T> beanFactory;
	
	private final List<ColumnConsumer> consumers = new ArrayList<>();
	
	/**
	 * Constructor with main and mandatory arguments
	 * 
	 * @param columnName the name of the column that contains bean key
	 * @param reader object to ease column reading, indicates column type
	 * @param beanFactory the bean creator, bean key will be passed as argument. Not called if bean key is null (no instanciation needed)
	 */
	public ResultSetRowConverter(String columnName, ResultSetReader<I> reader, Function<I, T> beanFactory) {
		this.columnName = columnName;
		this.reader = reader;
		this.beanFactory = beanFactory;
	}
	
	/**
	 * Defines a complementary column that will be mapped on a bean property.
	 * Null values will be passed to the consumer, hence the property mapper must be "null-value proof".
	 * 
	 * @param columnConsumer the object that will do the read and mapping
	 */
	public void add(ColumnConsumer columnConsumer) {
		this.consumers.add(columnConsumer);
	}
	
	/**
	 * Detailed version of {@link #add(ColumnConsumer)}
	 * 
	 * @param columnName the column name of the property source
	 * @param reader the object that helps to read the column
	 * @param consumer the applyer of the value over a bean property
	 * @param <V> the type of the read value, must be compatible with the bean property input
	 */
	public <V> void add(String columnName, ResultSetReader<V> reader, BiConsumer<T, V> consumer) {
		add(new ColumnConsumer<>(columnName, reader, consumer));
	}
	
	/**
	 * Implemented to create and fill a bean from a {@link ResultSet}
	 * 
	 * @param input the {@link ResultSet} positioned at a row to be read
	 * @return the bean returned by the bean factory given at construction time. Can be null if bean key is.
	 * 
	 * @throws SQLException in case of {@link ResultSet} read problem
	 */
	@Override
	public T convert(ResultSet input) throws SQLException {
		I beanKey = reader.get(input, columnName);
		if (beanKey == null) {
			return null;
		} else {
			T rowInstance = beanFactory.apply(beanKey);
			for (ColumnConsumer<T, ?> consumer : consumers) {
				consumer.accept(rowInstance, input);
			}
			return rowInstance;
		}
	}
}
