package org.gama.sql.result;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.gama.lang.bean.Converter;
import org.gama.sql.binder.ResultSetReader;

/**
 * A class aimed at creating flat (no graph) beans from a {@link ResultSet} row.
 * Will read a main/root column that determines if a bean can be created from it (is null ?), then applies some more "bean filler" that are
 * defined with {@link ColumnConsumer}.
 * Instances of this class can be reused over multiple {@link ResultSet} (supposed to have same columns).
 * They can also be adapt to other {@link ResultSet}s that haven't the exact same column names by duplicating them with {@link #copyWithMapping(Function)}.
 * Moreover they can also be cloned to another type of bean which uses the same column names with {@link #copyFor(Class, Function)}
 * 
 * @param <I> the type of the bean key (Input)
 * @param <T> the type of the bean
 *     
 * @author Guillaume Mary
 */
public class ResultSetRowConverter<I, T> extends AbstractResultSetConverter<I, T> implements Converter<ResultSet, T, SQLException> {
	
	private final String columnName;
	
	private final ResultSetReader<I> reader;
	
	private final Function<I, T> beanFactory;
	
	private final Class<T> beanType;
	
	private final List<ColumnConsumer<T, ?>> consumers = new ArrayList<>();
	
	/**
	 * Constructor with main and mandatory arguments
	 * 
	 * @param columnName the name of the column that contains bean key
	 * @param reader object to ease column reading, indicates column type
	 * @param beanFactory the bean creator, bean key will be passed as argument. Not called if bean key is null (no instanciation needed)
	 */
	public ResultSetRowConverter(Class<T> beanType, String columnName, ResultSetReader<I> reader, Function<I, T> beanFactory) {
		this.beanType = beanType;
		this.columnName = columnName;
		this.reader = reader;
		this.beanFactory = beanFactory;
	}
	
	public Class<T> getBeanType() {
		return beanType;
	}
	
	public String getColumnName() {
		return columnName;
	}
	
	public ResultSetReader<I> getReader() {
		return reader;
	}
	
	public Function<I, T> getBeanFactory() {
		return beanFactory;
	}
	
	public List<ColumnConsumer<T, ?>> getConsumers() {
		return consumers;
	}
	
	/**
	 * Defines a complementary column that will be mapped on a bean property.
	 * Null values will be passed to the consumer, hence the property mapper must be "null-value proof".
	 * 
	 * @param columnConsumer the object that will do the reading and mapping
	 */
	@Override
	public void add(ColumnConsumer<T, ?> columnConsumer) {
		this.consumers.add(columnConsumer);
	}
	
	@Override
	public <C> ResultSetRowConverter<I, C> copyFor(Class<C> beanType, Function<I, C> beanFactory) {
		ResultSetRowConverter<I, C> result = new ResultSetRowConverter<>(beanType, this.columnName, this.reader, beanFactory);
		this.consumers.forEach(c -> result.add(new ColumnConsumer(c.getColumnName(), c.getReader(), c.getConsumer())));
		return result;
	}
	
	@Override
	public ResultSetRowConverter<I, T> copyWithMapping(Function<String, String> columMapping) {
		ResultSetRowConverter<I, T> result = new ResultSetRowConverter<>(this.beanType, columMapping.apply(this.columnName), this.reader, this.beanFactory);
		this.consumers.forEach(c -> result.add(new ColumnConsumer(columMapping.apply(c.getColumnName()), c.getReader(), c.getConsumer())));
		return result;
	}
	
	@Override	// for adhoc return type
	public ResultSetRowConverter<I, T> copyWithMapping(Map<String, String> columMapping) {
		return copyWithMapping(columMapping::get);
	}
	
	/**
	 * Implemented to create and fill a bean from a {@link ResultSet}
	 * 
	 * @param resultSet the {@link ResultSet} positioned at a row to be read
	 * @return the bean returned by the bean factory given at construction time. Can be null if bean key is.
	 * 
	 * @throws SQLException in case of {@link ResultSet} read problem
	 */
	@Override
	public T convert(ResultSet resultSet) throws SQLException {
		return transform(resultSet);
	}
	
	@Override
	public T transform(ResultSet resultSet) throws SQLException {
		I beanKey = reader.get(resultSet, columnName);
		if (beanKey == null) {
			return null;
		} else {
			T rowInstance = giveRootInstance(beanKey);
			applyConsumers(rowInstance, resultSet);
			return rowInstance;
		}
	}
	
	protected T giveRootInstance(I beanKey) {
		return beanFactory.apply(beanKey);
	}
	
	protected void applyConsumers(T rowInstance, ResultSet input) throws SQLException {
		for (ColumnConsumer<T, ?> consumer : consumers) {
			consumer.accept(rowInstance, input);
		}
	}
}
