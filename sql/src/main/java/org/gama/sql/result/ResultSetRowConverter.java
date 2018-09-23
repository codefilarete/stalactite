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
 * They can also be adapt to other {@link ResultSet}s that haven't the exact same column names by duplicating them with {@link #copyWithAliases(Function)}.
 * Moreover they can also be cloned to another type of bean which uses the same column names with {@link #copyFor(Class, Function)}
 * 
 * @param <I> the type of the bean key (Input)
 * @param <C> the type of the bean
 *     
 * @author Guillaume Mary
 */
public class ResultSetRowConverter<I, C>
		implements ResultSetConverter<I, C>, Converter<ResultSet, C, SQLException>, ResultSetRowAssembler<C> {
	
	private final String columnName;
	
	private final ResultSetReader<I> reader;
	
	private final Function<I, C> beanFactory;
	
	private final Class<C> beanType;
	
	private final List<ColumnConsumer<C, Object>> consumers = new ArrayList<>();
	
	/**
	 * Constructor with main and mandatory arguments
	 * 
	 * @param columnName the name of the column that contains bean key
	 * @param reader object to ease column reading, indicates column type
	 * @param beanFactory the bean creator, bean key will be passed as argument. Not called if bean key is null (no instanciation needed)
	 */
	public ResultSetRowConverter(Class<C> beanType, String columnName, ResultSetReader<I> reader, Function<I, C> beanFactory) {
		this.beanType = beanType;
		this.columnName = columnName;
		this.reader = reader;
		this.beanFactory = beanFactory;
	}
	
	public Class<C> getBeanType() {
		return beanType;
	}
	
	public String getColumnName() {
		return columnName;
	}
	
	public ResultSetReader<I> getReader() {
		return reader;
	}
	
	public Function<I, C> getBeanFactory() {
		return beanFactory;
	}
	
	public List<ColumnConsumer<C, Object>> getConsumers() {
		return consumers;
	}
	
	/**
	 * Defines a complementary column that will be mapped on a bean property.
	 * Null values will be passed to the consumer, hence the property mapper must be "null-value proof".
	 * 
	 * @param columnConsumer the object that will do the reading and mapping
	 */
	@Override
	public void add(ColumnConsumer<C, ?> columnConsumer) {
		this.consumers.add((ColumnConsumer<C, Object>) columnConsumer);
	}
	
	@Override
	public <T extends C> ResultSetRowConverter<I, T> copyFor(Class<T> beanType, Function<I, T> beanFactory) {
		ResultSetRowConverter<I, T> result = new ResultSetRowConverter<>(beanType, this.columnName, this.reader, beanFactory);
		this.consumers.forEach(c -> result.add(new ColumnConsumer(c.getColumnName(), c.getReader(), c.getConsumer())));
		return result;
	}
	
	@Override
	public ResultSetRowConverter<I, C> copyWithAliases(Function<String, String> columnMapping) {
		ResultSetRowConverter<I, C> result = new ResultSetRowConverter<>(this.beanType, columnMapping.apply(this.columnName), this.reader, this.beanFactory);
		this.consumers.forEach(c -> result.add(new ColumnConsumer(columnMapping.apply(c.getColumnName()), c.getReader(), c.getConsumer())));
		return result;
	}
	
	@Override	// for adhoc return type
	public ResultSetRowConverter<I, C> copyWithAliases(Map<String, String> columnMapping) {
		return copyWithAliases(columnMapping::get);
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
	public C convert(ResultSet resultSet) throws SQLException {
		return transform(resultSet);
	}
	
	@Override
	public C transform(ResultSet resultSet) throws SQLException {
		I beanKey = reader.get(resultSet, columnName);
		if (beanKey == null) {
			return null;
		} else {
			C rowInstance = giveRootInstance(beanKey);
			assemble(rowInstance, resultSet);
			return rowInstance;
		}
	}
	
	protected C giveRootInstance(I beanKey) {
		return beanFactory.apply(beanKey);
	}
	
	/**
	 * Implementation that applies all {@link ColumnConsumer} to the given {@link ResultSet} 
	 * 
	 * @param rootBean the bean built for the row
	 * @param input
	 * @throws SQLException
	 */
	@Override
	public void assemble(C rootBean, ResultSet input) throws SQLException {
		for (ColumnConsumer<C, ?> consumer : consumers) {
			consumer.assemble(rootBean, input);
		}
	}
}
