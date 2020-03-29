package org.gama.stalactite.sql.result;

import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.gama.stalactite.sql.binder.ResultSetReader;

import static org.gama.lang.Nullable.nullable;

/**
 * A class aimed at creating flat (no graph) beans from a {@link ResultSet} row.
 * Will read a main/root column that determines if a bean can be created from it (is null ?), then applies some more "bean filler" that are
 * defined with {@link ColumnConsumer}.
 * Instances of this class can be reused over multiple {@link ResultSet} (supposed to have same columns).
 * They can also be adapted to other {@link ResultSet}s that haven't the exact same column names by duplicating them with {@link #copyWithAliases(Function)}.
 * Moreover they can also be cloned to another type of bean which uses the same column names with {@link #copyFor(Class, Function)}.
 * 
 * @param <I> the type of the bean key (Input)
 * @param <C> the type of the bean
 *     
 * @author Guillaume Mary
 */
public class ResultSetRowTransformer<I, C> implements ResultSetTransformer<I, C>, ResultSetRowAssembler<C> {
	
	private final ColumnReader<I> reader;
	
	private final Function<I, C> beanFactory;
	
	private final Class<C> beanType;
	
	private final Set<ColumnConsumer<C, Object>> consumers = new HashSet<>();
	
	/**
	 * Constructor focused on simple cases where beans are built only from one column key.
	 * Prefer {@link #ResultSetRowTransformer(Class, ColumnReader, Function)} for more general purpose cases (multiple columns key)
	 * 
	 * @param columnName the name of the column that contains bean key
	 * @param reader object to ease column reading, indicates column type
	 * @param beanFactory the bean creator, bean key will be passed as argument. Not called if bean key is null (no instanciation needed)
	 */
	public ResultSetRowTransformer(Class<C> beanType, String columnName, ResultSetReader<I> reader, Function<I, C> beanFactory) {
		this.beanType = beanType;
		this.reader = new SingleColumnReader<>(columnName, reader);
		this.beanFactory = beanFactory;
	}
	
	/**
	 * Constructor with main and mandatory arguments
	 *
	 * @param beanType type of built instances
	 * @param reader object to ease column reading, indicates column type
	 * @param beanFactory the bean creator, bean key will be passed as argument. Not called if bean key is null (no instanciation needed)
	 */
	public ResultSetRowTransformer(Class<C> beanType, ColumnReader<I> reader, Function<I, C> beanFactory) {
		this.beanType = beanType;
		this.reader = reader;
		this.beanFactory = beanFactory;
	}
	
	public Class<C> getBeanType() {
		return beanType;
	}
	
	public ColumnReader<I> getReader() {
		return reader;
	}
	
	public Function<I, C> getBeanFactory() {
		return beanFactory;
	}
	
	/**
	 * Gives {@link ColumnConsumer}s of this instances
	 * 
	 * @return not null
	 */
	public Set<ColumnConsumer<C, Object>> getConsumers() {
		return consumers;
	}
	
	/**
	 * Defines a complementary column that will be mapped on a bean property.
	 * Null values will be passed to the consumer, hence the property mapper must be "null-value proof".
	 * 
	 * @param columnConsumer the object that will do reading and mapping
	 */
	@Override
	public void add(ColumnConsumer<C, ?> columnConsumer) {
		this.consumers.add((ColumnConsumer<C, Object>) columnConsumer);
	}
	
	@Override
	public <T extends C> ResultSetRowTransformer<I, T> copyFor(Class<T> beanType, Function<I, T> beanFactory) {
		ResultSetRowTransformer<I, T> result = new ResultSetRowTransformer<>(beanType, this.reader, beanFactory);
		result.consumers.addAll((Set) this.consumers);
		return result;
	}
	
	@Override
	public ResultSetRowTransformer<I, C> copyWithAliases(Function<String, String> columnMapping) {
		ResultSetRowTransformer<I, C> result = new ResultSetRowTransformer<>(this.beanType, reader.copyWithAliases(columnMapping), this.beanFactory);
		this.consumers.forEach(c -> result.add(c.copyWithAliases(columnMapping)));
		return result;
	}
	
	@Override	// for adhoc return type
	public ResultSetRowTransformer<I, C> copyWithAliases(Map<String, String> columnMapping) {
		// equivalent to super.copyWithAlias(..) but because inteface default method can't be invoked we have to copy/paste its code ;(
		return copyWithAliases(columnMapping::get);
	}
	
	/**
	 * Converts the current {@link ResultSet} row into a bean.
	 * Depending on implementation of factory, it may return a brand new instance or a cached one (if the bean key is already known for instance).
	 * Consumers will be applied to instance returned by the factory, as a consequence if bean comes from a cache it will be completed again, this may
	 * do some extra work in case of simple property.
	 *
	 * @param resultSet not null
	 * @return an instance of T, newly created or not according to implementation
	 */
	@Override
	public C transform(ResultSet resultSet) {
		return nullable(readBeanKey(resultSet))
				.map(this::giveRootInstance)
				.invoke(b -> assemble(b, resultSet))
				.get();
	}
	
	protected I readBeanKey(ResultSet resultSet) {
		return reader.read(resultSet);
	}
	
	protected C giveRootInstance(I beanKey) {
		return beanFactory.apply(beanKey);
	}
	
	/**
	 * Implementation that applies all {@link ColumnConsumer} to the given {@link ResultSet}
	 *
	 * @param rootBean the bean built for the row
	 * @param input any {@link ResultSet} positioned at row that must be read
	 */
	@Override
	public void assemble(C rootBean, ResultSet input) {
		for (ColumnConsumer<C, ?> consumer : consumers) {
			consumer.assemble(rootBean, input);
		}
	}
}
