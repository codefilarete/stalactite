package org.gama.sql.result;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.gama.lang.Reflections;
import org.gama.lang.bean.IConverter;
import org.gama.lang.function.ThrowingBiConsumer;
import org.gama.sql.binder.ResultSetReader;

/**
 * A class aimed at transforming a whole {@link ResultSet} into a graph of objects.
 * Graph creation is declared through {@link #add(String, ResultSetReader, Class, Function, BiConsumer)}
 * 
 * @author Guillaume Mary
 * @see #add(String, ResultSetReader, BiConsumer)  
 * @see #add(String, ResultSetReader, Class, Function, BiConsumer) 
 */
public class ResultSetTransformer<T> implements IConverter<ResultSet, List<T>, SQLException> {
	
	private final ResultSetRowConverter<?, T> rootConverter;
	
	private SimpleBeanCache beanCache = new SimpleBeanCache();
	
	private T currentRowBean;
	
	private final List<Relation> hierachicalConsumers = new ArrayList<>();
	
	public <I> ResultSetTransformer(Class<T> rootType, String columnName, ResultSetReader<I> reader, Function<I, T> factory) {
		this.rootConverter = new ResultSetRowConverter<>(columnName, reader,
				// we don't call the factory if the column value is null (meaning that bean key is null)
				colValue -> colValue == null ? null : beanCache.computeIfAbsent(rootType, colValue, () -> factory.apply(colValue)));
	}
	
	/**
	 * Adds a bean property to be filled with a {@link ResultSet} column.
	 * Example:<br/>
	 * <code>add("property_alias", STRING_READER, MyBean::setProperty);</code>
	 * 
	 * @param columnName the name of the column whom value will be given to the consumer
	 * @param reader the object that will extract the value from the column
	 * @param consumer some code that will consume column value, can be expressed as method reference such as MyBean::setProperty
	 * @param <I>
	 */
	public <I> void add(String columnName, ResultSetReader<I> reader, BiConsumer<T, I> consumer) {
		rootConverter.add(columnName, reader, (bean, colValue) -> consumer.accept(bean, colValue));
	}
	
	/**
	 * Adds a bean relation to the main/root object. 
	 * 
	 * @param columnName the column name to read the bean key
	 * @param reader a reader for reading the bean key
	 * @param beanType the type of the new bean
	 * @param factory a factory for creating the new bean, not called if bean key is null
	 * @param relationFixer a callback that will combine the newly created bean and the one of this transformer, called even with a null bean (v)
	 * @param <I> ResultSet column type
	 * @param <V> new bean type
	 * 
	 * @return the newly created {@link ResultSetTransformer} that will manage the new bean type creation, NOT THIS
	 */
	public <I, V> ResultSetTransformer<V> add(String columnName, ResultSetReader<I> reader, Class<V> beanType, Function<I, V> factory, BiConsumer<T, V> relationFixer) {
		ResultSetTransformer<V> relatedBeanCreator = new ResultSetTransformer<>(beanType, columnName, reader, factory::apply);
		return add(relatedBeanCreator, relationFixer);
		
	}
	
	/**
	 * A simplified version of {@link #add(String, ResultSetReader, Class, Function, BiConsumer)} where the factory is the default constructor
	 * of the given type.
	 * Be aware that the factory doesn't take the column (bean key) value into account, so it must be filled through
	 * {@link #add(String, ResultSetReader, BiConsumer)}
	 * 
	 * @param columnName the column name to read the bean key
	 * @param reader a reader for reading the bean key
	 * @param beanType the type of the new bean
	 * @param relationFixer a callback that will combine the newly created bean and the one of this transformer, called even with a null bean (v)
	 * @param <I> ResultSet column type
	 * @param <V> new bean type
	 * 
	 * @return the newly created {@link ResultSetTransformer} that will manage the new bean type creation, NOT THIS
	 */
	public <I, V> ResultSetTransformer<V> add(String columnName, ResultSetReader<I> reader, Class<V> beanType, BiConsumer<T, V> relationFixer) {
		return add(columnName, reader, beanType, v -> Reflections.newInstance(beanType), relationFixer);
	}
	
	/**
	 * Combines this transformer with another one through a bean relation, hence a bean graph is created.
	 * BE AWARE that the returned instance can't be shared with another {@link ResultSetTransformer} because its bean cache is shared with this one.
	 * (enhancement to be done to support {@link ResultSetTransformer} reuse)
	 * 
	 * @param relatedBeanCreator the manager of the other beans
	 * @param relationFixer the wire between instances of this transformer and those of the given one
	 * @param <V> the type of the other beans
	 * @return a new (not shareable) {@link ResultSetTransformer}
	 */
	public <V> ResultSetTransformer<V> add(ResultSetTransformer<V> relatedBeanCreator, BiConsumer<T, V> relationFixer) {
		// we must share de bean cache else the newly created transformer won't find it's bean and vice-versa
		relatedBeanCreator.beanCache = beanCache;
		hierachicalConsumers.add(new Relation<>(relationFixer, relatedBeanCreator));
		return relatedBeanCreator;
	}
	
	@Override
	public List<T> convert(ResultSet resultSet) throws SQLException {
		// We convert the ResultSet with an iteration over a ResultSetIterator that uses the transform(ResultSet) method
		ResultSetIterator<T> resultSetIterator = new ResultSetIterator<T>(resultSet) {
			@Override
			public T convert(ResultSet resultSet) throws SQLException {
				return ResultSetTransformer.this.transform(resultSet);
			}
		};
		List<T> result = new ArrayList<>();
		while (resultSetIterator.hasNext()) {
			result.add(resultSetIterator.next());
		}
		// memory cleanup
		beanCache.clear();
		return result;
	}
	
	public T transform(ResultSet resultSet) throws SQLException {
		// Can it be possible to have a null root bean ? if such we should add a if-null prevention. But what's the case ?
		currentRowBean = rootConverter.convert(resultSet);
		for (Relation<T, ?> hierachicalConsumer : this.hierachicalConsumers) {
			hierachicalConsumer.accept(currentRowBean, resultSet);
		}
		return currentRowBean;
	}
	
	/**
	 * A relation between a property mutator (setter) and the provider of the bean to be given as the setter argument
	 * 
	 * @param <K>
	 * @param <V>
	 */
	public static class Relation<K, V> implements ThrowingBiConsumer<K, ResultSet, SQLException> {
		
		private final BiConsumer<K, V> relationFixer;
		
		private final ResultSetTransformer<V> transformer;
		
		public Relation(BiConsumer<K, V> relationFixer, ResultSetTransformer<V> transformer) {
			this.relationFixer = relationFixer;
			this.transformer = transformer;
		}
		
		@Override
		public void accept(K bean, ResultSet resultSet) throws SQLException {
			// getting the bean
			V value = transformer.transform(resultSet);
			// applying it to the setter
			relationFixer.accept(bean, value);
		}
	}
}
