package org.codefilarete.stalactite.sql.result;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.codefilarete.stalactite.sql.statement.binder.ResultSetReader;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.danekja.java.util.function.serializable.SerializableSupplier;

/**
 * @param <C> produced bean type
 * @param <I> the type of bean keys (input)
 * @author Guillaume Mary
 */
public interface ResultSetTransformer<C, I> {
	
	/**
	 * Defines a complementary column that will be mapped on a bean property.
	 * Null values will be passed to the consumer, hence the property mapper must be "null-value proof".
	 *
	 * @param columnConsumer the object that will do the reading and mapping
	 */
	<O> ResultSetTransformer<C, I> add(ColumnConsumer<C, O> columnConsumer);
	
	/**
	 * Detailed version of {@link #add(ColumnConsumer)}
	 *
	 * @param columnName the column name of the property source
	 * @param reader the object that helps to read the column
	 * @param combiner the applyer of the value over a bean property
	 * @param <V> the type of the read value, must be compatible with the bean property input
	 */
	default <V> void add(String columnName, ResultSetReader<V> reader, BiConsumer<C, V> combiner) {
		add(new ColumnConsumer<>(columnName, reader, combiner));
	}
	
	/**
	 * Specialized version of add(..) for a collection-typed property : will instantiate and set the collection before adding the
	 * {@link ResultSet} value. Be aware that this method focuses on filling a simple-typed {@link Collection} (Integer, String, ...), not
	 * complex-typed ones because {@link ResultSetReader} is only capable of reading one column. 
	 * 
	 * @param columnName the column name of the property source
	 * @param reader the object that helps to read the column
	 * @param collectionAccessor the collection getter
	 * @param collectionMutator the collection setter (called only if getter returns null)
	 * @param collectionFactory the collection factory (called only if getter returns null)
	 * @param <V> the type of the read value, must be compatible with the bean property input
	 * @param <U> the collection type
	 */
	default <V, U extends Collection<V>> void add(String columnName,
												  ResultSetReader<V> reader,
												  Function<C, U> collectionAccessor,
												  BiConsumer<C, U> collectionMutator,
												  Supplier<U> collectionFactory) {
		add(new ColumnConsumer<>(columnName, reader, (c, v) -> {
			U collection = collectionAccessor.apply(c);
			if (collection == null) {
				collection = collectionFactory.get();
				collectionMutator.accept(c, collection);
			}
			collection.add(v);
		}));
	}
	
	/**
	 * Converts the current {@link ResultSet} row into a bean.
	 * Depending on implementation, this can return a brand new instance, or a cached one (if the bean key is already known for instance).
	 * 
	 * @param resultSet not null
	 * @return an instance of T, newly created or not according to implementation
	 * @throws SQLException due to {@link ResultSet} reading
	 */
	C transform(ResultSet resultSet) throws SQLException;

	/**
	 * Clones this instance for another type of bean.
	 * Useful to map a bean inheriting from another because it avoids redeclaration of common column mapping. Then after cloning current instance,
	 * one has only to register specific columns of the inheriting bean.
	 * 
	 * @param beanType the target bean type
	 * @param beanFactory the adhoc constructor for the target bean
	 * @param <T> the target bean type
	 * @return a new instance, kind of clone of this but for another type
	 */
	<T extends C> ResultSetTransformer<T, I> copyFor(Class<T> beanType, SerializableFunction<I, T> beanFactory);
	
	/**
	 * Clones this instance for another type of bean.
	 * Useful to map a bean inheriting from another because it avoids redeclaration of common column mapping. Then after cloning current instance,
	 * one has only to register specific columns of the inheriting bean.
	 *
	 * @param beanType the target bean type
	 * @param beanFactory the adhoc constructor for the target bean
	 * @param <T> the target bean type
	 * @return a new instance, kind of clone of this but for another type
	 */
	<T extends C> ResultSetTransformer<T, I> copyFor(Class<T> beanType, SerializableSupplier<T> beanFactory);
	
	/**
	 * Will combine bean created by this instance with the one created by relatedBeanCreator thanks to given combiner.
	 *
	 * @param combiner the associative function between bean created by this instance and the one created by given transformer
	 * @param relatedBeanCreator creator of another type of bean that will be combined with the one created by this instance
	 * @param <K> other bean type
	 * @param <V> other bean key type
	 * @return this
	 */
	<K, V> ResultSetTransformer<C, I> add(BeanRelationFixer<C, V> combiner, ResultSetRowTransformer<V, K> relatedBeanCreator);
}
