package org.gama.stalactite.sql.result;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.danekja.java.util.function.serializable.SerializableFunction;
import org.danekja.java.util.function.serializable.SerializableSupplier;
import org.gama.lang.exception.NotImplementedException;
import org.gama.stalactite.sql.binder.ResultSetReader;

/**
 * @param <I> the type of bean keys (input)
 * @param <C> produced bean type
 * @author Guillaume Mary
 */
public interface ResultSetTransformer<I, C> extends CopiableForAnotherQuery<C> {
	
	/**
	 * Defines a complementary column that will be mapped on a bean property.
	 * Null values will be passed to the consumer, hence the property mapper must be "null-value proof".
	 *
	 * @param columnConsumer the object that will do the reading and mapping
	 */
	<O> ResultSetTransformer<I, C> add(ColumnConsumer<C, O> columnConsumer);
	
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
	 * Specialized version of add(..) for a collection-typed property : will instanciate and set the collection before adding the
	 * {@link ResultSet} value.
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
	 * Usefull to map a bean inheriting from another because it avoids redeclaration of common column mapping. Then after cloning current instance,
	 * one has only to register specific columns of the inheritant bean.
	 * 
	 * @param beanType the target bean type
	 * @param beanFactory the adhoc constructor for the target bean
	 * @param <T> the target bean type
	 * @return a new instance, kind of clone of this but for another type
	 */
	<T extends C> ResultSetTransformer<I, T> copyFor(Class<T> beanType, SerializableFunction<I, T> beanFactory);
	
	/**
	 * Clones this instance for another type of bean.
	 * Usefull to map a bean inheriting from another because it avoids redeclaration of common column mapping. Then after cloning current instance,
	 * one has only to register specific columns of the inheritant bean.
	 *
	 * @param beanType the target bean type
	 * @param beanFactory the adhoc constructor for the target bean
	 * @param <T> the target bean type
	 * @return a new instance, kind of clone of this but for another type
	 */
	<T extends C> ResultSetTransformer<I, T> copyFor(Class<T> beanType, SerializableSupplier<T> beanFactory);
	
	/**
	 * Will combine bean created by this instance with the one created by relatedBeanCreator thanks to given combiner.
	 *
	 * @param combiner the assciative function between bean created by this instance and the one created by given transformer
	 * @param relatedBeanCreator creattor of another type of bean that will be combine with the one creted by this instance
	 * @param <K> other bean type
	 * @param <V> other bean key type
	 * @return this
	 */
	<K, V> ResultSetTransformer<I, C> add(BiConsumer<C, V> combiner, ResultSetRowTransformer<K, V> relatedBeanCreator);
	
	/**
	 * Overriden for return type cast.
	 */
	@Override
	default ResultSetTransformer<I, C> copyWithAliases(Function<String, String> columnMapping) {
		throw new NotImplementedException("This instance doesn't support copy, please implement it if you wish to reuse its mapping for another query");
	}
	
	/**
	 * Overriden for return type cast.
	 */
	default ResultSetTransformer<I, C> copyWithAliases(Map<String, String> columnMapping) {
		return copyWithAliases(columnMapping::get);
	}
}
