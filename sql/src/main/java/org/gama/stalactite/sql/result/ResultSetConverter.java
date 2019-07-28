package org.gama.stalactite.sql.result;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.gama.stalactite.sql.binder.ResultSetReader;

/**
 * @param <I> the type of bean keys (input)
 * @param <C> the type of beans
 * @author Guillaume Mary
 */
public interface ResultSetConverter<I, C> {
	
	/**
	 * Defines a complementary column that will be mapped on a bean property.
	 * Null values will be passed to the consumer, hence the property mapper must be "null-value proof".
	 *
	 * @param columnConsumer the object that will do the reading and mapping
	 */
	void add(ColumnConsumer<C, ?> columnConsumer);
	
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
	default <V, U extends Collection<V>> void add(String columnName, ResultSetReader<V> reader,
												  Function<C, U> collectionAccessor, BiConsumer<C, U> collectionMutator, Supplier<U> collectionFactory) {
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
	 * Clones this for another type of bean.
	 * Usefull to map a bean inheriting from another because it avoids redeclaration of common column mapping. Then after cloning this, all that's
	 * left is to register specific columns of the inheritant bean.
	 * 
	 * @param beanType the target bean type
	 * @param beanFactory the adhoc constructor for the target bean
	 * @param <T> the target bean type
	 * @return a new instance, kind of clone of this but for another type
	 */
	<T extends C> ResultSetConverter<I, T> copyFor(Class<T> beanType, Function<I, T> beanFactory);
	
	/**
	 * Makes a copy of this instance with column translation.
	 * Made to reuse this instance on another kind of {@link ResultSet} on which column differs by their column names.
	 * 
	 * @param columnMapping a {@link Function} that gives a new column name for each one declared with {@link #add(String, ResultSetReader, BiConsumer)}.
	 * 						Can be implemented with a switch/case, a prefix/suffix concatenation, etc
	 * @return a new instance, kind of clone of this
	 */
	ResultSetConverter<I, C> copyWithAliases(Function<String, String> columnMapping);
	
	/**
	 * Same as {@link #copyWithAliases(Function)} but with a concrete mapping through a {@link Map}
	 * @param columnMapping the mapping between column names declared by {@link #add(String, ResultSetReader, BiConsumer)} and new ones
	 * @return a new instance, kind of clone of this
	 */
	default ResultSetConverter<I, C> copyWithAliases(Map<String, String> columnMapping) {
		return copyWithAliases(columnMapping::get);
	}
}
