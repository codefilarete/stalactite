package org.codefilarete.stalactite.engine;

import java.sql.ResultSet;
import java.util.function.BiConsumer;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.codefilarete.tool.function.Converter;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.sql.result.ResultSetRowAssembler;
import org.codefilarete.stalactite.sql.result.ResultSetRowTransformer;
import org.codefilarete.stalactite.sql.result.WholeResultSetTransformer.AssemblyPolicy;

/**
 * Methods that define bean property mapping when creating an SQL query through {@link PersistenceContext#newQuery(Query, Class)}.
 * 
 * @author Guillaume Mary
 */
public interface BeanPropertyQueryMapper<C> {
	
	/**
	 * Maps a column to a bean property
	 *
	 * @param columnName column name that will fill the property
	 * @param setter property setter
	 * @param columnType column and value type
	 * @param <I> column and value type
	 * @return an instance that allows method chaining
	 * @see #map(String, SerializableBiConsumer, Converter)
	 */
	<I> BeanPropertyQueryMapper<C> map(String columnName, BiConsumer<C, I> setter, Class<I> columnType);
	
	/**
	 * Equivalent of {@link #map(String, BiConsumer, Class)} with an additional converter.
	 * Maps a column to a bean property by converting its value before setting it.
	 *
	 * @param columnName column name that will fill the property
	 * @param setter property setter
	 * @param columnType column and value type
	 * @param converter value converter
	 * @param <I> column and value type
	 * @return an instance that allows method chaining
	 * @see #map(String, SerializableBiConsumer, Converter)
	 */
	<I, J> BeanPropertyQueryMapper<C> map(String columnName, SerializableBiConsumer<C, J> setter, Class<I> columnType, Converter<I, J> converter);
	
	/**
	 * Equivalent of {@link #map(String, BiConsumer, Class)} without ensuring column type argument : it will be deduced from setter.
	 * Prefer {@link #map(String, BiConsumer, Class)} to ensure value reading from {@link ResultSet}
	 *
	 * @param columnName column name that will fill the property
	 * @param setter property setter
	 * @param <I> column and value type
	 * @return an instance that allows method chaining
	 */
	<I> BeanPropertyQueryMapper<C> map(String columnName, SerializableBiConsumer<C, I> setter);
	
	/**
	 * Equivalent of {@link #map(String, SerializableBiConsumer, Class, Converter)} without ensuring column type argument : it will be deduced from setter.
	 * Prefer {@link #map(String, SerializableBiConsumer, Class, Converter)} to ensure value reading from {@link ResultSet}
	 *
	 * @param columnName column name that will fill the property
	 * @param setter property setter
	 * @param converter value converter
	 * @param <I> column and value type
	 * @return an instance that allows method chaining
	 */
	<I, J> BeanPropertyQueryMapper<C> map(String columnName, SerializableBiConsumer<C, J> setter, Converter<I, J> converter);
	
	/**
	 * Equivalent of {@link #map(String, SerializableBiConsumer)} with column argument.
	 *
	 * @param column column name that will fill the property
	 * @param setter property setter
	 * @param <I> column and value type
	 * @return an instance that allows method chaining
	 */
	<I> BeanPropertyQueryMapper<C> map(Column<? extends Table, I> column, BiConsumer<C, I> setter);
	
	/**
	 * Equivalent of {@link #map(String, SerializableBiConsumer, Converter)} with column argument.
	 *
	 * @param column column name that will fill the property
	 * @param setter property setter
	 * @param <I> column and value type
	 * @return an instance that allows method chaining
	 */
	<I, J> BeanPropertyQueryMapper<C> map(Column<? extends Table, I> column, BiConsumer<C, J> setter, Converter<I, J> converter);
	
	/**
	 * Associates beans created by this instance and the ones created by another converter with setter (represented as a {@link BiConsumer}).
	 * This allows to create bean graphs.
	 *
	 * @param combiner setter (on beans created by this instance) to fix beans created by given converter
	 * @param relatedBeanCreator creator of other beans from a {@link ResultSet}
	 * @param <V> type of beans created by given converter
	 * @return this
	 */
	<K, V> BeanPropertyQueryMapper<C> map(BeanRelationFixer<C, V> combiner, ResultSetRowTransformer<V, K> relatedBeanCreator);
	
	/**
	 * Adds a low level {@link ResultSet} transformer, for cases where mapping methods are insufficient.
	 * Assembly will occurs on each row ({@link ResultSetRowAssembler#assemble(Object, ResultSet)} will be call for each {@link ResultSet} row)
	 *
	 * @param assembler a low-level {@link ResultSet} transformer
	 * @return this
	 */
	default BeanPropertyQueryMapper<C> map(ResultSetRowAssembler<C> assembler) {
		return map(assembler, AssemblyPolicy.ON_EACH_ROW);
	}
	
	/**
	 * Adds a low level {@link ResultSet} transformer, for cases where mapping methods are insufficient.
	 * Be aware that any bean created by given assembler won't participate in cache, if this is required then one should implement
	 * its own cache.
	 *
	 * @param assembler a generic combiner of a root bean and each {@link ResultSet} row
	 * @param assemblyPolicy policy to decide if given assemble shall be invoked on each row or not
	 * @return this
	 */
	BeanPropertyQueryMapper<C> map(ResultSetRowAssembler<C> assembler, AssemblyPolicy assemblyPolicy);
	
	/**
	 * Sets a value for the given parameter
	 *
	 * @param paramName name of the parameter in the sql
	 * @param value value for the parameter
	 * @return this
	 */
	BeanPropertyQueryMapper<C> set(String paramName, Object value);
	
	/**
	 * Sets a value for the given parameter giving explicit binder type
	 *
	 * @param paramName name of the parameter in the sql
	 * @param value value for the parameter
	 * @param valueType type for value {@link java.sql.PreparedStatement} binder
	 * @return this
	 */
	<O> BeanPropertyQueryMapper<C> set(String paramName, O value, Class<? super O> valueType);
	
	/**
	 * Sets a value for the given parameter expected to be a multi-valued one (such as "in") giving values binder type.
	 *
	 * @param paramName name of the parameter in the sql
	 * @param value value for the parameter
	 * @param valueType type for values {@link java.sql.PreparedStatement} binder
	 * @return this
	 */
	<O> BeanPropertyQueryMapper<C> set(String paramName, Iterable<O> value, Class<? super O> valueType);
}
