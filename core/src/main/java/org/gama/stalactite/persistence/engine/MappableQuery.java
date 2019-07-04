package org.gama.stalactite.persistence.engine;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.danekja.java.util.function.serializable.SerializableSupplier;
import org.gama.lang.function.SerializableTriFunction;
import org.gama.lang.function.ThrowingConverter;
import org.gama.sql.result.ResultSetRowAssembler;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Contract to define mapping and execution of some SQL select
 * 
 * @author Guillaume Mary
 */
public interface MappableQuery<C> {
	
	<I> MappableQuery<C> mapKey(SerializableFunction<I, C> factory, String columnName, Class<I> columnType);
	
	<I, J> MappableQuery<C> mapKey(SerializableBiFunction<I, J, C> factory, String column1Name, Class<I> column1Type, String column2Name, Class<J> column2Type);
	
	<I, J, K> MappableQuery<C> mapKey(SerializableTriFunction<I, J, K, C> factory, String column1Name, Class<I> column1Type,
									  String column2Name, Class<J> column2Type,
									  String column3Name, Class<K> column3Type);
	
	<I> MappableQuery<C> mapKey(SerializableFunction<I, C> factory, Column<? extends Table, I> column);
	
	<I, J> MappableQuery<C> mapKey(SerializableBiFunction<I, J, C> factory, Column<? extends Table, I> column1, Column<? extends Table, J> column2);
	
	<I, J, K> MappableQuery<C> mapKey(SerializableTriFunction<I, J, K, C> factory,
									  Column<? extends Table, I> column1,
									  Column<? extends Table, J> column2,
									  Column<? extends Table, K> column3
	);
	
	<I> MappableQuery<C> mapKey(SerializableSupplier<C> javaBeanCtor, String columnName, SerializableBiConsumer<C, I> keySetter);
	
	<I> MappableQuery<C> mapKey(SerializableSupplier<C> javaBeanCtor, Column<? extends Table, I> column, SerializableBiConsumer<C, I> keySetter);
	
	<I> MappableQuery<C> map(String columnName, SerializableBiConsumer<C, I> setter, Class<I> columnType);
	
	<I, J> MappableQuery<C> map(String columnName, SerializableBiConsumer<C, J> setter, Class<I> columnType, SerializableFunction<I, J> converter);
	
	<I> MappableQuery<C> map(String columnName, SerializableBiConsumer<C, I> setter);
	
	<I> MappableQuery<C> map(String columnName, SerializableBiConsumer<C, I> setter, SerializableFunction<I, I> converter);
	
	<I> MappableQuery<C> map(Column<? extends Table, I> column, SerializableBiConsumer<C, I> setter);
	
	<I, J> MappableQuery<C> map(Column<? extends Table, I> column, SerializableBiConsumer<C, J> setter, ThrowingConverter<I, J, RuntimeException> converter);
	
	/**
	 * Sets a value for the given parameter
	 * 
	 * @param paramName name of the parameter in the sql
	 * @param value value for the parameter
	 * @return this
	 */
	MappableQuery<C> set(String paramName, Object value);
	
	/**
	 * Sets a value for the given parameter giving explicit binder type
	 *
	 * @param paramName name of the parameter in the sql
	 * @param value value for the parameter
	 * @param valueType type for value {@link java.sql.PreparedStatement} binder
	 * @return this
	 */
	<O> MappableQuery<C> set(String paramName, O value, Class<? super O> valueType);
	
	/**
	 * Sets a value for the given parameter expected to be a multi-valued one (such as "in") giving values binder type.
	 *
	 * @param paramName name of the parameter in the sql
	 * @param value value for the parameter
	 * @param valueType type for values {@link java.sql.PreparedStatement} binder
	 * @return this
	 */
	<O> MappableQuery<C> set(String paramName, Iterable<O> value, Class<? super O> valueType);
	
	/**
	 * Adds a low level {@link java.sql.ResultSet} transfomer, for cases where mapping methods are unsufficient.
	 * 
	 * @param assembler a low-level {@link java.sql.ResultSet} transformer
	 * @return this
	 */
	MappableQuery<C> add(ResultSetRowAssembler<C> assembler);
}
