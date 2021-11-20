package org.gama.stalactite.persistence.engine;

import java.sql.ResultSet;

import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.danekja.java.util.function.serializable.SerializableSupplier;
import org.gama.lang.function.SerializableTriFunction;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.Query;

/**
 * Methods that define root bean creation when creating an SQL query through {@link PersistenceContext#newQuery(Query, Class)}.
 * All methods return a {@link BeanKeyQueryMapper} to disallow chaining mapKey() several times since it doesn't seem relevant.
 * 
 * @author Guillaume Mary
 */
public interface BeanKeyQueryMapper<C> {
	
	/**
	 * Declares a 1-arg root bean constructor and its key from the {@link ResultSet}. Column type will be deduced from given constructor.
	 *
	 * @param javaBeanCtor bean constructor that will take column value as parameter
	 * @param columnName column containing identifier value
	 * @param <I> identifier type
	 * @return an instance that allows method chaining
	 */
	<I> BeanPropertyQueryMapper<C> mapKey(SerializableFunction<I, C> javaBeanCtor, String columnName);
	
	/**
	 * Declares a 2-args root bean constructor and its composed key from the {@link ResultSet}. Column type will be deduced from given constructor.
	 *
	 * @param javaBeanCtor bean constructor that will take column value as parameter
	 * @param columnName1 column containing first identifier value
	 * @param columnName2 column containing second identifier value
	 * @param <I> identifier type
	 * @return an instance that allows method chaining
	 */
	<I, J> BeanPropertyQueryMapper<C> mapKey(SerializableBiFunction<I, J, C> javaBeanCtor, String columnName1, String columnName2);
	
	/**
	 * Declares a 3-args root bean constructor and its composed key from the {@link ResultSet}. Column type will be deduced from given constructor.
	 *
	 * @param javaBeanCtor bean constructor that will take column value as parameter
	 * @param columnName1 column containing first identifier value
	 * @param columnName2 column containing second identifier value
	 * @param columnName3 column containing third identifier value
	 * @param <I> identifier type
	 * @return an instance that allows method chaining
	 */
	<I, J, K> BeanPropertyQueryMapper<C> mapKey(SerializableTriFunction<I, J, K, C> javaBeanCtor, String columnName1, String columnName2, String columnName3);
	
	/**
	 * Declares a no-arg root bean constructor.
	 * Please note that this constructor mapping implies that an instance per {@link ResultSet} row will be created : since no data key is given
	 * each row is considered different from another. 
	 * 
	 * @param javaBeanCtor no-arg bean constructor
	 * @return an instance that allows method chaining
	 */
	BeanPropertyQueryMapper<C> mapKey(SerializableSupplier<C> javaBeanCtor);
	
	/**
	 * Declares a 1-arg root bean constructor and its key from the {@link ResultSet}.
	 *
	 * @param javaBeanCtor bean constructor that will take column value as parameter
	 * @param columnName column containing identifier value
	 * @param columnType identifier type
	 * @param <I> identifier type
	 * @return an instance that allows method chaining
	 */
	<I> BeanPropertyQueryMapper<C> mapKey(SerializableFunction<I, C> javaBeanCtor, String columnName, Class<I> columnType);
	
	/**
	 * Equivalent of {@link #mapKey(SerializableFunction, String, Class)} with a 2-args constructor.
	 * Declares a 2-args root bean constructor and its composed key from the {@link ResultSet}.
	 *
	 * @param javaBeanCtor bean constructor that will take column values as parameters
	 * @param column1Name column containing first identifier value
	 * @param column1Type first identifier type
	 * @param column2Name column containing second identifier value
	 * @param column2Type second identifier type
	 * @param <I> first identifier type
	 * @param <J> second identifier type
	 * @return an instance that allows method chaining
	 */
	<I, J> BeanPropertyQueryMapper<C> mapKey(SerializableBiFunction<I, J, C> javaBeanCtor,
											 String column1Name, Class<I> column1Type,
											 String column2Name, Class<J> column2Type);
	
	/**
	 * Equivalent of {@link #mapKey(SerializableFunction, String, Class)} with a 3-args constructor.
	 * Declares a 3-args root bean constructor and its composed key from the {@link ResultSet}.
	 *
	 * @param javaBeanCtor bean constructor that will take column values as parameters
	 * @param column1Name column containing first identifier value
	 * @param column1Type first identifier type
	 * @param column2Name column containing second identifier value
	 * @param column2Type second identifier type
	 * @param column3Name column containing third identifier value
	 * @param column3Type third identifier type
	 * @param <I> first identifier type
	 * @param <J> second identifier type
	 * @param <K> third identifier type
	 * @return an instance that allows method chaining
	 */
	<I, J, K> BeanPropertyQueryMapper<C> mapKey(SerializableTriFunction<I, J, K, C> javaBeanCtor,
												String column1Name, Class<I> column1Type,
												String column2Name, Class<J> column2Type,
												String column3Name, Class<K> column3Type);
	
	/**
	 * Equivalent of {@link #mapKey(SerializableFunction, String, Class)} with a column argument.
	 * Declares a 1-arg root bean constructor and its key from the {@link ResultSet}.
	 *
	 * @param javaBeanCtor bean constructor that will take column values as parameters
	 * @param column column containing identifier value
	 * @param <I> first identifier type
	 * @return an instance that allows method chaining
	 */
	<I> BeanPropertyQueryMapper<C> mapKey(SerializableFunction<I, C> javaBeanCtor, Column<? extends Table, I> column);
	
	/**
	 * Equivalent of {@link #mapKey(SerializableFunction, Column)} with 2 columns argument.
	 * Declares a 2-args root bean constructor and its composed key from the {@link ResultSet}.
	 *
	 * @param javaBeanCtor bean constructor that will take column values as parameters
	 * @param column1 column containing first identifier value
	 * @param column2 column containing second identifier value
	 * @param <I> first identifier type
	 * @param <J> second identifier type
	 * @return an instance that allows method chaining
	 */
	<I, J> BeanPropertyQueryMapper<C> mapKey(SerializableBiFunction<I, J, C> javaBeanCtor, Column<? extends Table, I> column1, Column<? extends Table, J> column2);
	
	/**
	 * Equivalent of {@link #mapKey(SerializableFunction, Column)} with 3 columns argument.
	 * Declares a 3-args root bean constructor and its composed key from the {@link ResultSet}.
	 *
	 * @param javaBeanCtor bean constructor that will take column values as parameters
	 * @param column1 column containing first identifier value
	 * @param column2 column containing second identifier value
	 * @param column3 column containing third identifier value
	 * @param <I> first identifier type
	 * @param <J> second identifier type
	 * @param <K> thrid identifier type
	 * @return an instance that allows method chaining
	 */
	<I, J, K> BeanPropertyQueryMapper<C> mapKey(SerializableTriFunction<I, J, K, C> javaBeanCtor,
												Column<? extends Table, I> column1,
												Column<? extends Table, J> column2,
												Column<? extends Table, K> column3
	);
	
	/**
	 * Key mapper for cases where given column is the only retrieved, meaning that there's no bean to be built else than the column value
	 *
	 * @param columnName column containing value
	 * @param columnType column type
	 * @param <I> identifier type
	 * @return an instance that allows method chaining
	 */
	<I> BeanPropertyQueryMapper<I> mapKey(String columnName, Class<I> columnType);
}
