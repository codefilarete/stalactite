package org.codefilarete.stalactite.engine;

import java.util.Set;
import java.util.function.Consumer;

import org.codefilarete.stalactite.engine.PersistenceContext.ExecutableBeanPropertyKeyQueryMapper;
import org.codefilarete.stalactite.engine.PersistenceContext.ExecutableCriteria;
import org.codefilarete.stalactite.engine.PersistenceContext.SelectMapping;
import org.codefilarete.stalactite.query.builder.SQLBuilder;
import org.codefilarete.stalactite.query.model.ConditionalOperator;
import org.codefilarete.stalactite.query.model.CriteriaChain;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.QueryProvider;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Accumulator;
import org.codefilarete.tool.function.SerializableTriFunction;
import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.danekja.java.util.function.serializable.SerializableSupplier;

/**
 * Contract to interact with a Database with some basic manner
 * 
 * @author Guillaume Mary
 */
public interface DatabaseCrudOperations {
	
	/**
	 * Creates a {@link ExecutableBeanPropertyKeyQueryMapper} from a {@link QueryProvider}, so it helps to build beans from a {@link Query}.
	 * Should be chained with {@link QueryMapper} mapping methods and obviously with its {@link ExecutableQuery#execute(Accumulator)}
	 *
	 * @param queryProvider the query provider to give the {@link Query} execute to populate beans
	 * @param beanType type of created beans, used for returned type marker
	 * @param <C> type of created beans
	 * @return a new {@link ExecutableBeanPropertyKeyQueryMapper} that must be configured and executed
	 * @see org.codefilarete.stalactite.query.model.QueryEase
	 */
	<C> ExecutableBeanPropertyKeyQueryMapper<C> newQuery(QueryProvider<Query> queryProvider, Class<C> beanType);
	
	/**
	 * Creates a {@link ExecutableBeanPropertyKeyQueryMapper} from a {@link Query} in order to build beans from the {@link Query}.
	 * Should be chained with {@link ExecutableBeanPropertyKeyQueryMapper} mapping methods and obviously with its {@link ExecutableQuery#execute(Accumulator)}
	 *
	 * @param query the query to execute to populate beans
	 * @param beanType type of created beans, used for returned type marker
	 * @param <C> type of created beans
	 * @return a new {@link ExecutableBeanPropertyKeyQueryMapper} that must be configured and executed
	 */
	<C> ExecutableBeanPropertyKeyQueryMapper<C> newQuery(Query query, Class<C> beanType);
	
	/**
	 * Creates a {@link ExecutableBeanPropertyKeyQueryMapper} from some SQL in order to build beans from the SQL.
	 * Should be chained with {@link ExecutableBeanPropertyKeyQueryMapper} mapping methods and obviously with its {@link ExecutableQuery#execute(Accumulator)}
	 *
	 * @param sql the SQL to execute to populate beans
	 * @param beanType type of created beans, used for returned type marker
	 * @param <C> type of created beans
	 * @return a new {@link ExecutableBeanPropertyKeyQueryMapper} that must be configured and executed
	 */
	<C> ExecutableBeanPropertyKeyQueryMapper<C> newQuery(CharSequence sql, Class<C> beanType);
	
	/**
	 * Same as {@link #newQuery(CharSequence, Class)} with an {@link SQLBuilder} as argument to be more flexible : final SQL will be built just
	 * before execution.
	 *
	 * @param sql the builder of SQL to be called for final SQL
	 * @param beanType type of created beans, used for returned type marker
	 * @param <C> type of created beans
	 * @return a new {@link ExecutableBeanPropertyKeyQueryMapper} that must be configured and executed
	 */
	<C> ExecutableBeanPropertyKeyQueryMapper<C> newQuery(SQLBuilder sql, Class<C> beanType);
	
	/**
	 * Queries the database for given column and invokes given 1-arg constructor with it.
	 * <p>
	 * Usage is for very simple cases because IT DOESN'T FILTER DATABASE ROWS : no where clause is appended to the query.
	 * Moreover, only column table is queried : no join nor assembly is made.
	 * Prefer {@link #select(SerializableFunction, Column, Consumer, Consumer)} for a more complete use case, or even {@link #newQuery(SQLBuilder, Class)}
	 *
	 * @param factory a one-argument bean constructor
	 * @param column any table column (primary key may be preferred because its result is given to bean constructor but it is not expected)
	 * @param <C> type of created beans
	 * @param <I> constructor arg and column types
	 * @param <T> targeted table type
	 * @return a set of all table records mapped to the given bean
	 * @see #select(SerializableFunction, Column, Consumer, Consumer)
	 * @see #newQuery(SQLBuilder, Class)
	 */
	<C, I, T extends Table> Set<C> select(SerializableFunction<I, C> factory, Column<T, I> column);
	
	/**
	 * Queries the database for given columns and invokes given 2-args constructor with them.
	 * <p>
	 * Usage is for very simple cases because IT DOESN'T FILTER DATABASE ROWS : no where clause is appended to the query.
	 * Moreover, only columns table is queried : no join nor assembly are processed.
	 * Prefer {@link #select(SerializableBiFunction, Column, Column, Consumer, Consumer)} for a more complete use case, or even {@link #newQuery(SQLBuilder, Class)}
	 *
	 * @param factory a two-arguments bean constructor
	 * @param column1 a table column (may be a primary key column because its result is given to bean constructor but it is not expected)
	 * @param column2 a table column (may be a primary key column because its result is given to bean constructor but it is not expected)
	 * @param <C> type of created beans
	 * @param <I> constructor first-arg type and first column type
	 * @param <J> constructor second-arg type and second column type
	 * @param <T> targeted table type
	 * @return a set of all table records mapped to the given bean
	 * @see #select(SerializableBiFunction, Column, Column, Consumer, Consumer)
	 * @see #newQuery(SQLBuilder, Class)
	 */
	<C, I, J, T extends Table> Set<C> select(SerializableBiFunction<I, J, C> factory, Column<T, I> column1, Column<T, J> column2);
	
	/**
	 * Queries the database for given columns and invokes given 3-args constructor with them.
	 * <p>
	 * Usage is for very simple cases because IT DOESN'T FILTER DATABASE ROWS : no where clause is appended to the query.
	 * Moreover, only columns table is queried : no join nor assembly are processed.
	 * Prefer {@link #select(SerializableBiFunction, Column, Column, Consumer, Consumer)} for a more complete use case, or even {@link #newQuery(SQLBuilder, Class)}
	 *
	 * @param factory a two-arguments bean constructor
	 * @param column1 a table column (may be a primary key column because its result is given to bean constructor but it is not expected)
	 * @param column2 a table column (may be a primary key column because its result is given to bean constructor but it is not expected)
	 * @param column3 a table column (may be a primary key column because its result is given to bean constructor but it is not expected)
	 * @param <C> type of created beans
	 * @param <I> constructor first-arg type and first column type
	 * @param <J> constructor second-arg type and second column type
	 * @param <K> constructor third-arg type and third column type
	 * @param <T> targeted table type
	 * @return a set of all table records mapped to the given bean
	 * @see #select(SerializableBiFunction, Column, Column, Consumer, Consumer)
	 * @see #newQuery(SQLBuilder, Class)
	 */
	<C, I, J, K, T extends Table> Set<C> select(SerializableTriFunction<I, J, K, C> factory, Column<T, I> column1, Column<T, J> column2, Column<T, K> column3);
	
	/**
	 * Queries the database and invokes given no-arg constructor for each row.
	 * Additional bean fulfillment will be done by using configuration you'll give through {@link SelectMapping}.
	 * <p>
	 * Usage is for very simple cases because IT DOESN'T FILTER DATABASE ROWS : no where clause is appended to the query.
	 * Moreover, only columns table is queried : no join nor assembly are processed.
	 * Prefer {@link #select(SerializableBiFunction, Column, Column, Consumer, Consumer)} for a more complete use case, or even {@link #newQuery(SQLBuilder, Class)}
	 *
	 * @param factory a two-arguments bean constructor
	 * @param <C> type of created beans
	 * @param <T> targeted table type
	 * @return a set of all table records mapped to the given bean
	 * @see #select(SerializableBiFunction, Column, Column, Consumer, Consumer)
	 * @see #newQuery(SQLBuilder, Class)
	 */
	<C, T extends Table> Set<C> select(SerializableSupplier<C> factory, Consumer<SelectMapping<C, T>> selectMapping);
	
	/**
	 * Queries the database for given columns and invokes given 1-arg constructor with it.
	 * Additional bean fulfillment will be done by using configuration you'll give through {@link SelectMapping}.
	 * <p>
	 * Usage is for very simple cases because IT DOESN'T FILTER DATABASE ROWS : no where clause is appended to the query.
	 * Moreover, only column table is queried : no join nor assembly are processed.
	 * Prefer {@link #select(SerializableFunction, Column, Consumer, Consumer)} for a more complete use case, or even {@link #newQuery(SQLBuilder, Class)}
	 *
	 * @param factory a one-argument bean constructor
	 * @param column any table column (primary key may be preferred because its result is given to bean constructor but it is not expected)
	 * @param selectMapping allow to add some mapping beyond instantiation time
	 * @param <C> type of created beans
	 * @param <I> constructor arg and column types
	 * @param <T> targeted table type
	 * @return a set of all table records mapped to the given bean
	 * @see #select(SerializableFunction, Column, Consumer, Consumer)
	 * @see #newQuery(SQLBuilder, Class)
	 */
	<C, I, T extends Table> Set<C> select(SerializableFunction<I, C> factory, Column<T, I> column, Consumer<SelectMapping<C, T>> selectMapping);
	
	/**
	 * Queries the database for given columns and invokes given 2-args constructor with them.
	 * Additional bean fulfillment will be done by using configuration you'll give through {@link SelectMapping}.
	 * <p>
	 * Usage is for very simple cases because IT DOESN'T FILTER DATABASE ROWS : no where clause is appended to the query.
	 * Moreover, only columns table is queried : no join nor assembly are processed.
	 * Prefer {@link #select(SerializableFunction, Column, Consumer, Consumer)} for a more complete use case, or even {@link #newQuery(SQLBuilder, Class)}
	 *
	 * @param factory a two-arguments bean constructor
	 * @param column1 a table column (may be a primary key column because its result is given to bean constructor but it is not expected)
	 * @param column2 a table column (may be a primary key column because its result is given to bean constructor but it is not expected)
	 * @param selectMapping allow to add some mapping beyond instantiation time
	 * @param <C> type of created beans
	 * @param <I> constructor arg and column types
	 * @param <T> targeted table type
	 * @return a set of all table records mapped to the given bean
	 * @see #select(SerializableFunction, Column, Consumer, Consumer)
	 * @see #newQuery(SQLBuilder, Class)
	 */
	<C, I, J, T extends Table> Set<C> select(SerializableBiFunction<I, J, C> factory, Column<T, I> column1, Column<T, J> column2,
											 Consumer<SelectMapping<C, T>> selectMapping);
	
	/**
	 * Queries the database and invokes given no-arg constructor for each row.
	 * Additional bean fulfillment will be done by using configuration you'll give through {@link SelectMapping}.
	 * <p>
	 * Usage is for very simple cases : only columns table is targeted (no join nor assembly are processed).
	 * Prefer {@link #newQuery(SQLBuilder, Class)} for a more complete use case.
	 *
	 * @param factory a two-arguments bean constructor
	 * @param <C> type of created beans
	 * @param <T> targeted table type
	 * @return a set of all table records mapped to the given bean
	 * @see #select(SerializableBiFunction, Column, Column, Consumer, Consumer)
	 * @see #newQuery(SQLBuilder, Class)
	 */
	<C, T extends Table> Set<C> select(SerializableSupplier<C> factory, Consumer<SelectMapping<C, T>> selectMapping,
									   Consumer<CriteriaChain> where);
	
	/**
	 * Queries the database for given column and invokes given 1-arg constructor with it.
	 * Additional bean fulfillment will be done by using configuration you'll give through {@link SelectMapping}.
	 * <p>
	 * Usage is for very simple cases : only columns table is targeted (no join nor assembly are processed).
	 * Prefer {@link #newQuery(SQLBuilder, Class)} for a more complete use case.
	 *
	 * @param factory a one-argument bean constructor
	 * @param column any table column (primary key may be preferred because its result is given to bean constructor but it is not expected)
	 * @param selectMapping allow to add some mapping beyond instantiation time
	 * @param <C> type of created beans
	 * @param <I> constructor arg and column types
	 * @param <T> targeted table type
	 * @return a set of all table records mapped to the given bean
	 * @see #newQuery(SQLBuilder, Class)
	 */
	<C, I, T extends Table> Set<C> select(SerializableFunction<I, C> factory, Column<T, I> column,
										  Consumer<SelectMapping<C, T>> selectMapping,
										  Consumer<CriteriaChain> where);
	
	/**
	 * Queries the database for given columns and invokes given 2-args constructor with them.
	 * Additional bean fulfillment will be done by using configuration you'll give through {@link SelectMapping}.
	 * <p>
	 * Usage is for very simple cases : only columns table is targeted (no join nor assembly are processed).
	 * Prefer {@link #newQuery(SQLBuilder, Class)} for a more complete use case.
	 *
	 * @param factory a one-argument bean constructor
	 * @param column1 a table column (may be a primary key column because its result is given to bean constructor but it is not expected)
	 * @param column2 a table column (may be a primary key column because its result is given to bean constructor but it is not expected)
	 * @param selectMapping allow to add some mapping beyond instantiation time
	 * @param <C> type of created beans
	 * @param <I> constructor first-arg type and first column type
	 * @param <J> constructor second-arg type and second column type
	 * @param <T> targeted table type
	 * @return a set of all table records mapped to the given bean
	 * @see #newQuery(SQLBuilder, Class)
	 */
	<C, I, J, T extends Table> Set<C> select(SerializableBiFunction<I, J, C> factory, Column<T, I> column1, Column<T, J> column2,
											 Consumer<SelectMapping<C, T>> selectMapping,
											 Consumer<CriteriaChain> where);
	
	<T extends Table<T>> ExecutableUpdate<T> update(T table);
	
	<T extends Table<T>> ExecutableInsert<T> insert(T table);
	
	<T extends Table<T>> ExecutableDelete<T> delete(T table);
	
	interface ExecutableUpdate<T extends Table<T>> {
		
		/**
		 * Adds a column to update with its value.
		 *
		 * @param column any column
		 * @param value value for given column
		 * @param <O> value type
		 * @return this
		 */
		<O> ExecutableUpdate<T> set(Column<T, O> column, O value);
		
		/**
		 * Adds a target column which value is took from another column (which can be one of another table if this update is a multi-table one)
		 *
		 * @param column1 any column
		 * @param column2 any column
		 * @param <O> value type
		 * @return this
		 */
		<O> ExecutableUpdate<T> set(Column<T, O> column1, Column<?, O> column2);
		
		/**
		 * Executes this update statement with given values
		 */
		void execute();
		
		/**
		 * Adds a criteria to this update.
		 *
		 * @param column a column target of the condition
		 * @param condition the condition
		 * @return this
		 */
		ExecutableCriteria where(Column<T, ?> column, String condition);
		
		/**
		 * Adds a criteria to this update.
		 *
		 * @param column a column target of the condition
		 * @param condition the condition
		 * @return this
		 */
		ExecutableCriteria where(Column<T, ?> column, ConditionalOperator condition);
	}
	
	interface ExecutableInsert<T extends Table<T>> {
		
		/**
		 * Adds a column to set and its value. Overwrites any previous value put for that column.
		 *
		 * @param column any column
		 * @param value value to be inserted
		 * @param <C> value type
		 * @return this
		 */
		<C> ExecutableInsert<T> set(Column<T, C> column, C value);
		
		/**
		 * Executes this insert statement.
		 */
		void execute();
	}
	
	interface ExecutableDelete<T extends Table<T>> {
		
		/**
		 * Executes this delete statement with given values.
		 */
		void execute();
		
		/**
		 * Adds a criteria to this delete.
		 *
		 * @param column a column target of the condition
		 * @param condition the condition
		 * @return this
		 */
		ExecutableCriteria where(Column<T, ?> column, String condition);
		
		/**
		 * Adds a criteria to this delete.
		 *
		 * @param column a column target of the condition
		 * @param condition the condition
		 * @return this
		 */
		ExecutableCriteria where(Column<T, ?> column, ConditionalOperator condition);
	}
}
