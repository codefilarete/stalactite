package org.gama.stalactite.persistence.engine;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.gama.lang.Duo;
import org.gama.lang.collection.Iterables;
import org.gama.stalactite.persistence.engine.listening.IPersisterListener;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.query.model.AbstractRelationalOperator;

/**
 * @author Guillaume Mary
 */
public interface IPersister<C, I> extends IInsertExecutor<C>, IUpdateExecutor<C>, ISelectExecutor<C, I>, IDeleteExecutor<C, I>, IPersisterListener<C, I> {
	
	/**
	 * Persists an instance either it is already persisted or not (insert or update).
	 *
	 * Check between insert or update is determined by id state which itself depends on identifier policy,
	 * see {@link org.gama.stalactite.persistence.mapping.SimpleIdMappingStrategy#IsNewDeterminer} implementations and
	 * {@link org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager} implementations for id value computation. 
	 *
	 * @param entity an entity to be persisted
	 * @return persisted + updated rows count
	 * @throws StaleObjectExcepion if updated row count differs from entities count
	 * @see #insert(Iterable)
	 * @see #update(Iterable, boolean)
	 */
	default int persist(C entity) {
		// determine insert or update operation
		return persist(Collections.singleton(entity));
	}
	
	int persist(Iterable<C> entities);
	
	default int insert(C entity) {
		return insert(Collections.singletonList(entity));
	}
	
	/**
	 * Updates an instance that may have changes.
	 * Groups statements to benefit from JDBC batch. Usefull overall when allColumnsStatement
	 * is set to false.
	 *
	 * @param modified the supposing entity that has differences againt {@code unmodified} entity
	 * @param unmodified the "original" (freshly loaded from database ?) entity
	 * @param allColumnsStatement true if all columns must be in the SQL statement, false if only modified ones should be in
	 * @return number of rows updated (relation-less counter) (maximum is 1, may be 0 if row wasn't found in database)
	 */
	default int update(C modified, C unmodified, boolean allColumnsStatement) {
		return update(Collections.singletonList(new Duo<>(modified, unmodified)), allColumnsStatement);
	}
	
	/**
	 * Deletes instances.
	 * Takes optimistic lock into account.
	 *
	 * @param entity entity to be deleted
	 * @return deleted row count
	 * @throws StaleObjectExcepion if deleted row count differs from entities count
	 */
	default int delete(C entity) {
		return delete(Collections.singletonList(entity));
	}
	
	/**
	 * Will delete instances only by their identifier.
	 * This method will not take optimisic lock (versioned entity) into account, so it will delete database rows "roughly".
	 *
	 * @param entity entity to be deleted
	 * @return deleted row count
	 */
	default int deleteById(C entity) {
		return deleteById(Collections.singletonList(entity));
	}
	
	default C select(I id) {
		return Iterables.first(select(Collections.singleton(id)));
	}
	
	/**
	 * Creates a query which criteria target mapped properties.
	 * <strong>As for now aggregate result is truncated to entities returned by SQL selection : for example, if criteria on collection is used,
	 * only entities returned by SQL criteria will be loaded. This does not respect aggregate principle and should be enhanced in future.</strong>
	 *
	 * @param getter a property accessor
	 * @param operator criteria for the property
	 * @param <O> value type returned by property accessor
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute()}
	 */
	<O> ExecutableEntityQuery<C> selectWhere(SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator);
	
	/**
	 * Creates a query which criteria target mapped properties
	 * <strong>As for now aggregate result is truncated to entities returned by SQL selection : for example, if criteria on collection is used,
	 * only entities returned by SQL criteria will be loaded. This does not respect aggregate principle and should be enhanced in future.</strong>
	 *
	 * @param setter a property accessor
	 * @param operator criteria for the property
	 * @param <O> value type returned by property accessor
	 * @return a {@link EntityCriteria} enhance to be executed through {@link ExecutableQuery#execute()}
	 */
	<O> ExecutableEntityQuery<C> selectWhere(SerializableBiConsumer<C, O> setter, AbstractRelationalOperator<O> operator);
	
	List<C> selectAll();
	
	<T extends Table<T>> IEntityMappingStrategy<C, I, T> getMappingStrategy();
	
	Collection<Table> giveImpliedTables();
	
	interface ExecutableEntityQuery<C> extends EntityCriteria<C>, ExecutableQuery<C> {
		
	}
	
	/**
	 * Mashup between {@link EntityCriteria} and {@link ExecutableQuery} to make an {@link EntityCriteria} executable
	 * @param <C> type of object returned by query execution
	 */
	interface EntityCriteria<C> {
		
		/**
		 * Combines with "and" given criteria on property  
		 *
		 * @param getter a method reference to a getter
		 * @param operator operator of the criteria (will be the condition on the matching column)
		 * @param <O> getter return type, also criteria value
		 * @return this
		 * @throws org.gama.stalactite.persistence.engine.RuntimeMappingException if column matching getter was not found
		 */
		<O> EntityCriteria<C> and(SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator);
		
		/**
		 * Combines with "and" given criteria on property  
		 *
		 * @param setter a method reference to a setter
		 * @param operator operator of the criteria (will be the condition on the matching column)
		 * @param <O> getter return type, also criteria value
		 * @return this
		 * @throws org.gama.stalactite.persistence.engine.RuntimeMappingException if column matching setter was not found
		 */
		<O> EntityCriteria<C> and(SerializableBiConsumer<C, O> setter, AbstractRelationalOperator<O> operator);
		
		/**
		 * Combines with "or" given criteria on property  
		 *
		 * @param getter a method reference to a getter
		 * @param operator operator of the criteria (will be the condition on the matching column)
		 * @param <O> getter return type, also criteria value
		 * @return this
		 * @throws org.gama.stalactite.persistence.engine.RuntimeMappingException if column matching getter was not found
		 */
		<O> EntityCriteria<C> or(SerializableFunction<C, O> getter, AbstractRelationalOperator<O> operator);
		
		/**
		 * Combines with "or" given criteria on property  
		 *
		 * @param setter a method reference to a setter
		 * @param operator operator of the criteria (will be the condition on the matching column)
		 * @param <O> getter return type, also criteria value
		 * @return this
		 * @throws org.gama.stalactite.persistence.engine.RuntimeMappingException if column matching setter was not found
		 */
		<O> EntityCriteria<C> or(SerializableBiConsumer<C, O> setter, AbstractRelationalOperator<O> operator);
		
		/**
		 * Combines with "and" given criteria on an embedded or one-to-one bean property
		 *
		 * @param getter1 a method reference to the embbeded bean
		 * @param getter2 a method reference to the embbeded bean property
		 * @param operator operator of the criteria (will be the condition on the matching column)
		 * @param <A> embedded bean type
		 * @param <B> embbeded bean property type, also criteria value
		 * @return this
		 * @throws org.gama.stalactite.persistence.engine.RuntimeMappingException if column matching getter was not found
		 */
		<A, B> EntityCriteria<C> and(SerializableFunction<C, A> getter1, SerializableFunction<A, B> getter2, AbstractRelationalOperator<B> operator);
	}
}
