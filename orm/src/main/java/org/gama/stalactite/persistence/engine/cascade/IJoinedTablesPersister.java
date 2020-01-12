package org.gama.stalactite.persistence.engine.cascade;

import org.gama.stalactite.persistence.engine.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.IConfiguredPersister;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface IJoinedTablesPersister<C, I> {
	
	<U, J, Z> String addPersister(String ownerStrategyName, IConfiguredPersister<U, J> persister, BeanRelationFixer<Z, U> beanRelationFixer,
								  Column leftJoinColumn,
								  Column rightJoinColumn,
								  boolean isOuterJoin);
	
	/**
	 * Gives a copy of current joins to given persister
	 * @param sourcePersister source that needs this instance joins
	 * @param leftColumn left part of the join, expected to be one of source table 
	 * @param rightColumn right part of the join, expected to be one of current instance table
	 * @param beanRelationFixer setter that fix relation ofthis instance onto source persister instance
	 * @param nullable true for optional relation, makes an outer join, else should create a inner join
	 */
	<SRC> void joinWith(IJoinedTablesPersister<SRC, I> sourcePersister,
						Column leftColumn, Column rightColumn, BeanRelationFixer<SRC, C> beanRelationFixer, boolean nullable);
	
	JoinedStrategiesSelect<C, I, ?> getJoinedStrategiesSelect();
	
	/**
	 * Copies current instance joins root to given select
	 * 
	 * @param joinedStrategiesSelect target of the copy
	 * @param joinName name of target select join on which joins of thisinstance must be copied
	 * @param <E> target select entity type
	 * @param <ID> identifier tyoe
	 * @param <T> table type
	 */
	<E, ID, T extends Table> void copyJoinsRootTo(JoinedStrategiesSelect<E, ID, T> joinedStrategiesSelect, String joinName);
	
}
