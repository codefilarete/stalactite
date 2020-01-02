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
	 * Imports sourcePersister joins into current instance
	 * 
	 * @param joinName node name that contains joins to be imported
	 * @param sourcePersister joins source
	 */
	void addPersisterJoins(String joinName, IJoinedTablesPersister<?, ?> sourcePersister);
	
	JoinedStrategiesSelect<C, I, ?> getJoinedStrategiesSelect();
	
	/**
	 * Copies current instance joins root to given select
	 * 
	 * @param joinedStrategiesSelect
	 * @param joinName
	 * @param <I>
	 * @param <T>
	 * @param <C>
	 */
	<I, T extends Table, C> void copyJoinsRootTo(JoinedStrategiesSelect<C, I, T> joinedStrategiesSelect, String joinName);
}
