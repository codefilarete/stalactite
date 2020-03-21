package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface JoinableSelectExecutor {
	
	<U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addRelation(String leftStrategyName,
																		   IEntityMappingStrategy<U, ID, T2> strategy,
																		   BeanRelationFixer beanRelationFixer,
																		   Column<T1, ID> leftJoinColumn,
																		   Column<T2, ID> rightJoinColumn,
																		   boolean isOuterJoin);
	
	<U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addComplementaryJoin(String leftStrategyName,
																					IEntityMappingStrategy<U, ID, T2> strategy,
																					Column<T1, ID> leftJoinColumn,
																					Column<T2, ID> rightJoinColumn);
}
