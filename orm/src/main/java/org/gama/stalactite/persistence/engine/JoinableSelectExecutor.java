package org.codefilarete.stalactite.persistence.engine;

import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.persistence.mapping.EntityMappingStrategy;
import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface JoinableSelectExecutor {
	
	<U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addRelation(String leftStrategyName,
																		   EntityMappingStrategy<U, ID, T2> strategy,
																		   BeanRelationFixer beanRelationFixer,
																		   Column<T1, ID> leftJoinColumn,
																		   Column<T2, ID> rightJoinColumn,
																		   boolean isOuterJoin);
	
	<U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addComplementaryJoin(String leftStrategyName,
																					EntityMappingStrategy<U, ID, T2> strategy,
																					Column<T1, ID> leftJoinColumn,
																					Column<T2, ID> rightJoinColumn);
}
