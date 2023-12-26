package org.codefilarete.stalactite.engine;

import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * @author Guillaume Mary
 */
public interface JoinableSelectExecutor {
	
	<U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addRelation(String leftStrategyName,
																		   EntityMapping<U, ID, T2> strategy,
																		   BeanRelationFixer beanRelationFixer,
																		   Key<T1, ID> leftJoinColumn,
																		   Key<T2, ID> rightJoinColumn,
																		   boolean isOuterJoin);
	
	<U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addMergeJoin(String leftStrategyName,
																			EntityMapping<U, ID, T2> strategy,
																			Key<T1, ID> leftJoinColumn,
																			Key<T2, ID> rightJoinColumn);
}
