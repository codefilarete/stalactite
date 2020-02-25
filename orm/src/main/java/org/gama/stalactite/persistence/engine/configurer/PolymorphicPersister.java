package org.gama.stalactite.persistence.engine.configurer;

import org.gama.stalactite.persistence.engine.BeanRelationFixer;
import org.gama.stalactite.persistence.engine.cascade.IJoinedTablesPersister;
import org.gama.stalactite.persistence.structure.Column;

/**
 * @author Guillaume Mary
 */
public interface PolymorphicPersister<C, I> {
	
	<SRC> void joinAsMany(IJoinedTablesPersister<SRC, I> sourcePersister,
						  Column leftColumn, Column rightColumn, BeanRelationFixer<SRC, C> beanRelationFixer, String joinName);
}