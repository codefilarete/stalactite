package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.engine.cascade.IJoinedTablesPersister;

/**
 * @author Guillaume Mary
 */
public interface IConfiguredJoinedTablesPersister<C, I> extends IConfiguredPersister<C, I>, IJoinedTablesPersister<C, I> {
}
