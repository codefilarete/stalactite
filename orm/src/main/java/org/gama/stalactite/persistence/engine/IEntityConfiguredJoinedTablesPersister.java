package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.engine.runtime.IEntityConfiguredPersister;

/**
 * @author Guillaume Mary
 */
public interface IEntityConfiguredJoinedTablesPersister<C, I> extends IConfiguredJoinedTablesPersister<C, I>, IEntityConfiguredPersister<C, I> {
}
