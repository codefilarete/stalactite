package org.codefilarete.stalactite.engine.configurer.builder;

import org.codefilarete.stalactite.engine.VersioningStrategy;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.mapping.DefaultEntityMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Handle versioning strategy.
 *
 * @param <C>
 * @param <I>
 * @author Guillaume Mary
 */
public class VersioningStep<C, I> {
	
	public <T extends Table<T>> void handleVersioningStrategy(VersioningStrategy<C, ?> versioningStrategy, SimpleRelationalEntityPersister<C, I, T> mainEntityPersister) {
		if (versioningStrategy != null) {
			// we have to declare it to the mapping strategy. To do that we must find the versioning column
			Column column = mainEntityPersister.getMapping().getPropertyToColumn().get(versioningStrategy.getVersionAccessor());
			((DefaultEntityMapping) mainEntityPersister.getMapping()).addVersionedColumn(versioningStrategy.getVersionAccessor(), column);
			// and don't forget to give it to the workers !
			mainEntityPersister.getUpdateExecutor().setVersioningStrategy(versioningStrategy);
			mainEntityPersister.getInsertExecutor().setVersioningStrategy(versioningStrategy);
		}
	}
}
