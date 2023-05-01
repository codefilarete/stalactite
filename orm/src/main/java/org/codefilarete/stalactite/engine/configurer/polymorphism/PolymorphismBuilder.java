package org.codefilarete.stalactite.engine.configurer.polymorphism;

import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Wide contract for {@link ConfiguredRelationalPersister} builders.
 *
 * @param <C> persisted entity type
 * @param <I> identifier type
 * @param <T> table type
 */
interface PolymorphismBuilder<C, I, T extends Table> {
	
	/**
	 * @param dialect the {@link Dialect} use for type binding
	 * @param connectionConfiguration the connection configuration
	 * @param persisterRegistry {@link PersisterRegistry} used to check for already defined persister
	 * @return a persister
	 */
	ConfiguredRelationalPersister<C, I> build(Dialect dialect, ConnectionConfiguration connectionConfiguration, PersisterRegistry persisterRegistry);
}
