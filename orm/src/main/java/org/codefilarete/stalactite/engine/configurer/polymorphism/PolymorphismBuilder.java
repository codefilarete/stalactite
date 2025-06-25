package org.codefilarete.stalactite.engine.configurer.polymorphism;

import org.codefilarete.stalactite.engine.runtime.AbstractPolymorphismPersister;
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
	 * @return a persister
	 */
	AbstractPolymorphismPersister<C, I> build(Dialect dialect, ConnectionConfiguration connectionConfiguration);
}
