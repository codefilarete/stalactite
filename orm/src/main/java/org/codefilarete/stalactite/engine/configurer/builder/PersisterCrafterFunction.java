package org.codefilarete.stalactite.engine.configurer.builder;

import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;

public interface PersisterCrafterFunction<C, I> {
	
	ConfiguredRelationalPersister<C, I> build(Dialect dialect, ConnectionConfiguration connectionConfiguration, PersisterBuilderContext context);
}
