package org.codefilarete.stalactite.engine.configurer;

import org.codefilarete.stalactite.dsl.naming.TableNamingStrategy;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

import java.util.Set;

public class AbstractRelationConfigurer<SRC, SRCID, TRGT, TRGTID> {

	protected final Dialect dialect;
	protected final ConnectionConfiguration connectionConfiguration;
	protected final ConfiguredRelationalPersister<SRC, SRCID> sourcePersister;

	protected final TableNamingStrategy tableNamingStrategy;
	protected final PersisterBuilderContext currentBuilderContext;

	public AbstractRelationConfigurer(Dialect dialect,
									  ConnectionConfiguration connectionConfiguration,
									  ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
									  TableNamingStrategy tableNamingStrategy,
									  PersisterBuilderContext currentBuilderContext) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
		this.sourcePersister = sourcePersister;
		this.tableNamingStrategy = tableNamingStrategy;
		this.currentBuilderContext = currentBuilderContext;
	}

	protected Table lookupTableInRegisteredPersisters(Class<TRGT> entityType) {
		String expectedTargetTableName = tableNamingStrategy.giveName(entityType);
		Set<EntityPersister> persisters = currentBuilderContext.getPersisterRegistry().getPersisters();
		return persisters.stream()
				.flatMap(p -> ((ConfiguredPersister<?, ?>) p).giveImpliedTables().stream())
				.filter(table -> table.getName().equals(expectedTargetTableName))
				.findFirst().orElseGet(() -> null);
	}
}