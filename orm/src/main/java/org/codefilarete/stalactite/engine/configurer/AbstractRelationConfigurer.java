package org.codefilarete.stalactite.engine.configurer;

import java.util.Set;

import org.codefilarete.stalactite.dsl.naming.TableNamingStrategy;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.configurer.builder.DefaultPersisterBuilder;
import org.codefilarete.stalactite.engine.configurer.builder.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.runtime.ConfiguredPersister;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalEntityPersister;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

public class AbstractRelationConfigurer<SRC, SRCID, TRGT, TRGTID> {

	protected final Dialect dialect;
	protected final ConnectionConfiguration connectionConfiguration;
	protected final ConfiguredRelationalEntityPersister<SRC, SRCID, ?> sourcePersister;

	protected final TableNamingStrategy tableNamingStrategy;
	protected final PersisterBuilderContext currentBuilderContext;
	protected final DefaultPersisterBuilder persisterBuilder;
	
	public AbstractRelationConfigurer(Dialect dialect,
									  ConnectionConfiguration connectionConfiguration,
									  ConfiguredRelationalEntityPersister<SRC, SRCID, ?> sourcePersister,
									  TableNamingStrategy tableNamingStrategy,
									  PersisterBuilderContext currentBuilderContext) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
		this.sourcePersister = sourcePersister;
		this.tableNamingStrategy = tableNamingStrategy;
		this.currentBuilderContext = currentBuilderContext;
		this.persisterBuilder = new DefaultPersisterBuilder(dialect, connectionConfiguration, currentBuilderContext.getPersisterRegistry());
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
