package org.codefilarete.stalactite.engine.configurer.builder;

import java.util.Map;

import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.configurer.AbstractIdentification;
import org.codefilarete.stalactite.engine.configurer.NamingConfiguration;
import org.codefilarete.stalactite.engine.configurer.NamingConfigurationCollector;
import org.codefilarete.stalactite.engine.configurer.builder.InheritanceMappingStep.Mapping;
import org.codefilarete.stalactite.engine.configurer.builder.InheritanceMappingStep.MappingPerTable;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

import static org.codefilarete.tool.collection.Iterables.first;

public class PersisterBuilderPipeline<C, I> {
	
	private final TableLookupStep<C, I> tableLookupStep;
	private final TableMappingStep<C, I> tableMappingStep;
	private final IdentificationStep<C, I> identificationStep;
	private final InheritanceMappingStep<C, I> inheritanceMappingStep;
	private final PrimaryKeyStep<C, I> primaryKeyStep;
	private final PrimaryKeyPropagationStep<C, I> primaryKeyPropagationStep;
	private final IdentifierManagerStep<C, I> identifierManagerStep;
	private final MainPersisterStep<C, I> mainPersisterStep;
	private final RelationsStep<C, I> relationsStep;
	private final PolymorphismStep<C, I> polymorphismStep;
	private final AlreadyAssignedMarkerStep<C, I> alreadyAssignedMarkerStep;
	private final ParentPersistersStep<C, I> parentPersistersStep;
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	private final PersisterRegistry persisterRegistry;
	
	private PersisterBuilderContext persisterBuilderContext;
	
	public PersisterBuilderPipeline(Dialect dialect, ConnectionConfiguration connectionConfiguration, PersisterRegistry persisterRegistry) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
		this.tableLookupStep = new TableLookupStep<>();
		this.tableMappingStep = new TableMappingStep<>();
		this.identificationStep = new IdentificationStep<>();
		this.inheritanceMappingStep = new InheritanceMappingStep<>();
		this.primaryKeyStep = new PrimaryKeyStep<>();
		this.primaryKeyPropagationStep = new PrimaryKeyPropagationStep<>();
		this.identifierManagerStep = new IdentifierManagerStep<>();
		this.mainPersisterStep = new MainPersisterStep<>();
		this.relationsStep = new RelationsStep<>();
		this.polymorphismStep = new PolymorphismStep<>();
		this.alreadyAssignedMarkerStep = new AlreadyAssignedMarkerStep<>();
		this.parentPersistersStep = new ParentPersistersStep<>();
		this.persisterRegistry = persisterRegistry;
	}
	
	public ConfiguredRelationalPersister<C, I> build(EntityMappingConfiguration<C, I> entityMappingConfiguration) {
		persisterBuilderContext = PersisterBuilderContext.CURRENT.get();
		boolean isInitiator = false;
		if (persisterBuilderContext == null) {
			persisterBuilderContext = new PersisterBuilderContext(persisterRegistry);
			PersisterBuilderContext.CURRENT.set(persisterBuilderContext);
			isInitiator = true;
		}
		
		try {
			ConfiguredRelationalPersister<C, I> result = doBuild(entityMappingConfiguration);
			// making aggregate persister available for external usage
			persisterRegistry.addPersister(result);
			if (isInitiator) {
				// This if is only there to execute code below only once, at the very end of persistence graph build,
				// even if it could seem counterintuitive since it compares "isInitiator" whereas this comment talks about end of graph :
				// because persistence configuration is made with a deep-first algorithm, this code (after doBuild()) will be called at the very end.
				persisterBuilderContext.getBuildLifeCycleListeners().forEach(BuildLifeCycleListener::afterBuild);
				persisterBuilderContext.getBuildLifeCycleListeners().forEach(BuildLifeCycleListener::afterAllBuild);
			}
			return result;
		} finally {
			if (isInitiator) {
				PersisterBuilderContext.CURRENT.remove();
			}
		}
	}
	
	private ConfiguredRelationalPersister<C, I> doBuild(EntityMappingConfiguration<C, I> entityMappingConfiguration) {
		NamingConfigurationCollector namingConfigurationCollector = new NamingConfigurationCollector(entityMappingConfiguration);
		NamingConfiguration namingConfiguration = namingConfigurationCollector.collect();
		
		Table table = tableLookupStep.lookupForTable(entityMappingConfiguration, namingConfiguration.getTableNamingStrategy());
		Map<EntityMappingConfiguration, Table> entityMappingConfigurationTables = tableMappingStep.mapEntityConfigurationToTable(entityMappingConfiguration,
				table,
				namingConfiguration.getTableNamingStrategy());
		AbstractIdentification<C, I> identification = identificationStep.determineIdentification(entityMappingConfiguration);
		
		MappingPerTable<C> inheritanceMappingPerTable = inheritanceMappingStep.collectPropertiesMappingFromInheritance(entityMappingConfiguration,
				entityMappingConfigurationTables,
				dialect.getColumnBinderRegistry(),
				namingConfiguration.getColumnNamingStrategy(),
				namingConfiguration.getIndexNamingStrategy());
		PrimaryKey<?, I> primaryKey = primaryKeyStep.addIdentifyingPrimarykey(identification,
				entityMappingConfigurationTables,
				dialect.getColumnBinderRegistry(),
				namingConfiguration.getColumnNamingStrategy(),
				namingConfiguration.getIndexNamingStrategy());
		primaryKeyPropagationStep.propagate(primaryKey, inheritanceMappingPerTable, namingConfiguration.getForeignKeyNamingStrategy());
		// determining insertion manager must be done AFTER primary key addition, else it would fall into NullPointerException
		identifierManagerStep.applyIdentifierManager(identification, inheritanceMappingPerTable, identification.getIdAccessor(), dialect, connectionConfiguration);
		SimpleRelationalEntityPersister<C, I, ?> mainPersister = mainPersisterStep.buildMainPersister(
				entityMappingConfiguration,
				identification,
				inheritanceMappingPerTable,
				namingConfiguration,
				dialect,
				connectionConfiguration);
		
		relationsStep.configureRelations(mainPersister, inheritanceMappingPerTable, persisterBuilderContext, namingConfiguration, dialect, connectionConfiguration);
		
		ConfiguredRelationalPersister<C, I> result = polymorphismStep.eventuallyTransformToPolymorphicPersister(mainPersister,
				entityMappingConfiguration,
				identification,
				(Mapping<C, ?>) first(inheritanceMappingPerTable.getMappings()),
				namingConfiguration,
				dialect,
				connectionConfiguration,
				persisterBuilderContext);
		
		alreadyAssignedMarkerStep.handleAlreadyAssignedMarker(result);
		
		parentPersistersStep.buildParentPersisters(mainPersister, identification, inheritanceMappingPerTable, dialect, connectionConfiguration);
		
		return result;
	}
}
