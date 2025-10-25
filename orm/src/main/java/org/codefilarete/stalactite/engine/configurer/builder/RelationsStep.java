package org.codefilarete.stalactite.engine.configurer.builder;

import org.codefilarete.stalactite.dsl.RelationalMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.configurer.NamingConfiguration;
import org.codefilarete.stalactite.engine.configurer.RelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.builder.InheritanceMappingStep.Mapping;
import org.codefilarete.stalactite.engine.configurer.builder.InheritanceMappingStep.MappingPerTable;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;

/***
 * Configure relations using {@link RelationConfigurer}.
 * Visit parent entity mapping configuration to also configure relations.
 * Will handle graph cycle with {@link PersisterBuilderContext#runInContext(EntityPersister, Runnable)}.
 *
 * @param <C>
 * @param <I>
 * @author Guillaume Mary
 */
public class RelationsStep<C, I> {
	
	void configureRelations(SimpleRelationalEntityPersister<C, I, ?> mainPersister,
							MappingPerTable<C> inheritanceMappingPerTable,
							PersisterBuilderContext persisterBuilderContext,
							NamingConfiguration namingConfiguration,
							Dialect dialect,
							ConnectionConfiguration connectionConfiguration) {
		RelationConfigurer<C, I> relationConfigurer = new RelationConfigurer<>(dialect, connectionConfiguration, mainPersister,
				namingConfiguration, persisterBuilderContext);
		
		persisterBuilderContext.runInContext(mainPersister, () -> {
			// registering relations on parent entities
			// WARN : this MUST BE DONE BEFORE POLYMORPHISM HANDLING because it needs them to create adhoc joins on sub entities tables
			inheritanceMappingPerTable.getMappings().stream()
					.map(Mapping::getMappingConfiguration)
					.filter(RelationalMappingConfiguration.class::isInstance)
					.map(RelationalMappingConfiguration.class::cast)
					.forEach(relationConfigurer::configureRelations);
		});
	}
}
