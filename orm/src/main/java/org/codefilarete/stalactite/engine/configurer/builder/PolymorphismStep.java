package org.codefilarete.stalactite.engine.configurer.builder;

import org.codefilarete.stalactite.dsl.PolymorphismPolicy;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.configurer.AbstractIdentification;
import org.codefilarete.stalactite.engine.configurer.NamingConfiguration;
import org.codefilarete.stalactite.engine.configurer.builder.InheritanceMappingStep.Mapping;
import org.codefilarete.stalactite.engine.configurer.polymorphism.PolymorphismPersisterBuilder;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Build a {@link org.codefilarete.stalactite.engine.runtime.PolymorphicPersister} the configuration is polymorphic, else do nothing and returns
 * given {@link ConfiguredRelationalPersister}
 *
 * @param <C>
 * @param <I>
 * @author Guillaume Mary
 */
public class PolymorphismStep<C, I> {
	
	<T extends Table<T>> ConfiguredRelationalPersister<C, I> eventuallyTransformToPolymorphicPersister(ConfiguredRelationalPersister<C, I> mainPersister,
																									   EntityMappingConfiguration<C, I> entityMappingConfiguration,
																									   AbstractIdentification<C, I> identification,
																									   Mapping<C, T> mainMapping,
																									   NamingConfiguration namingConfiguration,
																									   Dialect dialect,
																									   ConnectionConfiguration connectionConfiguration,
																									   PersisterBuilderContext persisterBuilderContext) {
		ConfiguredRelationalPersister<C, I> result = mainPersister;
		// polymorphism handling
		PolymorphismPolicy<C> polymorphismPolicy = entityMappingConfiguration.getPolymorphismPolicy();
		if (polymorphismPolicy != null) {
			PolymorphismPersisterBuilder<C, I, T> polymorphismPersisterBuilder = new PolymorphismPersisterBuilder<>(
					polymorphismPolicy, identification, mainPersister, dialect.getColumnBinderRegistry(),
					mainMapping.getMapping(),
					mainMapping.getReadonlyMapping(),
					mainMapping.getReadConverters(),
					mainMapping.getWriteConverters(),
					namingConfiguration,
					persisterBuilderContext);
			result = polymorphismPersisterBuilder.build(dialect, connectionConfiguration);
		}
		
		return result;
	}
}
