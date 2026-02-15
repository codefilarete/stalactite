package org.codefilarete.stalactite.engine.configurer.builder;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.dsl.RelationalMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.configurer.embeddable.Inset;
import org.codefilarete.stalactite.engine.configurer.NamingConfiguration;
import org.codefilarete.stalactite.engine.configurer.RelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.builder.InheritanceMappingStep.Mapping;
import org.codefilarete.stalactite.engine.configurer.builder.InheritanceMappingStep.MappingPerTable;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementCollectionRelation;
import org.codefilarete.stalactite.engine.configurer.manyToOne.ManyToOneRelation;
import org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.map.MapRelation;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneRelation;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.tool.collection.Iterables;

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
			configuredInheritedRelations(inheritanceMappingPerTable, relationConfigurer);
			
			configuredEmbeddableRelations(inheritanceMappingPerTable, relationConfigurer, mainPersister.getClassToPersist());
		});
	}
	
	private void configuredInheritedRelations(MappingPerTable<C> inheritanceMappingPerTable, RelationConfigurer<C, I> relationConfigurer) {
		inheritanceMappingPerTable.getMappings().stream()
				.map(Mapping::getMappingConfiguration)
				.filter(RelationalMappingConfiguration.class::isInstance)
				.map(RelationalMappingConfiguration.class::cast)
				.forEach(relationConfigurer::configureRelations);
	}
	
	private <D> void configuredEmbeddableRelations(MappingPerTable<C> inheritanceMappingPerTable, RelationConfigurer<C, I> relationConfigurer, Class<C> rootEntityType) {
		inheritanceMappingPerTable.getMappings().stream()
				.map(Mapping::getMappingConfiguration)
				.filter(EntityMappingConfiguration.class::isInstance)
				.map(EntityMappingConfiguration.class::cast)
				.map(EntityMappingConfiguration::getPropertiesMapping)
				// we the configurations of all embedded bean through the configuration insets
				.flatMap(conf -> ((Collection<Inset<C, D>>) conf.getInsets()).stream()
						// we must check the inherited class of the embeddable because they may also define some relations
						.flatMap(inset -> Iterables.stream(inset.getConfigurationProvider().getConfiguration().inheritanceIterable())
								.map(confInHierarchy -> new ShiftedRelationalMappingConfiguration<>(rootEntityType, confInHierarchy, inset))))
				.forEach(shiftedRelationalMappingConfiguration -> {
					relationConfigurer.configureRelations((RelationalMappingConfiguration<C>) shiftedRelationalMappingConfiguration);
				});
	}
	
	/**
	 * A {@link RelationalMappingConfiguration} which relations are prepredended by a prefix that is the accessor to
	 * the embeddable which is embedded in the main entity.
	 *
	 * @param <D>
	 * @author Guillaume Mary
	 */
	private class ShiftedRelationalMappingConfiguration<D> implements RelationalMappingConfiguration<C> {
		private final Class<C> rootEntityType;
		private final RelationalMappingConfiguration<D> configuration;
		private final Inset<C, D> inset;
		
		public ShiftedRelationalMappingConfiguration(Class<C> rootEntityType, RelationalMappingConfiguration<D> configuration, Inset<C, D> inset) {
			this.rootEntityType = rootEntityType;
			this.configuration = configuration;
			this.inset = inset;
		}
		
		@Override
		public Class<C> getEntityType() {
			return rootEntityType;
		}
		
		@Override
		public <TRGT, TRGTID> List<OneToOneRelation<C, TRGT, TRGTID>> getOneToOnes() {
			return configuration.<TRGT, TRGTID>getOneToOnes().stream()
					.map(oneToOne -> oneToOne.embedInto(inset.getAccessor()))
					.collect(Collectors.toList());
		}
		
		@Override
		public <TRGT, TRGTID> List<OneToManyRelation<C, TRGT, TRGTID, Collection<TRGT>>> getOneToManys() {
			return configuration.<TRGT, TRGTID>getOneToManys().stream()
					.map(oneToMany -> oneToMany.embedInto(inset.getAccessor(), configuration.getEntityType()))
					.collect(Collectors.toList());
		}
		
		@Override
		public <TRGT, TRGTID> List<ManyToManyRelation<C, TRGT, TRGTID, Collection<TRGT>, Collection<C>>> getManyToManys() {
			return configuration.<TRGT, TRGTID>getManyToManys().stream()
					.map(manyToMany -> manyToMany.embedInto(inset.getAccessor(), configuration.getEntityType()))
					.collect(Collectors.toList());
		}
		
		@Override
		public <TRGT, TRGTID> List<ManyToOneRelation<C, TRGT, TRGTID, Collection<C>>> getManyToOnes() {
			return configuration.<TRGT, TRGTID>getManyToOnes().stream()
					.map(manyToOne -> manyToOne.embedInto(inset.getAccessor()))
					.collect(Collectors.toList());
		}
		
		@Override
		public <TRGT> List<ElementCollectionRelation<C, TRGT, ? extends Collection<TRGT>>> getElementCollections() {
			return configuration.<TRGT>getElementCollections().stream()
					.map(collectionRelation -> collectionRelation.embedInto(inset.getAccessor(), configuration.getEntityType()))
					.collect(Collectors.toList());
		}
		
		@Override
		public List<MapRelation<C, ?, ?, ? extends Map>> getMaps() {
			return Collections.emptyList();
		}
	}
}
