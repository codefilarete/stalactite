package org.codefilarete.stalactite.engine.configurer;

import java.util.Collection;
import java.util.Map;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.EntityPersister.EntityCriteria;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.RelationalMappingConfiguration;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl.PostInitializer;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementCollectionRelation;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementCollectionRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.map.EntityAsKeyAndValueMapRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.map.EntityAsKeyMapRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.map.MapRelation;
import org.codefilarete.stalactite.engine.configurer.map.MapRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.map.ValueAsKeyMapRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneRelationConfigurer;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;

/**
 * Main class that manage the configurations of all type of relations:
 * - one-to-one
 * - one-to-many
 * - many-to-many
 * - element collection
 * 
 * @author Guillaume Mary
 */
public class RelationConfigurer<C, I> {
	
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	private final ConfiguredRelationalPersister<C, I> sourcePersister;
	private final NamingConfiguration namingConfiguration;
	private final OneToOneRelationConfigurer<C, I> oneToOneRelationConfigurer;
	private final OneToManyRelationConfigurer<C, ?, I, ?> oneToManyRelationConfigurer;
	private final ManyToManyRelationConfigurer<C, ?, I, ?, ?, Collection<C>> manyRelationConfigurer;
	private final ElementCollectionRelationConfigurer<C, ?, I, ? extends Collection<?>> elementCollectionRelationConfigurer;
	
	public RelationConfigurer(Dialect dialect,
							  ConnectionConfiguration connectionConfiguration,
							  ConfiguredRelationalPersister<C, I> sourcePersister,
							  NamingConfiguration namingConfiguration) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
		this.sourcePersister = sourcePersister;
		this.namingConfiguration = namingConfiguration;
		this.oneToOneRelationConfigurer = new OneToOneRelationConfigurer<>(
				this.dialect,
				this.connectionConfiguration,
				this.sourcePersister,
				this.namingConfiguration.getJoinColumnNamingStrategy(),
				this.namingConfiguration.getForeignKeyNamingStrategy());
		this.oneToManyRelationConfigurer = new OneToManyRelationConfigurer<>(
				this.sourcePersister,
				this.dialect,
				this.connectionConfiguration,
				this.namingConfiguration.getForeignKeyNamingStrategy(),
				this.namingConfiguration.getJoinColumnNamingStrategy(),
				this.namingConfiguration.getAssociationTableNamingStrategy(),
				this.namingConfiguration.getIndexColumnNamingStrategy());
		this.manyRelationConfigurer = new ManyToManyRelationConfigurer<>(
				this.sourcePersister,
				this.dialect,
				this.connectionConfiguration,
				this.namingConfiguration.getForeignKeyNamingStrategy(),
				this.namingConfiguration.getJoinColumnNamingStrategy(),
				this.namingConfiguration.getIndexColumnNamingStrategy(),
				this.namingConfiguration.getAssociationTableNamingStrategy()
		);
		this.elementCollectionRelationConfigurer = new ElementCollectionRelationConfigurer<>(
				sourcePersister,
				this.namingConfiguration.getForeignKeyNamingStrategy(),
				this.namingConfiguration.getColumnNamingStrategy(),
				this.namingConfiguration.getElementCollectionTableNamingStrategy(),
				dialect,
				this.connectionConfiguration);
	}
	
	public <TRGT, TRGTID> void configureRelations(RelationalMappingConfiguration<C> entityMappingConfiguration) {
		
		for (OneToOneRelation<C, TRGT, TRGTID> oneToOneRelation : entityMappingConfiguration.<TRGT, TRGTID>getOneToOnes()) {
			oneToOneRelationConfigurer.configure(oneToOneRelation);
		}
		for (OneToManyRelation<C, TRGT, TRGTID, Collection<TRGT>> oneToManyRelation : entityMappingConfiguration.<TRGT, TRGTID>getOneToManys()) {
			((OneToManyRelationConfigurer<C, TRGT, I, TRGTID>) oneToManyRelationConfigurer).configure(oneToManyRelation);
		}
		
		for (ManyToManyRelation<C, TRGT, TRGTID, Collection<TRGT>, Collection<C>> manyToManyRelation : entityMappingConfiguration.<TRGT, TRGTID>getManyToManyRelations()) {
			((ManyToManyRelationConfigurer<C, TRGT, I, TRGTID, Collection<TRGT>, Collection<C>>) manyRelationConfigurer).configure(manyToManyRelation);
		}
		
		// taking element collections into account
		for (ElementCollectionRelation<C, ?, ? extends Collection<?>> elementCollection : entityMappingConfiguration.<TRGT>getElementCollections()) {
			elementCollectionRelationConfigurer.configure((ElementCollectionRelation) elementCollection);
		}
		
		// taking map relations into account
		for (MapRelation<C, ?, ?, ? extends Map> map : entityMappingConfiguration.getMaps()) {
			if (map.getKeyEntityConfigurationProvider() != null && map.getValueEntityConfigurationProvider() != null) {
				EntityMappingConfiguration<Object, Object> keyEntityConfiguration = (EntityMappingConfiguration<Object, Object>) map.getKeyEntityConfigurationProvider().getConfiguration();
				ConfiguredRelationalPersister<Object, Object> keyEntityPersister = new PersisterBuilderImpl<>(keyEntityConfiguration)
						.build(dialect, connectionConfiguration, null);
				EntityMappingConfiguration<Object, Object> valueEntityConfiguration = (EntityMappingConfiguration<Object, Object>) map.getValueEntityConfigurationProvider().getConfiguration();
				ConfiguredRelationalPersister<Object, Object> valueEntityPersister = new PersisterBuilderImpl<>(valueEntityConfiguration)
						.build(dialect, connectionConfiguration, null);
				EntityAsKeyAndValueMapRelationConfigurer entityAsKeyMapRelationConfigurer = new EntityAsKeyAndValueMapRelationConfigurer<>(
						(MapRelation) map,
						sourcePersister,
						keyEntityPersister,
						valueEntityPersister,
						namingConfiguration.getForeignKeyNamingStrategy(),
						namingConfiguration.getColumnNamingStrategy(),
						namingConfiguration.getEntryMapTableNamingStrategy(),
						dialect,
						connectionConfiguration);
				entityAsKeyMapRelationConfigurer.configure();
			} else if (map.getKeyEntityConfigurationProvider() != null) {
				EntityMappingConfiguration<Object, Object> keyEntityConfiguration = (EntityMappingConfiguration<Object, Object>) map.getKeyEntityConfigurationProvider().getConfiguration();
				ConfiguredRelationalPersister<Object, Object> keyEntityPersister = new PersisterBuilderImpl<>(keyEntityConfiguration)
						.build(dialect, connectionConfiguration, null);
				EntityAsKeyMapRelationConfigurer entityAsKeyMapRelationConfigurer = new EntityAsKeyMapRelationConfigurer<>(
						(MapRelation) map,
						sourcePersister,
						keyEntityPersister,
						namingConfiguration.getForeignKeyNamingStrategy(),
						namingConfiguration.getColumnNamingStrategy(),
						namingConfiguration.getEntryMapTableNamingStrategy(),
						dialect,
						connectionConfiguration);
				entityAsKeyMapRelationConfigurer.configure();
			} else if (map.getValueEntityConfigurationProvider() != null) {
				EntityMappingConfiguration<Object, Object> valueEntityConfiguration = (EntityMappingConfiguration<Object, Object>) map.getValueEntityConfigurationProvider().getConfiguration();
				ConfiguredRelationalPersister<Object, Object> valueEntityPersister = new PersisterBuilderImpl<>(valueEntityConfiguration)
						.build(dialect, connectionConfiguration, null);
				ValueAsKeyMapRelationConfigurer valueAsKeyMapRelationConfigurer = new ValueAsKeyMapRelationConfigurer<>(
						(MapRelation) map,
						sourcePersister,
						valueEntityPersister,
						namingConfiguration.getForeignKeyNamingStrategy(),
						namingConfiguration.getColumnNamingStrategy(),
						namingConfiguration.getEntryMapTableNamingStrategy(),
						dialect,
						connectionConfiguration);
				valueAsKeyMapRelationConfigurer.configure();
			} else {
				MapRelationConfigurer<C, I, ?, ?, ? extends Map> mapRelationConfigurer = new MapRelationConfigurer<>(
						map,
						sourcePersister,
						namingConfiguration.getForeignKeyNamingStrategy(),
						namingConfiguration.getColumnNamingStrategy(),
						namingConfiguration.getEntryMapTableNamingStrategy(),
						dialect,
						connectionConfiguration
				);
				mapRelationConfigurer.configure();
			}
		}
	}
	
	/**
	 * Small container aimed at lazily registering a relation into source persister so it can be targeted by {@link EntityCriteria}
	 * 
	 * @param <TRGT> relation entity type
	 */
	public static class GraphLoadingRelationRegisterer<C, I, TRGT> extends PostInitializer<TRGT> {
		
		private final ReversibleAccessor<C, ?> targetEntityAccessor;
		private final Class<C> sourceEntityType;
		
		public GraphLoadingRelationRegisterer(Class<TRGT> targetEntityType, ReversibleAccessor<C, ?> targetEntityAccessor, Class<C> sourceEntityType) {
			super(targetEntityType);
			this.targetEntityAccessor = targetEntityAccessor;
			this.sourceEntityType = sourceEntityType;
		}
		
		@Override
		public void consume(ConfiguredRelationalPersister<TRGT, ?> targetPersister) {
			// we must dynamically retrieve the persister into the registry because sourcePersister might not be the
			// final / registered one in particular in case of polymorphism
			PersisterRegistry persisterRegistry = PersisterBuilderContext.CURRENT.get().getPersisterRegistry();
			ConfiguredRelationalPersister<C, I> registeredSourcePersister = ((ConfiguredRelationalPersister<C, I>) persisterRegistry.getPersister(sourceEntityType));
			registeredSourcePersister.registerRelation(targetEntityAccessor, targetPersister);
			
		}
	}
}
