package org.codefilarete.stalactite.engine.configurer;

import java.util.Collection;
import java.util.Map;

import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.RelationalMappingConfiguration;
import org.codefilarete.stalactite.engine.configurer.builder.DefaultPersisterBuilder;
import org.codefilarete.stalactite.engine.configurer.builder.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementCollectionRelation;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementCollectionRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.manyToOne.ManyToOneRelation;
import org.codefilarete.stalactite.engine.configurer.manyToOne.ManyToOneRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.map.EntityAsKeyAndValueMapRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.map.EntityAsKeyMapRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.map.MapRelation;
import org.codefilarete.stalactite.engine.configurer.map.MapRelationConfigurer;
import org.codefilarete.stalactite.engine.configurer.map.EntityAsValueMapRelationConfigurer;
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
	private final OneToOneRelationConfigurer<C, I, ?, ?> oneToOneRelationConfigurer;
	private final OneToManyRelationConfigurer<C, I, ?, ?> oneToManyRelationConfigurer;
	private final ManyToManyRelationConfigurer<C, ?, I, ?, ?, Collection<C>> manyToManyRelationConfigurer;
	private final ManyToOneRelationConfigurer<C, I, ?, ?> manyToOneRelationConfigurer;
	private final ElementCollectionRelationConfigurer<C, ?, I, ? extends Collection<?>> elementCollectionRelationConfigurer;
	protected final DefaultPersisterBuilder persisterBuilder;
	
	public RelationConfigurer(Dialect dialect,
							  ConnectionConfiguration connectionConfiguration,
							  ConfiguredRelationalPersister<C, I> sourcePersister,
							  NamingConfiguration namingConfiguration,
							  PersisterBuilderContext currentBuilderContext) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
		this.sourcePersister = sourcePersister;
		this.namingConfiguration = namingConfiguration;
		this.persisterBuilder = new DefaultPersisterBuilder(dialect, connectionConfiguration, currentBuilderContext.getPersisterRegistry());
		this.oneToOneRelationConfigurer = new OneToOneRelationConfigurer<>(
				this.dialect,
				this.connectionConfiguration,
				this.sourcePersister,
				this.namingConfiguration.getTableNamingStrategy(),
				this.namingConfiguration.getJoinColumnNamingStrategy(),
				this.namingConfiguration.getForeignKeyNamingStrategy(),
				this.namingConfiguration.getIndexNamingStrategy(),
				currentBuilderContext);
		this.oneToManyRelationConfigurer = new OneToManyRelationConfigurer<>(
				this.sourcePersister,
				this.dialect,
				this.connectionConfiguration,
				this.namingConfiguration.getTableNamingStrategy(),
				this.namingConfiguration.getForeignKeyNamingStrategy(),
				this.namingConfiguration.getJoinColumnNamingStrategy(),
				this.namingConfiguration.getAssociationTableNamingStrategy(),
				this.namingConfiguration.getIndexColumnNamingStrategy(),
				currentBuilderContext);
		this.manyToManyRelationConfigurer = new ManyToManyRelationConfigurer<>(
				this.sourcePersister,
				this.dialect,
				this.connectionConfiguration,
				this.namingConfiguration.getTableNamingStrategy(),
				this.namingConfiguration.getForeignKeyNamingStrategy(),
				this.namingConfiguration.getJoinColumnNamingStrategy(),
				this.namingConfiguration.getIndexColumnNamingStrategy(),
				this.namingConfiguration.getAssociationTableNamingStrategy(),
				currentBuilderContext);
		this.manyToOneRelationConfigurer = new ManyToOneRelationConfigurer<>(
				this.dialect,
				this.connectionConfiguration,
				this.sourcePersister,
				this.namingConfiguration.getTableNamingStrategy(),
				this.namingConfiguration.getJoinColumnNamingStrategy(),
				this.namingConfiguration.getForeignKeyNamingStrategy(),
				currentBuilderContext);
		this.elementCollectionRelationConfigurer = new ElementCollectionRelationConfigurer<>(
				sourcePersister,
				this.namingConfiguration.getForeignKeyNamingStrategy(),
				this.namingConfiguration.getColumnNamingStrategy(),
				this.namingConfiguration.getIndexColumnNamingStrategy(),
				this.namingConfiguration.getElementCollectionTableNamingStrategy(),
				dialect,
				this.connectionConfiguration,
				this.namingConfiguration.getIndexNamingStrategy());
	}
	
	public <TRGT, TRGTID> void configureRelations(RelationalMappingConfiguration<C> entityMappingConfiguration) {
		
		for (OneToOneRelation<C, TRGT, TRGTID> oneToOneRelation : entityMappingConfiguration.<TRGT, TRGTID>getOneToOnes()) {
			((OneToOneRelationConfigurer<C, I, TRGT, TRGTID>) oneToOneRelationConfigurer).configure(oneToOneRelation);
		}
		
		for (OneToManyRelation<C, TRGT, TRGTID, Collection<TRGT>> oneToManyRelation : entityMappingConfiguration.<TRGT, TRGTID>getOneToManys()) {
			((OneToManyRelationConfigurer<C, I, TRGT, TRGTID>) oneToManyRelationConfigurer).configure(oneToManyRelation);
		}
		
		for (ManyToManyRelation<C, TRGT, TRGTID, Collection<TRGT>, Collection<C>> manyToManyRelation : entityMappingConfiguration.<TRGT, TRGTID>getManyToManys()) {
			((ManyToManyRelationConfigurer<C, TRGT, I, TRGTID, Collection<TRGT>, Collection<C>>) manyToManyRelationConfigurer).configure(manyToManyRelation);
		}
		
		for (ManyToOneRelation<C, TRGT, TRGTID, Collection<C>> manyToOneRelation : entityMappingConfiguration.<TRGT, TRGTID>getManyToOnes()) {
			((ManyToOneRelationConfigurer<C, I, TRGT, TRGTID>) manyToOneRelationConfigurer).configure(manyToOneRelation);
		}
		
		// taking element collections into account
		for (ElementCollectionRelation<C, ?, ? extends Collection<?>> elementCollection : entityMappingConfiguration.<TRGT>getElementCollections()) {
			elementCollectionRelationConfigurer.configure((ElementCollectionRelation) elementCollection);
		}
		
		// taking map relations into account
		for (MapRelation<C, ?, ?, ? extends Map> map : entityMappingConfiguration.getMaps()) {
			if (map.getKeyEntityConfigurationProvider() != null && map.getValueEntityConfigurationProvider() != null) {
				EntityMappingConfiguration<Object, Object> keyEntityConfiguration = (EntityMappingConfiguration<Object, Object>) map.getKeyEntityConfigurationProvider().getConfiguration();
				ConfiguredRelationalPersister<Object, Object> keyEntityPersister = persisterBuilder.buildOrGiveExisting(keyEntityConfiguration);
				EntityMappingConfiguration<Object, Object> valueEntityConfiguration = (EntityMappingConfiguration<Object, Object>) map.getValueEntityConfigurationProvider().getConfiguration();
				ConfiguredRelationalPersister<Object, Object> valueEntityPersister = persisterBuilder.buildOrGiveExisting(valueEntityConfiguration);
				EntityAsKeyAndValueMapRelationConfigurer entityAsKeyMapRelationConfigurer = new EntityAsKeyAndValueMapRelationConfigurer<>(
						(MapRelation) map,
						sourcePersister,
						keyEntityPersister,
						valueEntityPersister,
						namingConfiguration.getForeignKeyNamingStrategy(),
						namingConfiguration.getColumnNamingStrategy(),
						namingConfiguration.getEntryMapTableNamingStrategy(),
						dialect,
						connectionConfiguration,
						namingConfiguration.getIndexNamingStrategy());
				entityAsKeyMapRelationConfigurer.configure();
			} else if (map.getKeyEntityConfigurationProvider() != null) {
				EntityMappingConfiguration<Object, Object> keyEntityConfiguration = (EntityMappingConfiguration<Object, Object>) map.getKeyEntityConfigurationProvider().getConfiguration();
				ConfiguredRelationalPersister<Object, Object> keyEntityPersister = persisterBuilder.buildOrGiveExisting(keyEntityConfiguration);
				EntityAsKeyMapRelationConfigurer entityAsKeyMapRelationConfigurer = new EntityAsKeyMapRelationConfigurer<>(
						(MapRelation) map,
						sourcePersister,
						keyEntityPersister,
						namingConfiguration.getForeignKeyNamingStrategy(),
						namingConfiguration.getColumnNamingStrategy(),
						namingConfiguration.getEntryMapTableNamingStrategy(),
						dialect,
						connectionConfiguration,
						namingConfiguration.getIndexNamingStrategy());
				entityAsKeyMapRelationConfigurer.configure();
			} else if (map.getValueEntityConfigurationProvider() != null) {
				EntityMappingConfiguration<Object, Object> valueEntityConfiguration = (EntityMappingConfiguration<Object, Object>) map.getValueEntityConfigurationProvider().getConfiguration();
				ConfiguredRelationalPersister<Object, Object> valueEntityPersister = persisterBuilder.buildOrGiveExisting(valueEntityConfiguration);
				EntityAsValueMapRelationConfigurer entityAsValueMapRelationConfigurer = new EntityAsValueMapRelationConfigurer<>(
						(MapRelation) map,
						sourcePersister,
						valueEntityPersister,
						namingConfiguration.getForeignKeyNamingStrategy(),
						namingConfiguration.getColumnNamingStrategy(),
						namingConfiguration.getEntryMapTableNamingStrategy(),
						dialect,
						connectionConfiguration,
						namingConfiguration.getIndexNamingStrategy());
				entityAsValueMapRelationConfigurer.configure();
			} else {
				MapRelationConfigurer<C, I, ?, ?, ? extends Map> mapRelationConfigurer = new MapRelationConfigurer<>(
						map,
						sourcePersister,
						namingConfiguration.getForeignKeyNamingStrategy(),
						namingConfiguration.getColumnNamingStrategy(),
						namingConfiguration.getEntryMapTableNamingStrategy(),
						dialect,
						connectionConfiguration,
						namingConfiguration.getIndexNamingStrategy()
				);
				mapRelationConfigurer.configure();
			}
		}
	}
}
