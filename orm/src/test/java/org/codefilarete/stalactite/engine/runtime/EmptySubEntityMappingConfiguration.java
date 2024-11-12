package org.codefilarete.stalactite.engine.runtime;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.codefilarete.stalactite.engine.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.engine.PolymorphismPolicy;
import org.codefilarete.stalactite.engine.SubEntityMappingConfiguration;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementCollectionRelation;
import org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.map.MapRelation;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneRelation;

/**
 * Simple support for a "no-operation" of a {@link SubEntityMappingConfiguration}
 * 
 * @param <C> sub-entity type
 * @author Guillaume Mary
 */
class EmptySubEntityMappingConfiguration<C> implements SubEntityMappingConfiguration<C> {
	
	private Class<C> entityType;
	
	EmptySubEntityMappingConfiguration(Class<C> entityType) {
		this.entityType = entityType;
	}
	
	@Override
	public Class<C> getEntityType() {
		return entityType;
	}
	
	@Override
	public EmbeddableMappingConfiguration<C> getPropertiesMapping() {
		return null;
	}
	
	@Override
	public <TRGT, TRGTID> List<OneToOneRelation<C, TRGT, TRGTID>> getOneToOnes() {
		return Collections.emptyList();
	}
	
	@Override
	public <TRGT, TRGTID> List<OneToManyRelation<C, TRGT, TRGTID, ? extends Collection<TRGT>>> getOneToManys() {
		return Collections.emptyList();
	}
	
	@Override
	public <TRGT, TRGTID> List<ManyToManyRelation<C, TRGT, TRGTID, Collection<TRGT>, Collection<C>>> getManyToManyRelations() {
		return Collections.emptyList();
	}
	
	@Override
	public List<ElementCollectionRelation<C, ?, ? extends Collection>> getElementCollections() {
		return Collections.emptyList();
	}
	
	@Override
	public List<MapRelation<C, ?, ?, ? extends Map>> getMaps() {
		return Collections.emptyList();
	}
	
	@Override
	public PolymorphismPolicy<C> getPolymorphismPolicy() {
		return null;
	}
}
