package org.codefilarete.stalactite.engine;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementCollectionRelation;
import org.codefilarete.stalactite.engine.configurer.manytomany.ManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.map.MapRelation;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneRelation;

/**
 * Contract that defines a mapping with relations 
 * 
 * @author Guillaume Mary
 */
public interface RelationalMappingConfiguration<C> {
	
	Class<C> getEntityType();
	
	<TRGT, TRGTID> List<OneToOneRelation<C, TRGT, TRGTID>> getOneToOnes();
	
	<TRGT, TRGTID> List<OneToManyRelation<C, TRGT, TRGTID, Collection<TRGT>>> getOneToManys();
	
	<TRGT, TRGTID> List<ManyToManyRelation<C, TRGT, TRGTID, Collection<TRGT>, Collection<C>>> getManyToManyRelations();
	
	List<ElementCollectionRelation<C, ?, ? extends Collection>> getElementCollections();
	
	List<MapRelation<C, ?, ?, ? extends Map>> getMaps();
	
}
