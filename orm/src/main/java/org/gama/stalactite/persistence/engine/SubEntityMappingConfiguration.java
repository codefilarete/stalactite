package org.gama.stalactite.persistence.engine;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.gama.stalactite.persistence.engine.configurer.CascadeMany;
import org.gama.stalactite.persistence.engine.configurer.ElementCollectionLinkage;
import org.gama.stalactite.persistence.engine.configurer.CascadeOne;
import org.gama.stalactite.persistence.structure.Column;

/**
 * Configuration dedicated to polymorphism : in such cases sub-entities don't define identifier policy because it is done by the polymorphic
 * type itself. Hence it is all what an EntityMappingConfiguraiton can do, without identification information. 
 * 
 * @author Guillaume Mary
 */
public interface SubEntityMappingConfiguration<C> {
	
	Class<C> getEntityType();
	
	@SuppressWarnings("squid:S1452")
	Function<Function<Column, Object>, C> getEntityFactory();
	
	EmbeddableMappingConfiguration<C> getPropertiesMapping();
	
	List<CascadeOne<C, ?, ?>> getOneToOnes();
	
	List<CascadeMany<C, ?, ?, ? extends Collection>> getOneToManys();
	
	List<ElementCollectionLinkage<C, ?, ? extends Collection>> getElementCollections();
	
	PolymorphismPolicy<C> getPolymorphismPolicy();
}
