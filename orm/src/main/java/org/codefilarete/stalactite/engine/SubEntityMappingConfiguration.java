package org.codefilarete.stalactite.engine;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementCollectionRelation;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneRelation;

/**
 * Configuration dedicated to polymorphism : in such cases sub-entities don't define identifier policy because it is done by the polymorphic
 * type itself. Hence, it is all what an EntityMappingConfiguration can do, without identification information. 
 * 
 * @author Guillaume Mary
 */
public interface SubEntityMappingConfiguration<C> {
	
	Class<C> getEntityType();
	
	EmbeddableMappingConfiguration<C> getPropertiesMapping();
	
	<TRGT, TRGTID> List<OneToOneRelation<C, TRGT, TRGTID>> getOneToOnes();
	
	<TRGT, TRGTID> List<OneToManyRelation<C, TRGT, TRGTID, ? extends Collection<TRGT>>> getOneToManys();
	
	List<ElementCollectionRelation<C, ?, ? extends Collection>> getElementCollections();
	
	@Nullable
	PolymorphismPolicy<C> getPolymorphismPolicy();
}
