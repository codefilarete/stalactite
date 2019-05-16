package org.gama.stalactite.persistence.engine;

import java.util.Collection;
import java.util.List;

import org.gama.reflection.IReversibleAccessor;
import org.gama.stalactite.persistence.engine.builder.CascadeMany;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Defines elements needed to configure a mapping of an entity class
 * 
 * @author Guillaume Mary
 */
public interface EntityMappingConfiguration<C, I> {
	
	Class<C> getPersistedClass();
	
	TableNamingStrategy getTableNamingStrategy();
	
	IReversibleAccessor getIdentifierAccessor();
	
	IdentifierInsertionManager<C, I> getIdentifierInsertionManager();
	
	EmbeddableMappingConfiguration<C> getPropertiesMapping();
	
	List<CascadeOne<C, ?, ?>> getOneToOnes();
	
	List<CascadeMany<C, ?, ?, ? extends Collection>> getOneToManys();
	
	VersioningStrategy getOptimisticLockOption();
	
	EntityMappingConfiguration<? super C, I> getInheritanceConfiguration();
	
	boolean isJoinTable();
	
	Table getInheritanceTable();
	
	ForeignKeyNamingStrategy getForeignKeyNamingStrategy();
	
	AssociationTableNamingStrategy getAssociationTableNamingStrategy();
	
}
