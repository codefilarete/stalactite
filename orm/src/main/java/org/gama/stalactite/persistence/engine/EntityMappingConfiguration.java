package org.gama.stalactite.persistence.engine;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.gama.lang.collection.ReadOnlyIterator;
import org.gama.reflection.IReversibleAccessor;
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.builder.CascadeMany;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Defines elements needed to configure a mapping of an entity class
 * 
 * @author Guillaume Mary
 */
public interface EntityMappingConfiguration<C, I> {
	
	Class<C> getEntityType();
	
	@SuppressWarnings("squid:S1452")
	Function<Function<Column, Object>, C> getEntityFactory();
	
	TableNamingStrategy getTableNamingStrategy();
	
	IdentifierPolicy getIdentifierPolicy();
	
	IReversibleAccessor<C, I> getIdentifierAccessor();
	
	EmbeddableMappingConfiguration<C> getPropertiesMapping();
	
	List<CascadeOne<C, ?, ?>> getOneToOnes();
	
	List<CascadeMany<C, ?, ?, ? extends Collection>> getOneToManys();
	
	VersioningStrategy getOptimisticLockOption();
	
	@SuppressWarnings("squid:S1452" /* Can't remove wildcard here because it requires to create a local generic "super" type which is forbidden */)
	EntityMappingConfiguration<? super C, I> getInheritanceConfiguration();
	
	boolean isJoinTable();
	
	Table getInheritanceTable();
	
	ForeignKeyNamingStrategy getForeignKeyNamingStrategy();
	
	AssociationTableNamingStrategy getAssociationTableNamingStrategy();
	
	ColumnNamingStrategy getJoinColumnNamingStrategy();
	
	PolymorphismPolicy getPolymorphismPolicy();
	
	/**
	 * @return an iterable for all inheritance configurations, including this
	 */
	default Iterable<EntityMappingConfiguration> inheritanceIterable() {
		
		return () -> new ReadOnlyIterator<EntityMappingConfiguration>() {
			
			private EntityMappingConfiguration next = EntityMappingConfiguration.this;
			
			@Override
			public boolean hasNext() {
				return next != null;
			}
			
			@Override
			public EntityMappingConfiguration next() {
				EntityMappingConfiguration result = this.next;
				this.next = this.next.getInheritanceConfiguration();
				return result;
			}
		};
	}
}
