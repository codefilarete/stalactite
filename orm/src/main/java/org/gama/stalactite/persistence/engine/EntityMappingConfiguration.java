package org.gama.stalactite.persistence.engine;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.gama.lang.Nullable;
import org.gama.lang.collection.ReadOnlyIterator;
import org.gama.reflection.ReversibleAccessor;
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.configurer.CascadeMany;
import org.gama.stalactite.persistence.engine.configurer.CascadeOne;
import org.gama.stalactite.persistence.engine.configurer.ElementCollectionLinkage;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * Defines elements needed to configure a mapping of an entity class
 * 
 * @author Guillaume Mary
 */
public interface EntityMappingConfiguration<C, I> {
	
	Class<C> getEntityType();
	
	EntityFactoryProvider<C> getEntityFactoryProvider();
	
	TableNamingStrategy getTableNamingStrategy();
	
	IdentifierPolicy getIdentifierPolicy();
	
	ReversibleAccessor<C, I> getIdentifierAccessor();
	
	EmbeddableMappingConfiguration<C> getPropertiesMapping();
	
	<TRGT, TRGTID> List<CascadeOne<C, TRGT, TRGTID>> getOneToOnes();
	
	<TRGT, TRGTID> List<CascadeMany<C, TRGT, TRGTID, ? extends Collection<TRGT>>> getOneToManys();
	
	List<ElementCollectionLinkage<C, ?, ? extends Collection>> getElementCollections();
		
	VersioningStrategy getOptimisticLockOption();
	
	/** Gives inheritance informations if inheritance has been defined, else returns null */
	@SuppressWarnings("squid:S1452" /* Can't remove wildcard here because it requires to create a local generic "super" type which is forbidden */)
	@javax.annotation.Nullable
	InheritanceConfiguration<? super C, I> getInheritanceConfiguration();
	
	ForeignKeyNamingStrategy getForeignKeyNamingStrategy();
	
	AssociationTableNamingStrategy getAssociationTableNamingStrategy();
	
	ElementCollectionTableNamingStrategy getElementCollectionTableNamingStrategy();
	
	ColumnNamingStrategy getJoinColumnNamingStrategy();
	
	/**
	 * Gives {@link ColumnNamingStrategy} for index column of one-to-many {@link List} association
	 * @return maybe null, {@link ColumnNamingStrategy#INDEX_DEFAULT} will be used instead
	 */
	ColumnNamingStrategy getIndexColumnNamingStrategy();
	
	PolymorphismPolicy<C> getPolymorphismPolicy();
	
	/**
	 * @return an iterable for all inheritance configurations, including this
	 */
	default Iterable<EntityMappingConfiguration<? super C, I>> inheritanceIterable() {
		
		return () -> new ReadOnlyIterator<EntityMappingConfiguration<? super C, I>>() {
			
			private EntityMappingConfiguration<? super C, I> next = EntityMappingConfiguration.this;
			
			@Override
			public boolean hasNext() {
				return next != null;
			}
			
			@Override
			public EntityMappingConfiguration<? super C, I> next() {
				EntityMappingConfiguration<? super C, I> result = this.next;
				this.next = Nullable.nullable(this.next.getInheritanceConfiguration()).map(InheritanceConfiguration::getConfiguration).get();
				return result;
			}
		};
	}

	interface EntityFactoryProvider<C> {

		Function<Function<Column, Object>, C> giveEntityFactory(Table table);
	}
	
	interface InheritanceConfiguration<E, I> {
		
		/** Entity configuration */
		EntityMappingConfiguration<E, I> getConfiguration();
		
		boolean isJoinTable();
		
		/** Table to be used in case of joined tables ({@link #isJoinTable()} returns true) */
		Table getTable();
	}
}
