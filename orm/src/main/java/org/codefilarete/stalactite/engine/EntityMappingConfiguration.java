package org.codefilarete.stalactite.engine;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.ColumnOptions.IdentifierPolicy;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.ReadOnlyIterator;

/**
 * Defines elements needed to configure a mapping of an entity class
 * 
 * @author Guillaume Mary
 */
public interface EntityMappingConfiguration<C, I> extends RelationalMappingConfiguration<C> {
	
	@Nullable
	EntityFactoryProvider<C, Table> getEntityFactoryProvider();
	
	TableNamingStrategy getTableNamingStrategy();
	
	ColumnNamingStrategy getColumnNamingStrategy();
	
	KeyMapping<C, I> getKeyMapping();
	
	EmbeddableMappingConfiguration<C> getPropertiesMapping();
	
	VersioningStrategy getOptimisticLockOption();
	
	/** Gives inheritance information if inheritance has been defined, else returns null */
	@SuppressWarnings("squid:S1452" /* Can't remove wildcard here because it requires to create a local generic "super" type which is forbidden */)
	@Nullable
	InheritanceConfiguration<? super C, I> getInheritanceConfiguration();
	
	ForeignKeyNamingStrategy getForeignKeyNamingStrategy();
	
	AssociationTableNamingStrategy getAssociationTableNamingStrategy();
	
	ElementCollectionTableNamingStrategy getElementCollectionTableNamingStrategy();
	
	JoinColumnNamingStrategy getJoinColumnNamingStrategy();
	
	/**
	 * Gives {@link ColumnNamingStrategy} for index column of one-to-many {@link List} association
	 * @return maybe null, {@link ColumnNamingStrategy#INDEX_DEFAULT} will be used instead
	 */
	ColumnNamingStrategy getIndexColumnNamingStrategy();
	
	MapEntryTableNamingStrategy getEntryMapTableNamingStrategy();
	
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
				if (!hasNext()) {
					// comply with next() method contract
					throw new NoSuchElementException();
				}
				EntityMappingConfiguration<? super C, I> result = this.next;
				this.next = org.codefilarete.tool.Nullable.nullable(this.next.getInheritanceConfiguration()).map(InheritanceConfiguration::getConfiguration).get();
				return result;
			}
		};
	}
	
	interface EntityFactoryProvider<C, T extends Table> {

		Function<Function<Column<?, ?>, Object>, C> giveEntityFactory(T table);
		
		boolean isIdentifierSetByFactory();
	}
	
	interface InheritanceConfiguration<E, I> {
		
		/** Entity configuration */
		EntityMappingConfiguration<E, I> getConfiguration();
		
		boolean isJoinTable();
		
		/** Table to be used in case of joined tables ({@link #isJoinTable()} returns true) */
		Table getTable();
	}
	
	interface KeyMapping<C, I> {
		
		ReversibleAccessor<C, I> getAccessor();
		
		boolean isSetByConstructor();
	}
	
	interface SingleKeyMapping<C, I> extends KeyMapping<C, I> {
		
		IdentifierPolicy<I> getIdentifierPolicy();
		
		@Nullable
		ColumnLinkageOptions getColumnOptions();
	}
	
	interface CompositeKeyMapping<C, I> extends KeyMapping<C, I> {
		
		@Nullable
		CompositeKeyLinkageOptions getColumnsOptions();
	}
	
	interface ColumnLinkageOptions {
		
		@Nullable
		String getColumnName();
		
	}
	
	interface CompositeKeyLinkageOptions {
		
		Map<ValueAccessPoint, String> getColumnsNames();
		
	}
}
