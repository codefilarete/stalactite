package org.codefilarete.stalactite.dsl.embeddable;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.MappableSuperClassConfiguration;
import org.codefilarete.stalactite.dsl.RelationalMappingConfiguration;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.UniqueConstraintNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.embeddable.Inset;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry.EnumBindType;
import org.codefilarete.tool.collection.ReadOnlyIterator;
import org.codefilarete.tool.function.Converter;

/**
 * Defines elements needed to configure a mapping of an embeddable class
 * 
 * @author Guillaume Mary
 */
public interface EmbeddableMappingConfiguration<C> extends MappableSuperClassConfiguration<C>, RelationalMappingConfiguration<C> {
	
	Class<C> getBeanType();
	
	/**
	 * Overridden to specialize the result type : because {@link EmbeddableMappingConfiguration} can't inherit from
	 * {@link org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration} (else they would be entity configuration),
	 * the result type can onbly be made of {@link EmbeddableMappingConfiguration}.
	 * 
	 * @return the configuration of a parent type
	 */
	@SuppressWarnings("squid:S1452" /* Can't remove wildcard here because it requires to create a local generic "super" type which is forbidden */)
	@Nullable
	EmbeddableMappingConfiguration<? super C> getMappedSuperClassConfiguration();
	
	List<Linkage> getPropertiesMapping();
	
	<O> Collection<Inset<C, O>> getInsets();
	
	@Nullable
	ColumnNamingStrategy getColumnNamingStrategy();
	
	@Nullable
	UniqueConstraintNamingStrategy getUniqueConstraintNamingStrategy();
	
	/**
	 * Returns an {@link Iterable} over all mapped super class configurations.
	 * Overridden for result typ accuracy : because {@link EmbeddableMappingConfiguration} can't inherit from
	 * {@link org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration} (else they would be entity configuration),
	 * result type can onbly be made of {@link EmbeddableMappingConfiguration}.
	 * 
	 * @return an iterable for all inheritance configurations, including this
	 */
	@Override
	default Iterable<EmbeddableMappingConfiguration<C>> inheritanceIterable() {
		// overridden to specialize the result
		Iterable<? extends MappableSuperClassConfiguration<?>> superIterable = MappableSuperClassConfiguration.super.inheritanceIterable();
		return () -> new ReadOnlyIterator<EmbeddableMappingConfiguration<C>>() {
			private final Iterator<? extends MappableSuperClassConfiguration<?>> delegate = superIterable.iterator();
			
			@Override
			public boolean hasNext() {
				return delegate.hasNext();
			}
			
			@Override
			public EmbeddableMappingConfiguration<C> next() {
				return (EmbeddableMappingConfiguration<C>) delegate.next();
			}
		};
	}
	
	/**
	 * Small contract for defining property configuration storage
	 * 
	 * @param <C> property owner type
	 * @param <O> property type
	 */
	interface Linkage<C, O> {
		
		ReadWritePropertyAccessPoint<C, O> getAccessor();
		
		@Nullable
		String getFieldName();
		
		@Nullable
		String getColumnName();
		
		@Nullable
		Size getColumnSize();
		
		Class<O> getColumnType();
		
		@Nullable
		ParameterBinder<Object> getParameterBinder();
		
		/**
		 * Gives the choice made by the user to define how to bind enum values: by name or ordinal.
		 * @return null if no info was given
		 */
		@Nullable
		EnumBindType getEnumBindType();
		
		/**
		 *
		 * @return null if the user didn't mention nullability, then we'll make a choice for him according to property type
		 */
		@Nullable
		Boolean isNullable();
		
		boolean isUnique();
		
		/** Indicates if this property is managed by entity constructor (information coming from user) */
		boolean isSetByConstructor();
		
		/** Indicates if this property should not be writable to database */
		boolean isReadonly();
		
		@Nullable
		Table getExtraTable();
		
		@Nullable
		Converter<?, O> getReadConverter();
		
		@Nullable
		Converter<O, ?> getWriteConverter();
	}
}
