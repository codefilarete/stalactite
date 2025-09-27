package org.codefilarete.stalactite.engine;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.configurer.FluentEmbeddableMappingConfigurationSupport.Inset;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry.EnumBindType;
import org.codefilarete.tool.collection.ReadOnlyIterator;
import org.codefilarete.tool.function.Converter;

/**
 * Defines elements needed to configure a mapping of an embeddable class
 * 
 * @author Guillaume Mary
 */
public interface EmbeddableMappingConfiguration<C> {
	
	Class<C> getBeanType();
	
	@SuppressWarnings("squid:S1452" /* Can't remove wildcard here because it requires to create a local generic "super" type which is forbidden */)
	@Nullable
	EmbeddableMappingConfiguration<? super C> getMappedSuperClassConfiguration();
	
	List<Linkage> getPropertiesMapping();
	
	Collection<Inset<C, Object>> getInsets();
	
	@Nullable
	ColumnNamingStrategy getColumnNamingStrategy();
	
	/**
	 * @return an iterable for all inheritance configurations, including this
	 */
	default Iterable<EmbeddableMappingConfiguration> inheritanceIterable() {
		
		return () -> new ReadOnlyIterator<EmbeddableMappingConfiguration>() {
			
			private EmbeddableMappingConfiguration next = EmbeddableMappingConfiguration.this;
			
			@Override
			public boolean hasNext() {
				return next != null;
			}
			
			@Override
			public EmbeddableMappingConfiguration next() {
				if (!hasNext()) {
					// comply with next() method contract
					throw new NoSuchElementException();
				}
				EmbeddableMappingConfiguration result = this.next;
				this.next = this.next.getMappedSuperClassConfiguration();
				return result;
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
		
		ReversibleAccessor<C, O> getAccessor();
		
		@Nullable
		Field getField();
		
		@Nullable
		String getColumnName();
		
		Class<O> getColumnType();
		
		@Nullable
		ParameterBinder<Object> getParameterBinder();
		
		/**
		 * Gives the choice made by the user to define how to bind enum values: by name or ordinal.
		 * @return null if no info was given
		 */
		@Nullable
		EnumBindType getEnumBindType();
		
		boolean isNullable();
		
		/** Indicates if this property is managed by entity constructor (information coming from user) */
		boolean isSetByConstructor();
		
		/** Indicates if this property should not be writable to database */
		boolean isReadonly();
		
		@Nullable
		String getExtraTableName();
		
		@Nullable
		Converter<?, O> getReadConverter();
		
		@Nullable
		Converter<O, ?> getWriteConverter();
	}
}