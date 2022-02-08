package org.gama.stalactite.persistence.engine;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

import org.codefilarete.tool.collection.ReadOnlyIterator;
import org.codefilarete.reflection.ReversibleAccessor;
import org.gama.stalactite.persistence.engine.configurer.FluentEmbeddableMappingConfigurationSupport.Inset;
import org.gama.stalactite.sql.binder.ParameterBinder;

/**
 * Defines elements needed to configure a mapping of an embeddable class
 * 
 * @author Guillaume Mary
 */
public interface EmbeddableMappingConfiguration<C> {
	
	Class<C> getBeanType();
	
	@SuppressWarnings("squid:S1452" /* Can't remove wildcard here because it requires to create a local generic "super" type which is forbidden */)
	EmbeddableMappingConfiguration<? super C> getMappedSuperClassConfiguration();
	
	List<Linkage> getPropertiesMapping();
	
	Collection<Inset<C, ?>> getInsets();
	
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
	 */
	interface Linkage<C> {
		
		<I> ReversibleAccessor<C, I> getAccessor();
		
		@Nullable
		String getColumnName();
		
		Class<?> getColumnType();
		
		ParameterBinder getParameterBinder();
		
		boolean isNullable();
		
		/** Indicates if this property is managed by entity constructor (information coming from user) */
		boolean isSetByConstructor();
	}
}
