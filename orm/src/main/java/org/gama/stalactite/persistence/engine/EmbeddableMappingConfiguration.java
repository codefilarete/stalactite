package org.gama.stalactite.persistence.engine;

import java.util.Collection;
import java.util.List;

import org.gama.reflection.PropertyAccessor;
import org.gama.sql.binder.ParameterBinder;
import org.gama.stalactite.persistence.engine.FluentEmbeddableMappingConfigurationSupport.AbstractInset;

/**
 * Defines elements needed to configure a mapping of an embeddable class
 * 
 * @author Guillaume Mary
 */
public interface EmbeddableMappingConfiguration<C> {
	
	EmbeddableMappingConfiguration<? super C> getMappedSuperClassConfiguration();
	
	List<Linkage> getPropertiesMapping();
	
	Collection<AbstractInset<C, ?>> getInsets();
	
	ColumnNamingStrategy getColumnNamingStrategy();
	
	/**
	 * Small constract for defining property storage
	 * 
	 * @param <T> property owner type
	 */
	interface Linkage<T> {
		
		<I> PropertyAccessor<T, I> getAccessor();
		
		String getColumnName();
		
		Class<?> getColumnType();
		
		ParameterBinder getParameterBinder();
	}
}
