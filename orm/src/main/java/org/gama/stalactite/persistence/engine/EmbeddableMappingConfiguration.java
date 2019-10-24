package org.gama.stalactite.persistence.engine;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

import org.gama.reflection.IReversibleAccessor;
import org.gama.stalactite.sql.binder.ParameterBinder;
import org.gama.stalactite.persistence.engine.FluentEmbeddableMappingConfigurationSupport.AbstractInset;

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
	
	<I extends AbstractInset<C, ?>> Collection<I> getInsets();
	
	ColumnNamingStrategy getColumnNamingStrategy();
	
	/**
	 * Small constract for defining property configuration storage
	 * 
	 * @param <C> property owner type
	 */
	interface Linkage<C> {
		
		<I> IReversibleAccessor<C, I> getAccessor();
		
		@Nullable
		String getColumnName();
		
		Class<?> getColumnType();
		
		ParameterBinder getParameterBinder();
		
		boolean isNullable();
	}
}
