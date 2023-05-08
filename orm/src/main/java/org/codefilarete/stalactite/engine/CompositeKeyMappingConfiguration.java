package org.codefilarete.stalactite.engine;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.configurer.FluentCompositeKeyMappingConfigurationSupport.Inset;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.tool.collection.ReadOnlyIterator;

/**
 * Defines elements needed to configure a mapping of an embeddable class
 * 
 * @author Guillaume Mary
 */
public interface CompositeKeyMappingConfiguration<C> {
	
	Class<C> getBeanType();
	
	@SuppressWarnings("squid:S1452" /* Can't remove wildcard here because it requires to create a local generic "super" type which is forbidden */)
	CompositeKeyMappingConfiguration<? super C> getMappedSuperClassConfiguration();
	
	List<CompositeKeyLinkage> getPropertiesMapping();
	
	Collection<Inset<C, Object>> getInsets();
	
	@Nullable
	ColumnNamingStrategy getColumnNamingStrategy();
	
	/**
	 * Small contract for defining property configuration storage
	 * 
	 * @param <C> property owner type
	 * @param <O> property type
	 */
	interface CompositeKeyLinkage<C, O> {
		
		ReversibleAccessor<C, O> getAccessor();
		
		@Nullable
		String getColumnName();
		
		Class<O> getColumnType();
		
		ParameterBinder<O> getParameterBinder();
	}
}