package org.codefilarete.stalactite.dsl.key;

import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.FluentCompositeKeyMappingConfigurationSupport.Inset;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry.EnumBindType;

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
	
	<O> Collection<Inset<C, O>> getInsets();
	
	@Nullable
	ColumnNamingStrategy getColumnNamingStrategy();
	
	/**
	 * Small contract that defines composite key configuration storage
	 * 
	 * @param <C> property owner type
	 * @param <O> property type
	 */
	interface CompositeKeyLinkage<C, O> {
		
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
	}
}
