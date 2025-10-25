package org.codefilarete.stalactite.engine.configurer.builder.embeddable;

import javax.annotation.Nullable;
import java.lang.reflect.Field;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry.EnumBindType;
import org.codefilarete.tool.function.Converter;

/**
 * Small contract for defining property configuration storage
 *
 * @param <C> property owner type
 * @param <O> property type
 */
public interface EmbeddableLinkage<C, O> {
	
	ReversibleAccessor<C, O> getAccessor();
	
	@Nullable
	Field getField();
	
	@Nullable
	String getColumnName();
	
	@Nullable
	Size getColumnSize();
	
	Class<O> getColumnType();
	
	@Nullable
	String getExtraTableName();
	
	@Nullable
	ParameterBinder<Object> getParameterBinder();
	
	/**
	 * Gives the choice made by the user to define how to bind enum values: by name or ordinal.
	 *
	 * @return null if no info was given
	 */
	@Nullable
	EnumBindType getEnumBindType();
	
	boolean isNullable();
	
	boolean isReadonly();
	
	boolean isUnique();
	
	@Nullable
	Converter<?, O> getReadConverter();
	
	@Nullable
	Converter<O, ?> getWriteConverter();
}
