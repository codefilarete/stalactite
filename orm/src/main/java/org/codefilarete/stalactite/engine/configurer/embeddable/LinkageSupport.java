package org.codefilarete.stalactite.engine.configurer.embeddable;

import javax.annotation.Nullable;
import java.lang.reflect.Field;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.configurer.PropertyAccessorResolver;
import org.codefilarete.stalactite.engine.configurer.property.ColumnLinkageOptionsByColumn;
import org.codefilarete.stalactite.engine.configurer.property.ColumnLinkageOptionsSupport;
import org.codefilarete.stalactite.engine.configurer.property.LocalColumnLinkageOptions;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry;
import org.codefilarete.tool.function.Converter;
import org.codefilarete.tool.function.ThreadSafeLazyInitializer;
import org.danekja.java.util.function.serializable.SerializableBiConsumer;
import org.danekja.java.util.function.serializable.SerializableFunction;

import static org.codefilarete.tool.Nullable.nullable;

/**
 * Small contract for mapping definition storage. See add(..) methods.
 *
 * @param <T> property owner type
 * @param <O> property type
 */
public class LinkageSupport<T, O> implements EmbeddableMappingConfiguration.Linkage<T, O> {
	
	/**
	 * Optional binder for this mapping
	 */
	private ParameterBinder<?> parameterBinder;
	
	@Nullable
	private ParameterBinderRegistry.EnumBindType enumBindType;
	
	private boolean nullable = true;
	
	private boolean unique;
	
	private boolean setByConstructor = false;
	
	private LocalColumnLinkageOptions columnOptions = new ColumnLinkageOptionsSupport();
	
	private final ThreadSafeLazyInitializer<ReversibleAccessor<T, O>> accessor;
	
	private SerializableFunction<T, O> getter;
	
	private SerializableBiConsumer<T, O> setter;
	
	private Field field;
	
	private boolean readonly;
	
	private String extraTableName;
	
	private Converter<? /* value coming from database */, O> readConverter;
	
	private Converter<O, ? /* value going to database */> writeConverter;
	
	public LinkageSupport(SerializableFunction<T, O> getter) {
		this.getter = getter;
		this.accessor = new AccessorFieldLazyInitializer();
	}
	
	public LinkageSupport(SerializableBiConsumer<T, O> setter) {
		this.setter = setter;
		this.accessor = new AccessorFieldLazyInitializer();
	}
	
	@Override
	@Nullable
	public ParameterBinder<Object> getParameterBinder() {
		return (ParameterBinder<Object>) parameterBinder;
	}
	
	public void setParameterBinder(@Nullable ParameterBinder<?> parameterBinder) {
		this.parameterBinder = parameterBinder;
	}
	
	@Override
	@Nullable
	public ParameterBinderRegistry.EnumBindType getEnumBindType() {
		return enumBindType;
	}
	
	public void setEnumBindType(@Nullable ParameterBinderRegistry.EnumBindType enumBindType) {
		this.enumBindType = enumBindType;
	}
	
	@Override
	public boolean isNullable() {
		return nullable;
	}
	
	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}
	
	public void setUnique(boolean unique) {
		this.unique = unique;
	}
	
	@Override
	public boolean isUnique() {
		return unique;
	}
	
	public void setByConstructor() {
		this.setByConstructor = true;
	}
	
	@Override
	public boolean isSetByConstructor() {
		return setByConstructor;
	}
	
	public LocalColumnLinkageOptions getColumnOptions() {
		return columnOptions;
	}
	
	public void setColumnOptions(LocalColumnLinkageOptions columnOptions) {
		this.columnOptions = columnOptions;
	}
	
	@Override
	public ReversibleAccessor<T, O> getAccessor() {
		return accessor.get();
	}
	
	@Nullable
	@Override
	public Field getField() {
		return field;
	}
	
	public void setField(Field field) {
		this.field = field;
	}
	
	@Nullable
	@Override
	public String getColumnName() {
		return nullable(this.columnOptions).map(EntityMappingConfiguration.ColumnLinkageOptions::getColumnName).get();
	}
	
	@Nullable
	@Override
	public Size getColumnSize() {
		return nullable(this.columnOptions).map(EntityMappingConfiguration.ColumnLinkageOptions::getColumnSize).get();
	}
	
	@Override
	public Class<O> getColumnType() {
		return this.columnOptions instanceof ColumnLinkageOptionsByColumn
				? (Class<O>) ((ColumnLinkageOptionsByColumn) this.columnOptions).getColumnType()
				: AccessorDefinition.giveDefinition(this.accessor.get()).getMemberType();
	}
	
	@Override
	public boolean isReadonly() {
		return readonly;
	}
	
	public void readonly() {
		this.readonly = true;
	}
	
	@Override
	public String getExtraTableName() {
		return extraTableName;
	}
	
	public void setExtraTableName(String extraTableName) {
		this.extraTableName = extraTableName;
	}
	
	@Override
	public Converter<?, O> getReadConverter() {
		return readConverter;
	}
	
	public void setReadConverter(Converter<?, O> readConverter) {
		this.readConverter = readConverter;
	}
	
	@Override
	public Converter<O, ?> getWriteConverter() {
		return writeConverter;
	}
	
	public void setWriteConverter(Converter<O, ?> writeConverter) {
		this.writeConverter = writeConverter;
	}
	
	/**
	 * Internal class that computes a {@link PropertyAccessor} from getter or setter according to which one is set up
	 */
	private class AccessorFieldLazyInitializer extends ThreadSafeLazyInitializer<ReversibleAccessor<T, O>> {
		
		@Override
		protected ReversibleAccessor<T, O> createInstance() {
			return new PropertyAccessorResolver<>(new PropertyAccessorResolver.PropertyMapping<T, O>() {
				@Override
				public SerializableFunction<T, O> getGetter() {
					return LinkageSupport.this.getter;
				}
				
				@Override
				public SerializableBiConsumer<T, O> getSetter() {
					return LinkageSupport.this.setter;
				}
				
				@Override
				public Field getField() {
					return LinkageSupport.this.getField();
				}
			}).resolve();
		}
	}
}
