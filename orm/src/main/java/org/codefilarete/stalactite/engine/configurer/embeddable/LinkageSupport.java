package org.codefilarete.stalactite.engine.configurer.embeddable;

import javax.annotation.Nullable;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.engine.configurer.ValueAccessPointVariantSupport;
import org.codefilarete.stalactite.engine.configurer.property.ColumnLinkageOptionsByColumn;
import org.codefilarete.stalactite.engine.configurer.property.ColumnLinkageOptionsSupport;
import org.codefilarete.stalactite.engine.configurer.property.LocalColumnLinkageOptions;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinderRegistry;
import org.codefilarete.tool.function.Converter;
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
	
	@Nullable
	private Boolean nullable;
	
	private boolean unique;
	
	private boolean setByConstructor = false;
	
	private LocalColumnLinkageOptions columnOptions = new ColumnLinkageOptionsSupport();
	
	private final ValueAccessPointVariantSupport<T, O> accessor;
	
	@Nullable
	private String fieldName;
	
	private boolean readonly;
	
	private String extraTableName;
	
	private Converter<? /* value coming from database */, O> readConverter;
	
	private Converter<O, ? /* value going to database */> writeConverter;
	
	
	public LinkageSupport(SerializableFunction<T, O> getter) {
		this.accessor = new ValueAccessPointVariantSupport<>(getter);
	}
	
	public LinkageSupport(SerializableBiConsumer<T, O> setter) {
		this.accessor = new ValueAccessPointVariantSupport<>(setter);
	}
	
	public LinkageSupport(Class persistedClass, String fieldName) {
		this.accessor = new ValueAccessPointVariantSupport<>(persistedClass, fieldName);
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
	
	@Nullable
	@Override
	public Boolean isNullable() {
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
		return accessor.getAccessor();
	}
	
	@Override
	@Nullable
	public String getFieldName() {
		return fieldName;
	}
	
	public void setField(Class<T> classToPersist, String fieldName) {
		this.fieldName = fieldName;
		// Note that getField(..) will throw an exception if field is not found, at the opposite of findField(..)
		this.accessor.setField(classToPersist, fieldName);
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
				: AccessorDefinition.giveDefinition(this.accessor.getAccessor()).getMemberType();
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
}
