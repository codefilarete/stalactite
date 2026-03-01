package org.codefilarete.stalactite.engine.configurer.entity;

import javax.annotation.Nullable;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.SerializableAccessor;
import org.codefilarete.reflection.SerializableMutator;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration.SingleKeyMapping;
import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.engine.configurer.ValueAccessPointVariantSupport;
import org.codefilarete.stalactite.engine.configurer.property.ColumnLinkageOptionsSupport;
import org.codefilarete.stalactite.engine.configurer.property.LocalColumnLinkageOptions;

/**
 * Storage for single key mapping definition. See {@link org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder#mapKey(SerializableAccessor, IdentifierPolicy)} methods.
 */
class SingleKeyLinkageSupport<C, I> implements SingleKeyMapping<C, I> {
	
	private final IdentifierPolicy<I> identifierPolicy;
	
	private LocalColumnLinkageOptions columnOptions = new ColumnLinkageOptionsSupport();
	
	private boolean setByConstructor;
	
	private final ValueAccessPointVariantSupport<C, I> accessor;
	
	@Nullable
	private String fieldName;
	
	public SingleKeyLinkageSupport(SerializableAccessor<C, I> getter, IdentifierPolicy<I> identifierPolicy) {
		this.accessor = new ValueAccessPointVariantSupport<>(getter);
		this.identifierPolicy = identifierPolicy;
	}
	
	public SingleKeyLinkageSupport(SerializableMutator<C, I> setter, IdentifierPolicy<I> identifierPolicy) {
		this.accessor = new ValueAccessPointVariantSupport<>(setter);
		this.identifierPolicy = identifierPolicy;
	}
	
	public SingleKeyLinkageSupport(Class<C> classToPersist, String fieldName, IdentifierPolicy<I> identifierPolicy) {
		this.accessor = new ValueAccessPointVariantSupport<>(classToPersist, fieldName);
		this.identifierPolicy = identifierPolicy;
	}
	
	@Override
	public IdentifierPolicy<I> getIdentifierPolicy() {
		return identifierPolicy;
	}
	
	@Override
	public ReversibleAccessor<C, I> getAccessor() {
		return accessor.getAccessor();
	}
	
	@Override
	public LocalColumnLinkageOptions getColumnOptions() {
		return columnOptions;
	}
	
	public void setColumnOptions(LocalColumnLinkageOptions columnOptions) {
		this.columnOptions = columnOptions;
	}
	
	public void setByConstructor() {
		this.setByConstructor = true;
	}
	
	@Override
	@Nullable
	public String getFieldName() {
		return fieldName;
	}
	
	public void setField(Class<C> classToPersist, String fieldName) {
		this.fieldName = fieldName;
		this.accessor.setField(classToPersist, fieldName);
	}
	
	@Override
	public boolean isSetByConstructor() {
		return setByConstructor;
	}
}
