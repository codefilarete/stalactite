package org.codefilarete.stalactite.engine.runtime;

import java.util.Map;
import java.util.Set;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.EmbeddedBeanMapping;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.RowTransformer;
import org.codefilarete.stalactite.mapping.RowTransformer.TransformerListener;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.function.Converter;

/**
 * {@link EntityMapping} that wraps another one and delegates all its methods to it without any additionnal feature.
 * Made for overriding only some targeted methods.
 * 
 * @author Guillaume Mary
 */
public class EntityMappingWrapper<C, I, T extends Table<T>> implements EntityMapping<C, I, T> {
	
	private final EntityMapping<C, I, T> delegate;
	
	public EntityMappingWrapper(EntityMapping<C, I, T> delegate) {
		this.delegate = delegate;
	}
	
	@Override
	public T getTargetTable() {
		return delegate.getTargetTable();
	}
	
	@Override
	public Class<C> getClassToPersist() {
		return delegate.getClassToPersist();
	}
	
	@Override
	public boolean isNew(C c) {
		return delegate.isNew(c);
	}
	
	@Override
	public IdMapping<C, I> getIdMapping() {
		return delegate.getIdMapping();
	}
	
	@Override
	public Set<Column<T, ?>> getInsertableColumns() {
		return delegate.getInsertableColumns();
	}
	
	@Override
	public Set<Column<T, ?>> getSelectableColumns() {
		return delegate.getSelectableColumns();
	}
	
	@Override
	public Set<Column<T, ?>> getUpdatableColumns() {
		return delegate.getUpdatableColumns();
	}
	
	@Override
	public Iterable<Column<T, ?>> getVersionedKeys() {
		return delegate.getVersionedKeys();
	}
	
	@Override
	public Map<Column<T, ?>, Object> getVersionedKeyValues(C c) {
		return delegate.getVersionedKeyValues(c);
	}
	
	@Override
	public Map<ReversibleAccessor<C, Object>, EmbeddedBeanMapping<Object, T>> getEmbeddedBeanStrategies() {
		return delegate.getEmbeddedBeanStrategies();
	}
	
	@Override
	public Map<Column<T, ?>, ?> getInsertValues(C c) {
		return delegate.getInsertValues(c);
	}
	
	@Override
	public Map<UpwhereColumn<T>, ?> getUpdateValues(C modified, C unmodified, boolean allColumns) {
		return delegate.getUpdateValues(modified, unmodified, allColumns);
	}
	
	@Override
	public C transform(Row row) {
		return delegate.transform(row);
	}
	
	@Override
	public void addShadowColumnInsert(ShadowColumnValueProvider<C, T> valueProvider) {
		delegate.addShadowColumnInsert(valueProvider);
	}
	
	@Override
	public void addShadowColumnUpdate(ShadowColumnValueProvider<C, T> valueProvider) {
		delegate.addShadowColumnUpdate(valueProvider);
	}
	
	@Override
	public <O> void addShadowColumnSelect(Column<T, O> column) {
		delegate.addShadowColumnSelect(column);
	}
	
	@Override
	public void addPropertySetByConstructor(ValueAccessPoint<C> accessor) {
		delegate.addPropertySetByConstructor(accessor);
	}
	
	@Override
	public Map<ReversibleAccessor<C, ?>, Column<T, ?>> getPropertyToColumn() {
		return delegate.getPropertyToColumn();
	}
	
	@Override
	public Map<ReversibleAccessor<C, ?>, Column<T, ?>> getReadonlyPropertyToColumn() {
		return delegate.getReadonlyPropertyToColumn();
	}
	
	@Override
	public ValueAccessPointMap<C, Converter<Object, Object>> getReadConverters() {
		return delegate.getReadConverters();
	}
	
	@Override
	public RowTransformer<C> copyTransformerWithAliases(ColumnedRow columnedRow) {
		return delegate.copyTransformerWithAliases(columnedRow);
	}
	
	@Override
	public void addTransformerListener(TransformerListener<C> listener) {
		delegate.addTransformerListener(listener);
	}
	
	@Override
	public I getId(C c) {
		return delegate.getId(c);
	}
	
	@Override
	public void setId(C c, I identifier) {
		delegate.setId(c, identifier);
	}
}
