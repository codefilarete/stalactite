package org.codefilarete.stalactite.engine.runtime;

import java.util.Map;
import java.util.Set;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.EmbeddedBeanMapping;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.mapping.IdMapping;
import org.codefilarete.stalactite.mapping.RowTransformer;
import org.codefilarete.stalactite.mapping.RowTransformer.TransformerListener;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.Row;

/**
 * {@link EntityMapping} that wraps another one and delegates all its methods to it without any additionnal feature.
 * Made for overriding only some targeted methods.
 * 
 * @author Guillaume Mary
 */
public class EntityMappingWrapper<C, I, T extends Table<T>> implements EntityMapping<C, I, T> {
	
	private final EntityMapping<C, I, T> surrogate;
	
	public EntityMappingWrapper(EntityMapping<C, I, T> surrogate) {
		this.surrogate = surrogate;
	}
	
	@Override
	public T getTargetTable() {
		return surrogate.getTargetTable();
	}
	
	@Override
	public Class<C> getClassToPersist() {
		return surrogate.getClassToPersist();
	}
	
	@Override
	public boolean isNew(C c) {
		return surrogate.isNew(c);
	}
	
	@Override
	public IdMapping<C, I> getIdMapping() {
		return surrogate.getIdMapping();
	}
	
	@Override
	public Set<Column<T, Object>> getInsertableColumns() {
		return surrogate.getInsertableColumns();
	}
	
	@Override
	public Set<Column<T, Object>> getSelectableColumns() {
		return surrogate.getSelectableColumns();
	}
	
	@Override
	public Set<Column<T, Object>> getUpdatableColumns() {
		return surrogate.getUpdatableColumns();
	}
	
	@Override
	public Iterable<Column<T, Object>> getVersionedKeys() {
		return surrogate.getVersionedKeys();
	}
	
	@Override
	public Map<Column<T, Object>, Object> getVersionedKeyValues(C c) {
		return surrogate.getVersionedKeyValues(c);
	}
	
	@Override
	public Map<ReversibleAccessor<C, Object>, EmbeddedBeanMapping<Object, T>> getEmbeddedBeanStrategies() {
		return surrogate.getEmbeddedBeanStrategies();
	}
	
	@Override
	public Map<Column<T, Object>, Object> getInsertValues(C c) {
		return surrogate.getInsertValues(c);
	}
	
	@Override
	public Map<UpwhereColumn<T>, Object> getUpdateValues(C modified, C unmodified, boolean allColumns) {
		return surrogate.getUpdateValues(modified, unmodified, allColumns);
	}
	
	@Override
	public C transform(Row row) {
		return surrogate.transform(row);
	}
	
	@Override
	public void addShadowColumnInsert(ShadowColumnValueProvider<C, T> valueProvider) {
		surrogate.addShadowColumnInsert(valueProvider);
	}
	
	@Override
	public void addShadowColumnUpdate(ShadowColumnValueProvider<C, T> valueProvider) {
		surrogate.addShadowColumnUpdate(valueProvider);
	}
	
	@Override
	public <O> void addShadowColumnSelect(Column<T, O> column) {
		surrogate.addShadowColumnSelect(column);
	}
	
	@Override
	public void addPropertySetByConstructor(ValueAccessPoint accessor) {
		surrogate.addPropertySetByConstructor(accessor);
	}
	
	@Override
	public Map<ReversibleAccessor<C, Object>, Column<T, Object>> getPropertyToColumn() {
		return surrogate.getPropertyToColumn();
	}
	
	@Override
	public RowTransformer<C> copyTransformerWithAliases(ColumnedRow columnedRow) {
		return surrogate.copyTransformerWithAliases(columnedRow);
	}
	
	@Override
	public void addTransformerListener(TransformerListener<C> listener) {
		surrogate.addTransformerListener(listener);
	}
	
	@Override
	public I getId(C c) {
		return surrogate.getId(c);
	}
	
	@Override
	public void setId(C c, I identifier) {
		surrogate.setId(c, identifier);
	}
}
