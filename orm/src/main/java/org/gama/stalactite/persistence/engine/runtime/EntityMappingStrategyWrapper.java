package org.gama.stalactite.persistence.engine.runtime;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Set;

import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.ValueAccessPoint;
import org.gama.stalactite.persistence.mapping.AbstractTransformer;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.persistence.mapping.EmbeddedBeanMappingStrategy;
import org.gama.stalactite.persistence.mapping.EntityMappingStrategy;
import org.gama.stalactite.persistence.mapping.RowTransformer.TransformerListener;
import org.gama.stalactite.persistence.mapping.IdMappingStrategy;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.result.Row;

/**
 * {@link EntityMappingStrategy} that wraps another one and delegates all its methods to it without any additionnal feature.
 * Made for overriding only some targeted methods.
 * 
 * @author Guillaume Mary
 */
public class EntityMappingStrategyWrapper<C, I, T extends Table> implements EntityMappingStrategy<C, I, T> {
	
	private final EntityMappingStrategy<C, I, T> surrogate;
	
	public EntityMappingStrategyWrapper(EntityMappingStrategy<C, I, T> surrogate) {
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
	public IdMappingStrategy<C, I> getIdMappingStrategy() {
		return surrogate.getIdMappingStrategy();
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
	public Map<IReversibleAccessor<C, Object>, EmbeddedBeanMappingStrategy<Object, T>> getEmbeddedBeanStrategies() {
		return surrogate.getEmbeddedBeanStrategies();
	}
	
	@Override
	@Nonnull
	public Map<Column<T, Object>, Object> getInsertValues(C c) {
		return surrogate.getInsertValues(c);
	}
	
	@Override
	@Nonnull
	public Map<UpwhereColumn<T>, Object> getUpdateValues(C modified, C unmodified, boolean allColumns) {
		return surrogate.getUpdateValues(modified, unmodified, allColumns);
	}
	
	@Override
	public C transform(Row row) {
		return surrogate.transform(row);
	}
	
	@Override
	public <O> void addShadowColumnInsert(ShadowColumnValueProvider<C, O, T> valueProvider) {
		surrogate.addShadowColumnInsert(valueProvider);
	}
	
	@Override
	public <O> void addShadowColumnUpdate(ShadowColumnValueProvider<C, O, T> valueProvider) {
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
	public Map<IReversibleAccessor<C, Object>, Column<T, Object>> getPropertyToColumn() {
		return surrogate.getPropertyToColumn();
	}
	
	@Override
	public AbstractTransformer<C> copyTransformerWithAliases(ColumnedRow columnedRow) {
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
