package org.gama.stalactite.persistence.engine;

import org.gama.reflection.IReversibleAccessor;
import org.gama.stalactite.persistence.engine.FluentEmbeddableMappingConfigurationSupport.AbstractLinkage;
import org.gama.stalactite.persistence.structure.Column;

/**
 * @author Guillaume Mary
 */
class EntityLinkageByColumn<T> extends AbstractLinkage<T> implements EntityLinkage<T> {
	
	private final IReversibleAccessor<T, ?> function;
	private final Column column;
	
	/**
	 * Constructor with mandatory objects
	 *
	 * @param propertyAccessor a {@link IReversibleAccessor}
	 * @param column an override of the default column that would have been generated
	 */
	EntityLinkageByColumn(IReversibleAccessor<T, ?> propertyAccessor, Column column) {
		this.function = propertyAccessor;
		this.column = column;
	}
	
	@Override
	public <I> IReversibleAccessor<T, I> getAccessor() {
		return (IReversibleAccessor<T, I>) function;
	}
	
	@Override
	public String getColumnName() {
		return column.getName();
	}
	
	@Override
	public Class<?> getColumnType() {
		return column.getJavaType();
	}
	
	@Override
	public boolean isPrimaryKey() {
		return column.isPrimaryKey();
	}
	
	public Column getColumn() {
		return column;
	}
}
