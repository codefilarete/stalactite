package org.gama.stalactite.persistence.engine.configurer;

import org.gama.reflection.ReversibleAccessor;
import org.gama.stalactite.persistence.engine.configurer.FluentEmbeddableMappingConfigurationSupport.AbstractLinkage;
import org.gama.stalactite.persistence.structure.Column;

/**
 * @author Guillaume Mary
 */
class EntityLinkageByColumn<T> extends AbstractLinkage<T> implements EntityLinkage<T> {
	
	private final Column column;
	
	/**
	 * Constructor with mandatory objects
	 *
	 * @param propertyAccessor a {@link ReversibleAccessor}
	 * @param column an override of the default column that would have been generated
	 */
	EntityLinkageByColumn(ReversibleAccessor<T, ?> propertyAccessor, Column column) {
		super(propertyAccessor);
		this.column = column;
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
