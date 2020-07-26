package org.gama.stalactite.persistence.engine.configurer;

import org.gama.reflection.IReversibleAccessor;
import org.gama.stalactite.persistence.engine.configurer.FluentEmbeddableMappingConfigurationSupport.LinkageByColumnName;

/**
 * @author Guillaume Mary
 */
class EntityLinkageByColumnName<T> extends LinkageByColumnName<T> implements EntityLinkage<T> {
	
	private boolean primaryKey;
	
	/**
	 * Constructor
	 *
	 * @param accessor a {@link IReversibleAccessor}
	 * @param columnType the Java type of the column, will be converted to sql type thanks to {@link
	 * org.gama.stalactite.persistence.sql.ddl.JavaTypeToSqlTypeMapping}
	 * @param columnName an override of the default name that will be generated
	 */
	<O> EntityLinkageByColumnName(IReversibleAccessor<T, O> accessor, Class<O> columnType, @javax.annotation.Nullable String columnName) {
		super(accessor, columnType, columnName);
	}
	
	public boolean isPrimaryKey() {
		return primaryKey;
	}
	
	public void primaryKey() {
		this.primaryKey = true;
	}
}
