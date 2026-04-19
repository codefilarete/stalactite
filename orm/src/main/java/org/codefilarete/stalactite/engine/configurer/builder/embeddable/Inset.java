package org.codefilarete.stalactite.engine.configurer.builder.embeddable;

import java.lang.reflect.Method;

import org.codefilarete.reflection.ReadWriteAccessPoint;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.reflection.ValueAccessPointSet;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;

public interface Inset<SRC, TRGT> {
	
	/**
	 * Equivalent of {@link #getInsetAccessor()} as a {@link ReadWriteAccessPoint}
	 */
	ReadWritePropertyAccessPoint<SRC, TRGT> getAccessor();
	
	/**
	 * Equivalent of given getter or setter at construction time as a {@link Method}
	 */
	Method getInsetAccessor();
	
	Class<TRGT> getEmbeddedClass();
	
	ValueAccessPointSet<TRGT, ValueAccessPoint<TRGT>> getExcludedProperties();
	
	ValueAccessPointMap<TRGT, String, ValueAccessPoint<TRGT>> getOverriddenColumnNames();
	
	ValueAccessPointMap<TRGT, Size, ValueAccessPoint<TRGT>> getOverriddenColumnSizes();
	
	ValueAccessPointMap<TRGT, Column, ValueAccessPoint<TRGT>> getOverriddenColumns();
	
	EmbeddableMappingConfiguration<TRGT> getConfiguration();
}
