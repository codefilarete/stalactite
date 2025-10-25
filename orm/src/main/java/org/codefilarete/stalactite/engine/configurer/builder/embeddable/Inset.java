package org.codefilarete.stalactite.engine.configurer.builder.embeddable;

import java.lang.reflect.Method;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.reflection.ValueAccessPointSet;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;

public interface Inset<SRC, TRGT> {
	
	/**
	 * Equivalent of {@link #getInsetAccessor()} as a {@link PropertyAccessor}
	 */
	Accessor<SRC, TRGT> getAccessor();
	
	/**
	 * Equivalent of given getter or setter at construction time as a {@link Method}
	 */
	Method getInsetAccessor();
	
	Class<TRGT> getEmbeddedClass();
	
	ValueAccessPointSet<SRC> getExcludedProperties();
	
	ValueAccessPointMap<SRC, String> getOverriddenColumnNames();
	
	ValueAccessPointMap<SRC, Size> getOverriddenColumnSizes();
	
	ValueAccessPointMap<SRC, Column> getOverriddenColumns();
	
	EmbeddableMappingConfiguration<TRGT> getConfiguration();
}
