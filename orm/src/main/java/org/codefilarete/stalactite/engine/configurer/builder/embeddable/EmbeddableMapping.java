package org.codefilarete.stalactite.engine.configurer.builder.embeddable;

import java.util.HashMap;
import java.util.Map;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.function.Converter;

/**
 * Resulting class of {@link EmbeddableMappingBuilder#build()} process
 *
 * @param <C>
 * @param <T>
 */
public class EmbeddableMapping<C, T extends Table<T>> {
	
	private final Map<ReversibleAccessor<C, Object>, Column<T, Object>> mapping = new HashMap<>();
	
	private final Map<ReversibleAccessor<C, Object>, Column<T, Object>> readonlyMapping = new HashMap<>();
	
	private final ValueAccessPointMap<C, Converter<Object, Object>> readConverters = new ValueAccessPointMap<>();
	
	private final ValueAccessPointMap<C, Converter<Object, Object>> writeConverters = new ValueAccessPointMap<>();
	
	/**
	 * @return mapped properties
	 */
	public Map<ReversibleAccessor<C, Object>, Column<T, Object>> getMapping() {
		return mapping;
	}
	
	/**
	 * @return mapped readonly properties
	 */
	public Map<ReversibleAccessor<C, Object>, Column<T, Object>> getReadonlyMapping() {
		return readonlyMapping;
	}
	
	public ValueAccessPointMap<C, Converter<Object, Object>> getReadConverters() {
		return readConverters;
	}
	
	public ValueAccessPointMap<C, Converter<Object, Object>> getWriteConverters() {
		return writeConverters;
	}
}
