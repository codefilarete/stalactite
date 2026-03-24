package org.codefilarete.stalactite.engine.configurer.builder.embeddable;

import java.util.Map;

import org.codefilarete.reflection.PropertyAccessPoint;
import org.codefilarete.reflection.PropertyMutator;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.function.Converter;

/**
 * Resulting class of {@link EmbeddableMappingBuilder#build()} process
 *
 * @param <C>
 * @param <T>
 */
public class EmbeddableMapping<C, T extends Table<T>> {
	
	// We keep the order defined by the user, because, in the particular case of composite key definition, it helps to
	// have stable tests. However, at runtime, I'm not sure about the interest of it, except for the foreign key and its
	// associated index impact. So it may be better to keep it as well, to let the user have a way to define its column
	// order... even if it's tied to the order of its own line of code (DSL usage)
	private final Map<ReadWritePropertyAccessPoint<C, Object>, Column<T, Object>> mapping = new KeepOrderMap<>();
	
	private final Map<PropertyMutator<C, Object>, Column<T, Object>> readonlyMapping = new KeepOrderMap<>();
	
	private final ValueAccessPointMap<C, Converter<Object, Object>, PropertyAccessPoint<C, ?>> readConverters = new ValueAccessPointMap<>();
	
	private final ValueAccessPointMap<C, Converter<Object, Object>, PropertyAccessPoint<C, ?>> writeConverters = new ValueAccessPointMap<>();
	
	/**
	 * @return mapped properties
	 */
	public Map<ReadWritePropertyAccessPoint<C, Object>, Column<T, Object>> getMapping() {
		return mapping;
	}
	
	/**
	 * @return mapped readonly properties
	 */
	public Map<PropertyMutator<C, Object>, Column<T, Object>> getReadonlyMapping() {
		return readonlyMapping;
	}
	
	public ValueAccessPointMap<C, Converter<Object, Object>, PropertyAccessPoint<C, ?>> getReadConverters() {
		return readConverters;
	}
	
	public ValueAccessPointMap<C, Converter<Object, Object>, PropertyAccessPoint<C, ?>> getWriteConverters() {
		return writeConverters;
	}
}
