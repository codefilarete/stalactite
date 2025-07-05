package org.codefilarete.stalactite.engine.configurer;

import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.MappingConfigurationException;
import org.codefilarete.stalactite.mapping.id.assembly.ComposedIdentifierAssembler;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Iterables;

import static org.codefilarete.tool.Reflections.PRIMITIVE_DEFAULT_VALUES;

/**
 * Default implementation of {@link ComposedIdentifierAssembler}: read and write values from key beans thanks to given mapping.
 * Note that for now this class only supports no-arg constructor for key-bean instantiation
 * 
 * @param <I> identifier type
 * @param <T> table type
 * @author Guillaume Mary
 */
public class DefaultComposedIdentifierAssembler<I, T extends Table<T>> extends ComposedIdentifierAssembler<I, T> {

	private final Function<ColumnedRow, I> keyFactory;
	private final Map<ReversibleAccessor<I, Object>, Column<T, Object>> mapping;
	private final Map<Accessor<I, ?>, Column<T, ?>> compositeKeyReaders;
	private final Map<Mutator<I, ?>, Column<T, ?>> compositeKeyWriters;
	private final Constructor<I> defaultConstructor;
	
	public DefaultComposedIdentifierAssembler(T targetTable,
											  Class<I> keyType,
											  Map<? extends ReversibleAccessor<I, Object>, ? extends Column<T, Object>> mapping) {
		super(targetTable);
		this.mapping = (Map<ReversibleAccessor<I, Object>, Column<T, Object>>) mapping;
		this.defaultConstructor = Reflections.findConstructor(keyType);
		// for now we only support no-arg constructor
		if (defaultConstructor == null) {
			// we'll lately throw an exception (we could do it now) but the lack of constructor may be due to an abstract class in inheritance
			// path which currently won't be instanced at runtime (because its concrete subclass will be) so there's no reason to throw
			// the exception now
			this.keyFactory = keyValueProvider -> {
				throw new MappingConfigurationException("Key class " + Reflections.toString(keyType) + " doesn't have a compatible accessible constructor,"
						+ " please implement a no-arg constructor"
//						+ " or " + Reflections.toString(idDefinition.getMemberType()) + "-arg constructor"
				);
			};
		} else {
			this.keyFactory = keyValueProvider -> Reflections.newInstance(defaultConstructor);
		}
		
		this.compositeKeyReaders = Iterables.map(mapping.entrySet(), Map.Entry::getKey, Map.Entry::getValue);
		this.compositeKeyWriters = Iterables.map(mapping.entrySet(), entry -> entry.getKey().toMutator(), Map.Entry::getValue);
	}

	public Map<ReversibleAccessor<I, ?>, Column<T, ?>> getMapping() {
		return (Map) mapping;
	}

	public Constructor<I> getDefaultConstructor() {
		return defaultConstructor;
	}

	public Map<Accessor<I, ?>, Column<T, ?>> getCompositeKeyReaders() {
		return compositeKeyReaders;
	}

	public Map<Mutator<I, ?>, Column<T, ?>> getCompositeKeyWriters() {
		return compositeKeyWriters;
	}

	@Override
	public Map<Column<T, ?>, Object> getColumnValues(I id) {
		Map<Column<T, ?>, Object> result = new HashMap<>();
		compositeKeyReaders.forEach((propertyAccessor, column) -> {
			result.put(column, id == null ? null : propertyAccessor.get(id));
		});
		return result;
	}

	@Nullable
	@Override
	public I assemble(ColumnedRow columnValueProvider) {
		// we should not return an id if any value is null
		boolean hasAnyNullValue = getColumns().stream().anyMatch(column -> {
			Object partialKeyValue = columnValueProvider.get(column);
			return partialKeyValue == null || PRIMITIVE_DEFAULT_VALUES.containsValue(partialKeyValue);
		});
		if (hasAnyNullValue) {
			return null;
		}
		
		I result = keyFactory.apply(columnValueProvider);
		mapping.forEach((setter, col) -> {
			setter.toMutator().set(result, columnValueProvider.get((Selectable<Object>) col));
		});
		return result;
	}
}
