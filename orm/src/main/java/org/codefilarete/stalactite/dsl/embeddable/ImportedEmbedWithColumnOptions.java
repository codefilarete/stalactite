package org.codefilarete.stalactite.dsl.embeddable;

import org.codefilarete.reflection.SerializableAccessor;
import org.codefilarete.reflection.SerializableMutator;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Equivalent of {@link ImportedEmbedOptions} in context of an entity : allow to define some {@link Column} for some properties.
 * Those methods are not available in {@link ImportedEmbedOptions} because it is supposed to be reusable in different entities and therefore for
 * different tables, hence letting one define column for an embeddable bean makes no sense.
 * 
 * @author Guillaume Mary
 */
public interface ImportedEmbedWithColumnOptions<C> extends ImportedEmbedOptions<C> {
	
	@Override
	<IN> ImportedEmbedWithColumnOptions<C> overrideName(SerializableAccessor<C, IN> getter, String columnName);
	
	@Override
	<IN> ImportedEmbedWithColumnOptions<C> overrideName(SerializableMutator<C, IN> setter, String columnName);
	
	@Override
	<IN> ImportedEmbedWithColumnOptions<C> overrideSize(SerializableAccessor<C, IN> getter, Size columnSize);
	
	@Override
	<IN> ImportedEmbedWithColumnOptions<C> overrideSize(SerializableMutator<C, IN> setter, Size columnSize);
	
	@Override
	<IN> ImportedEmbedWithColumnOptions<C> exclude(SerializableAccessor<C, IN> getter);
	
	@Override
	<IN> ImportedEmbedWithColumnOptions<C> exclude(SerializableMutator<C, IN> setter);
	
	/**
	 * Overrides embedding with an existing target column
	 *
	 * @param getter the getter as a method reference
	 * @param targetColumn a column that's the target of the getter
	 * @param <IN> input of the function (type of the embedded element)
	 * @return a mapping configurer, specialized for embedded elements
	 */
	<IN> ImportedEmbedWithColumnOptions<C> override(SerializableAccessor<C, IN> getter, Column<? extends Table, IN> targetColumn);
	
	/**
	 * Overrides embedding with an existing target column
	 *
	 * @param setter the setter as a method reference
	 * @param targetColumn a column that's the target of the getter
	 * @param <IN> input of the function (type of the embedded element)
	 * @return a mapping configurer, specialized for embedded elements
	 */
	<IN> ImportedEmbedWithColumnOptions<C> override(SerializableMutator<C, IN> setter, Column<? extends Table, IN> targetColumn);
	
}
