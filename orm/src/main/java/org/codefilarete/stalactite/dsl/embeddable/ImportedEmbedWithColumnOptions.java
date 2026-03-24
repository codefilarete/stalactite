package org.codefilarete.stalactite.dsl.embeddable;

import org.codefilarete.reflection.SerializablePropertyAccessor;
import org.codefilarete.reflection.SerializablePropertyMutator;
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
	<IN> ImportedEmbedWithColumnOptions<C> overrideName(SerializablePropertyAccessor<C, IN> getter, String columnName);
	
	@Override
	<IN> ImportedEmbedWithColumnOptions<C> overrideName(SerializablePropertyMutator<C, IN> setter, String columnName);
	
	@Override
	<IN> ImportedEmbedWithColumnOptions<C> overrideSize(SerializablePropertyAccessor<C, IN> getter, Size columnSize);
	
	@Override
	<IN> ImportedEmbedWithColumnOptions<C> overrideSize(SerializablePropertyMutator<C, IN> setter, Size columnSize);
	
	@Override
	<IN> ImportedEmbedWithColumnOptions<C> exclude(SerializablePropertyAccessor<C, IN> getter);
	
	@Override
	<IN> ImportedEmbedWithColumnOptions<C> exclude(SerializablePropertyMutator<C, IN> setter);
	
	/**
	 * Overrides embedding with an existing target column
	 *
	 * @param getter the getter as a method reference
	 * @param targetColumn a column that's the target of the getter
	 * @param <IN> input of the function (type of the embedded element)
	 * @return a mapping configurer, specialized for embedded elements
	 */
	<IN> ImportedEmbedWithColumnOptions<C> override(SerializablePropertyAccessor<C, IN> getter, Column<? extends Table, IN> targetColumn);
	
	/**
	 * Overrides embedding with an existing target column
	 *
	 * @param setter the setter as a method reference
	 * @param targetColumn a column that's the target of the getter
	 * @param <IN> input of the function (type of the embedded element)
	 * @return a mapping configurer, specialized for embedded elements
	 */
	<IN> ImportedEmbedWithColumnOptions<C> override(SerializablePropertyMutator<C, IN> setter, Column<? extends Table, IN> targetColumn);
	
}
