package org.codefilarete.stalactite.engine.runtime;

import org.codefilarete.stalactite.sql.ddl.structure.Table;


/**
 * Mashup between {@link ConfiguredRelationalPersister} and {@link RelationalEntityPersister}, made to represent
 * a persister that is fully configured (knows its main table and mapping) and that is also able
 * to join itself with another persister to handle relations.
 * <p>
 * Used as the common contract for main and sub-entity persisters wherever both capabilities are required together,
 * such as in polymorphism engines.
 *
 * @param <C>
 * @param <I>
 * @param <T>
 *
 * @author Guillaume Mary
 */
public interface ConfiguredRelationalEntityPersister<C, I, T extends Table<T>>
		extends ConfiguredRelationalPersister<C, I, T>, RelationalEntityPersister<C, I> {
	
	@Override
	default T getMainTable() {
		return ConfiguredRelationalPersister.super.getMainTable();
	}
}
