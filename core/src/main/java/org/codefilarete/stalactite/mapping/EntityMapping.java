package org.codefilarete.stalactite.mapping;

import java.util.Map;
import java.util.Set;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.engine.runtime.BeanPersister;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * The interface defining methods necessary to persist an entity (ie an object with an id)
 *
 * @author Guillaume Mary
 */
public interface EntityMapping<C, I, T extends Table<T>> extends Mapping<C, T>, IdAccessor<C, I> {
	
	T getTargetTable();
	
	Class<C> getClassToPersist();
	
	/**
	 * Necessary to distinguish insert or update action on {@link BeanPersister#persist(Object)} call
	 * @param c an instance of C
	 * @return true if the instance is not persisted, false if not (a row for its identifier already exists in the targeted table)
	 */
	boolean isNew(C c);
	
	IdMapping<C, I> getIdMapping();
	
	Set<Column<T, ?>> getInsertableColumns();
	
	Set<Column<T, ?>> getSelectableColumns();
	
	Set<Column<T, ?>> getUpdatableColumns();
	
	Iterable<Column<T, ?>> getVersionedKeys();
	
	Map<Column<T, ?>, Object> getVersionedKeyValues(C c);
	
	Map<ReversibleAccessor<C, Object>, EmbeddedBeanMapping<Object, T>> getEmbeddedBeanStrategies();
}
