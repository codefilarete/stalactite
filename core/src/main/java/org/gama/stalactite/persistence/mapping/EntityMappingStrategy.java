package org.gama.stalactite.persistence.mapping;

import java.util.Map;
import java.util.Set;

import org.gama.reflection.IReversibleAccessor;
import org.gama.stalactite.persistence.engine.runtime.Persister;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * The interface defining methods necessary to persist an entity (ie an object with an id)
 *
 * @author Guillaume Mary
 */
public interface EntityMappingStrategy<C, I, T extends Table> extends MappingStrategy<C, T>, IdAccessor<C, I> {
	
	T getTargetTable();
	
	Class<C> getClassToPersist();
	
	/**
	 * Necessary to distinguish insert or update action on {@link Persister#persist(Object)} call
	 * @param c an instance of C
	 * @return true if the instance is not persisted, false if not (a row for its identifier already exists in the targeted table)
	 */
	boolean isNew(C c);
	
	IdMappingStrategy<C, I> getIdMappingStrategy();
	
	Set<Column<T, Object>> getInsertableColumns();
	
	Set<Column<T, Object>> getSelectableColumns();
	
	Set<Column<T, Object>> getUpdatableColumns();
	
	Iterable<Column<T, Object>> getVersionedKeys();
	
	Map<Column<T, Object>, Object> getVersionedKeyValues(C c);
	
	Map<IReversibleAccessor<C, Object>, EmbeddedBeanMappingStrategy<Object, T>> getEmbeddedBeanStrategies();
}
