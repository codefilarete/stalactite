package org.codefilarete.stalactite.engine.configurer.model;

import java.util.Set;

import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.KeepOrderSet;

/**
 * Base descriptor to store an entity mapping: table, property mappings, and side-table/relation joins.
 *
 * @param <C> the entity type
 * @param <T> the main table type
 */
public class Mapping<C, T extends Table<T>> {
	
	private final Class<C> entityType;
	
	private final T table;
	
	private final PropertyMappingHolder<C, T> propertyMappingHolder = new PropertyMappingHolder<>();
	
	private final Set<MappingJoin<?, ?, ?>> relations = new KeepOrderSet<>();
	
	protected Mapping(Class<C> entityType, T table) {
		this.entityType = entityType;
		this.table = table;
	}
	
	public Class<C> getEntityType() {
		return entityType;
	}
	
	public T getTable() {
		return table;
	}
	
	public PropertyMappingHolder<C, T> getPropertyMappingHolder() {
		return propertyMappingHolder;
	}
	
	public Set<MappingJoin<?, ?, ?>> getRelations() {
		return relations;
	}
	
	public void addRelation(MappingJoin<?, ?, ?> relation) {
		relations.add(relation);
	}
}
