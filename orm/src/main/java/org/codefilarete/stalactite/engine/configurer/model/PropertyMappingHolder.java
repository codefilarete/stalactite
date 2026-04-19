package org.codefilarete.stalactite.engine.configurer.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.codefilarete.reflection.PropertyMutator;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.engine.configurer.model.Entity.AbstractPropertyMapping;
import org.codefilarete.stalactite.engine.configurer.model.Entity.PropertyMapping;
import org.codefilarete.stalactite.engine.configurer.model.Entity.ReadOnlyPropertyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.KeepOrderSet;

public class PropertyMappingHolder<SRC, RIGHTTABLE extends Table<RIGHTTABLE>> {
	
	private final Set<PropertyMapping<SRC, ?, RIGHTTABLE>> writablePropertyToColumn = new KeepOrderSet<>();
	
	// Could be a Map<Mutator> if we could have an AccessorChain that can be a Mutator that is not currently the case
	private final Set<ReadOnlyPropertyMapping<SRC, ?, RIGHTTABLE>> readonlyPropertyToColumn = new KeepOrderSet<>();
	
	public void addMapping(Iterable<? extends AbstractPropertyMapping<SRC, ?, ?>> propertyMapping) {
		propertyMapping.forEach(this::addMapping);
	}
	
	public void addMapping(AbstractPropertyMapping<SRC, ?, ?> propertyMapping) {
		if (propertyMapping instanceof PropertyMapping) {
			this.writablePropertyToColumn.add((PropertyMapping<SRC, ?, RIGHTTABLE>) propertyMapping);
		} else {
			this.readonlyPropertyToColumn.add((ReadOnlyPropertyMapping<SRC, ?, RIGHTTABLE>) propertyMapping);
		}
	}
	
	public Set<PropertyMapping<SRC, ?, RIGHTTABLE>> getWritablePropertyToColumn() {
		return writablePropertyToColumn;
	}
	
	public Map<ReadWritePropertyAccessPoint<SRC, ?>, Column<RIGHTTABLE, ?>> getWritablePropertiesPerAccessor() {
		Map<ReadWritePropertyAccessPoint<SRC, ?>, Column<RIGHTTABLE, ?>> readonlyPropertyToColumn = new HashMap<>();
		getWritablePropertyToColumn().forEach(propertyMapping -> {
			readonlyPropertyToColumn.put(propertyMapping.getAccessPoint(), propertyMapping.getColumn());
		});
		return readonlyPropertyToColumn;
	}
	
	public Set<ReadOnlyPropertyMapping<SRC, ?, RIGHTTABLE>> getReadonlyPropertyToColumn() {
		return readonlyPropertyToColumn;
	}
	
	public Map<PropertyMutator<SRC, ?>, Column<RIGHTTABLE, ?>> getReadonlyPropertiesPerAccessor() {
		Map<PropertyMutator<SRC, ?>, Column<RIGHTTABLE, ?>> readonlyPropertyToColumn = new HashMap<>();
		getReadonlyPropertyToColumn().forEach(propertyMapping -> {
			readonlyPropertyToColumn.put(propertyMapping.getAccessPoint(), propertyMapping.getColumn());
		});
		return readonlyPropertyToColumn;
	}
}
