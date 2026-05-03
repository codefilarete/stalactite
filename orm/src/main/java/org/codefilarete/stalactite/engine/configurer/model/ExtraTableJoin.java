package org.codefilarete.stalactite.engine.configurer.model;

import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * {@link MappingJoin} dedicated to properties stored on a side table of the entity main one.
 * 
 * @param <SRC>
 * @param <LEFTTABLE>
 * @param <RIGHTTABLE>
 * @param <JOINTYPE>
 */
public class ExtraTableJoin<SRC, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINTYPE>
		extends MappingJoin<LEFTTABLE, RIGHTTABLE, JOINTYPE> {
	
	private final PropertyMappingHolder<SRC, RIGHTTABLE> propertyMappingHolder;
	
	public ExtraTableJoin(PropertyMappingHolder<SRC, RIGHTTABLE> propertyMapping,
	                      KeyMapping<LEFTTABLE, RIGHTTABLE, JOINTYPE> leftKey) {
		super(new DirectRelationJoin<>(leftKey));
		this.propertyMappingHolder = propertyMapping;
	}
	
	@Override
	public DirectRelationJoin<LEFTTABLE, RIGHTTABLE, JOINTYPE> getJoin() {
		return (DirectRelationJoin<LEFTTABLE, RIGHTTABLE, JOINTYPE>) super.getJoin();
	}
	
	public PropertyMappingHolder<SRC, RIGHTTABLE> getPropertyMappingHolder() {
		return propertyMappingHolder;
	}
	
}
