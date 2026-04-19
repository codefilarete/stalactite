package org.codefilarete.stalactite.engine.configurer.model;

import org.codefilarete.stalactite.sql.ddl.structure.Table;

public class MergeJoin<SRC, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINTYPE>
		extends MappingJoin<LEFTTABLE, RIGHTTABLE, JOINTYPE> {
	
	private final PropertyMappingHolder<SRC, RIGHTTABLE> propertyMappingHolder = new PropertyMappingHolder<>();
	
	public MergeJoin(DirectRelationJoin<LEFTTABLE, RIGHTTABLE, JOINTYPE> join) {
		super(join);
	}
	
	public PropertyMappingHolder<SRC, RIGHTTABLE> getPropertyMappingHolder() {
		return propertyMappingHolder;
	}
	
}
