package org.codefilarete.stalactite.engine.configurer.model;

import org.codefilarete.stalactite.sql.ddl.structure.Table;

public abstract class MappingJoin<LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINTYPE> {
	
	private final RelationJoin join;
	
	public MappingJoin(RelationJoin join) {
		this.join = join;
	}
	
	public RelationJoin getJoin() {
		return join;
	}
}
