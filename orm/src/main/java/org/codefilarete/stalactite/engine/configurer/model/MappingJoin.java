package org.codefilarete.stalactite.engine.configurer.model;

import org.codefilarete.stalactite.sql.ddl.structure.Table;

public abstract class MappingJoin<LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINTYPE> {
	
	private final RelationJoin<LEFTTABLE, RIGHTTABLE, JOINTYPE> join;
	
	public MappingJoin(RelationJoin<LEFTTABLE, RIGHTTABLE, JOINTYPE> join) {
		this.join = join;
	}
	
	public RelationJoin<LEFTTABLE, RIGHTTABLE, JOINTYPE> getJoin() {
		return join;
	}
}
