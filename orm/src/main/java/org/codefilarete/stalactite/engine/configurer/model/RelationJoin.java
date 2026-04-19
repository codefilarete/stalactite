package org.codefilarete.stalactite.engine.configurer.model;

import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Describes a join of a relation.
 * Can be either a single join between 2 tables, either a join with an intermediary table for *-to-many cases
 */
public abstract class RelationJoin<LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINTYPE> {
	
	private final Key<LEFTTABLE, JOINTYPE> leftKey;
	
	private final Key<RIGHTTABLE, JOINTYPE> rightKey;
	
	public RelationJoin(Key<LEFTTABLE, JOINTYPE> leftKey, Key<RIGHTTABLE, JOINTYPE> rightKey) {
		this.leftKey = leftKey;
		this.rightKey = rightKey;
	}
	
	public Key<LEFTTABLE, JOINTYPE> getLeftKey() {
		return leftKey;
	}
	
	public Key<RIGHTTABLE, JOINTYPE> getRightKey() {
		return rightKey;
	}
}
