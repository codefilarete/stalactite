package org.codefilarete.stalactite.engine.configurer.model;

import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Describes a join of a relation.
 * Can be either a single join between 2 tables, either a join with an intermediary table for *-to-many cases
 */
public abstract class RelationJoin<LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINTYPE> {
	
	private final KeyMapping<LEFTTABLE, RIGHTTABLE, JOINTYPE> keyMapping;
	
	public RelationJoin(KeyMapping<LEFTTABLE, RIGHTTABLE, JOINTYPE> keyMapping) {
		this.keyMapping = keyMapping;
	}
	
	public RelationJoin(Key<LEFTTABLE, JOINTYPE> keyMapping, Key<RIGHTTABLE, JOINTYPE> rightKey) {
		this(keyMapping.reference(rightKey));
	}
	
	public Key<LEFTTABLE, JOINTYPE> getLeftKey() {
		return keyMapping.getLeftKey();
	}
	
	public Key<RIGHTTABLE, JOINTYPE> getRightKey() {
		return keyMapping.getRightKey();
	}
}
