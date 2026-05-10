package org.codefilarete.stalactite.engine.configurer.model;

import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

public class DirectRelationJoin<LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINTYPE>
		implements RelationJoin {
	
	private final KeyMapping<LEFTTABLE, RIGHTTABLE, JOINTYPE> keyMapping;
	
	public DirectRelationJoin(KeyMapping<LEFTTABLE, RIGHTTABLE, JOINTYPE> keyMapping) {
		this.keyMapping = keyMapping;
	}
	
	public DirectRelationJoin(Key<LEFTTABLE, JOINTYPE> keyMapping, Key<RIGHTTABLE, JOINTYPE> rightKey) {
		this(keyMapping.reference(rightKey));
	}
	
	public Key<LEFTTABLE, JOINTYPE> getLeftKey() {
		return keyMapping.getLeftKey();
	}
	
	public Key<RIGHTTABLE, JOINTYPE> getRightKey() {
		return keyMapping.getRightKey();
	}
	
}
