package org.codefilarete.stalactite.engine.configurer.model;

import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

public class DirectRelationJoin<LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINTYPE>
		extends RelationJoin<LEFTTABLE, RIGHTTABLE, JOINTYPE> {
	
	public DirectRelationJoin(Key<LEFTTABLE, JOINTYPE> leftKey,
							  Key<RIGHTTABLE, JOINTYPE> rightKey) {
		super(leftKey, rightKey);
	}
	
	public DirectRelationJoin(KeyMapping<LEFTTABLE, RIGHTTABLE, JOINTYPE> foreignKey) {
		super(foreignKey);
	}
	
}
