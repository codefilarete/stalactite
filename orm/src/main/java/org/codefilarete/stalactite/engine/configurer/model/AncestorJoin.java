package org.codefilarete.stalactite.engine.configurer.model;

import org.codefilarete.stalactite.sql.ddl.structure.Table;

/**
 * Represents a parent entity of another one, from a mapped-superclass perspective (not as polymorphic one)
 * 
 * @param <SRC> the entity type of this parent type (a super type of the child one)
 * @param <LEFTTABLE> the type of table owning the child entity
 * @param <RIGHTTABLE> the type of the table owning this parent entity
 * @param <JOINTYPE> the type of the join between the two tables, expected to be the primary key type
 */
public class AncestorJoin<SRC, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINTYPE>
		extends MappingJoin<LEFTTABLE, RIGHTTABLE, JOINTYPE> {
	
	private final Entity<SRC, JOINTYPE, RIGHTTABLE> ancestor;
	
	public AncestorJoin(Entity<SRC, JOINTYPE, RIGHTTABLE> ancestor, DirectRelationJoin<LEFTTABLE, RIGHTTABLE, JOINTYPE> join) {
		super(join);
		this.ancestor = ancestor;
	}
	
	public Entity<SRC, JOINTYPE, RIGHTTABLE> getAncestor() {
		return ancestor;
	}
	
	@Override
	public DirectRelationJoin<LEFTTABLE, RIGHTTABLE, JOINTYPE> getJoin() {
		return (DirectRelationJoin<LEFTTABLE, RIGHTTABLE, JOINTYPE>) super.getJoin();
	}
}
