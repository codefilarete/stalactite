package org.codefilarete.stalactite.engine.configurer.model;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

public abstract class Relation<SRC, TRGT, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINTYPE>
		extends MappingJoin<LEFTTABLE, RIGHTTABLE, JOINTYPE> {
	
	private final ReversibleAccessor<SRC, TRGT> accessor;
	
	private final boolean fetchSeparately;
	
	private final RelationMode relationMode;
	
	private final RelationJoin<LEFTTABLE, RIGHTTABLE, JOINTYPE> join;
	
	protected Relation(ReversibleAccessor<SRC, TRGT> accessor,
	                   RelationMode relationMode,
					   boolean fetchSeparately,
	                   RelationJoin<LEFTTABLE, RIGHTTABLE, JOINTYPE> join) {
		super(join);
		this.accessor = accessor;
		this.fetchSeparately = fetchSeparately;
		this.relationMode = relationMode;
		this.join = join;
	}
	
	/**
	 * The function to be called with the current entity and the target one as arguments to fulfill the relation.
	 * Made of current relation readWriteAccessPoint and mapped-by one.
	 * Should also take into account Collection cases for *-to-many relation, or Map ones. As a consequence, the input
	 * of it can't be of TRGT type, but must be quite a 
	 * 
	 */
	public abstract BeanRelationFixer<SRC, ?> getBeanRelationFixer();
	
	public RelationJoin<LEFTTABLE, RIGHTTABLE, JOINTYPE> getJoin() {
		return join;
	}
}
