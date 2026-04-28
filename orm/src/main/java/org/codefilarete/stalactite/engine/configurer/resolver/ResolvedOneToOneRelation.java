package org.codefilarete.stalactite.engine.configurer.resolver;

import javax.annotation.Nullable;

import org.codefilarete.reflection.ReadWriteAccessPoint;
import org.codefilarete.stalactite.dsl.property.CascadeOptions;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.EntityRelation;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

public class ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINTYPE>
		extends EntityRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINTYPE> {
	
	private final boolean ownedByTarget;
	
	public ResolvedOneToOneRelation(Entity<TRGT, ?, RIGHTTABLE> targetEntity,
	                                ReadWriteAccessPoint<SRC, TRGT> accessor,
	                                @Nullable ReadWriteAccessPoint<TRGT, SRC> mappedByAccessor,
	                                CascadeOptions.RelationMode relationMode,
	                                boolean fetchSeparately,
	                                DirectRelationJoin<LEFTTABLE, RIGHTTABLE, JOINTYPE> join,
	                                BeanRelationFixer<SRC, TRGT> beanRelationFixer,
	                                boolean ownedByTarget) {
		super(targetEntity, accessor, mappedByAccessor, relationMode, fetchSeparately, join, beanRelationFixer);
		this.ownedByTarget = ownedByTarget;
	}
	
	public boolean isOwnedByTarget() {
		return ownedByTarget;
	}
}
