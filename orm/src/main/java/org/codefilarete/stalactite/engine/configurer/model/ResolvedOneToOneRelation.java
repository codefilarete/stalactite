package org.codefilarete.stalactite.engine.configurer.model;

import javax.annotation.Nullable;

import org.codefilarete.reflection.ReadWriteAccessPoint;
import org.codefilarete.stalactite.dsl.property.CascadeOptions;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

/**
 * {@link EntityRelation} dedicated to one-to-one
 * 
 * @param <SRC> the entity type owning the relation
 * @param <TRGT> the entity type related to the owning entity type
 * @param <LEFTTABLE> the entity table type owning the relation
 * @param <RIGHTTABLE> the table type of the related entity type
 * @param <JOINTYPE> the type of join used to link the two entities (either source primary or target primary key)
 * @author Guillaume Mary
 */
public class ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINTYPE>
		extends EntityRelation<SRC, TRGT, LEFTTABLE , RIGHTTABLE, JOINTYPE> {
	
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
