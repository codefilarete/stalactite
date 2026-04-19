package org.codefilarete.stalactite.engine.configurer.model;

import javax.annotation.Nullable;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

/**
 * Made for
 * - one-to-one,
 * - secondary-table,
 * - mapped-superclass cases.
 * 
 * @param <SRC>
 * @param <TRGT>
 * @author Guillaume Mary
 */
public class EntityRelation<SRC, TRGT, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINTYPE> extends Relation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINTYPE> {
	
	private final Entity<TRGT, ?, ?> targetEntity;
	
	@Nullable
	private final ReversibleAccessor<SRC, TRGT> mappedByAccessor;
	
	private final BeanRelationFixer<SRC, TRGT> beanRelationFixer;
	
	public EntityRelation(Entity<TRGT, ?, RIGHTTABLE> targetEntity,
						  ReversibleAccessor<SRC, TRGT> accessor,
						  @Nullable ReversibleAccessor<SRC, TRGT> mappedByAccessor,
						  RelationMode relationMode,
						  boolean fetchSeparately,
						  // JOINTYPE can be either SRC PK or TARGET PK for one-to-one. Can only by SRC PK for secondary-table and mapped-superclass
						  DirectRelationJoin<LEFTTABLE, RIGHTTABLE, JOINTYPE> join,
						  BeanRelationFixer<SRC, TRGT> beanRelationFixer) {
		super(accessor, relationMode, fetchSeparately, join);
		this.targetEntity = targetEntity;
		this.mappedByAccessor = mappedByAccessor;
		this.beanRelationFixer = beanRelationFixer;
	}
	
	public Entity<TRGT, ?, ?> getTargetEntity() {
		return targetEntity;
	}
	
	@Nullable
	public ReversibleAccessor<SRC, TRGT> getMappedByAccessor() {
		return mappedByAccessor;
	}
	
	@Override
	public DirectRelationJoin<LEFTTABLE, RIGHTTABLE, JOINTYPE> getJoin() {
		return (DirectRelationJoin<LEFTTABLE, RIGHTTABLE, JOINTYPE>) super.getJoin();
	}
	
	@Override
	public BeanRelationFixer<SRC, TRGT> getBeanRelationFixer() {
		return beanRelationFixer;
	}
}
