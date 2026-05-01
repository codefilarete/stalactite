package org.codefilarete.stalactite.engine.configurer.model;

import javax.annotation.Nullable;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
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
public abstract class EntityRelation<SRC, TRGT, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINTYPE> extends Relation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINTYPE> {
	
	private final Entity<TRGT, ?, RIGHTTABLE> targetEntity;
	
	@Nullable
	private final ReadWritePropertyAccessPoint<TRGT, SRC> mappedByAccessor;
	
	private final BeanRelationFixer<SRC, TRGT> beanRelationFixer;
	
	public EntityRelation(Entity<TRGT, ?, RIGHTTABLE> targetEntity,
	                      ReadWritePropertyAccessPoint<SRC, TRGT> accessor,
						  @Nullable ReadWritePropertyAccessPoint<TRGT, SRC> mappedByAccessor,
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
	
	public <TRGTID> Entity<TRGT, TRGTID, RIGHTTABLE> getTargetEntity() {
		return (Entity<TRGT, TRGTID, RIGHTTABLE>) targetEntity;
	}
	
	@Nullable
	public ReadWritePropertyAccessPoint<TRGT, SRC> getMappedByAccessor() {
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
