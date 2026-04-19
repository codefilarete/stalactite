package org.codefilarete.stalactite.engine.configurer.model;

import java.util.Collection;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

public class OneToManyRelation<
		SRC, TRGT,
		S extends Collection<TRGT>,
		SRCID, TRGTID,
		LEFTTABLE extends Table<LEFTTABLE>,
		RIGHTTABLE extends Table<RIGHTTABLE>>
		extends ComponentRelation<SRC, TRGT, S, LEFTTABLE, RIGHTTABLE, SRCID> {
	
	private final Entity<TRGT, ?, ?> targetEntity;
	
	@Nullable
	private final ReversibleAccessor<SRC, TRGT> mappedByAccessor;
	
	public OneToManyRelation(Entity<TRGT, ?, ?> targetEntity,
	                         ReversibleAccessor<SRC, S> accessor,
	                         @Nullable ReversibleAccessor<SRC, TRGT> mappedByAccessor,
	                         RelationMode relationMode,
	                         boolean fetchSeparately,
	                         DirectRelationJoin<LEFTTABLE, RIGHTTABLE, SRCID> join,
	                         BeanRelationFixer<SRC, TRGT> beanRelationFixer,
	                         Supplier<S> collectionFactory) {
		super(accessor, relationMode, fetchSeparately, join, beanRelationFixer, collectionFactory);
		this.targetEntity = targetEntity;
		this.mappedByAccessor = mappedByAccessor;
	}
	
	public Entity<TRGT, ?, ?> getTargetEntity() {
		return targetEntity;
	}
	
	@Nullable
	public ReversibleAccessor<SRC, TRGT> getMappedByAccessor() {
		return mappedByAccessor;
	}
}
