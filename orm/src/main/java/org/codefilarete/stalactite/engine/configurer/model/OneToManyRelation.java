package org.codefilarete.stalactite.engine.configurer.model;

import java.util.Collection;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
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
	private final ReadWritePropertyAccessPoint<SRC, TRGT> mappedByAccessor;
	
	public OneToManyRelation(Entity<TRGT, ?, ?> targetEntity,
	                         ReadWritePropertyAccessPoint<SRC, S> accessor,
	                         @Nullable ReadWritePropertyAccessPoint<SRC, TRGT> mappedByAccessor,
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
	public ReadWritePropertyAccessPoint<SRC, TRGT> getMappedByAccessor() {
		return mappedByAccessor;
	}
}
