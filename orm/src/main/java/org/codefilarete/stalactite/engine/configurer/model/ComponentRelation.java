package org.codefilarete.stalactite.engine.configurer.model;

import java.util.function.Supplier;

import org.codefilarete.reflection.ReadWriteAccessPoint;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

public abstract class ComponentRelation<SRC, TRGT, S, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINTYPE>
		extends Relation<SRC, S, LEFTTABLE, RIGHTTABLE, JOINTYPE> {
	
	private final BeanRelationFixer<SRC, TRGT> beanRelationFixer;
	
	private final Supplier<S> componentFactory;
	
	private Column<?, Integer> indexingColumn;
	
	protected ComponentRelation(ReadWriteAccessPoint<SRC, S> accessor,
	                            RelationMode relationMode,
	                            boolean fetchSeparately,
	                            RelationJoin<LEFTTABLE, RIGHTTABLE, JOINTYPE> join,
	                            BeanRelationFixer<SRC, TRGT> beanRelationFixer,
	                            Supplier<S> componentFactory) {
		super(accessor, relationMode, fetchSeparately, join);
		this.beanRelationFixer = beanRelationFixer;
		this.componentFactory = componentFactory;
	}
	
	@Override
	public BeanRelationFixer<SRC, TRGT> getBeanRelationFixer() {
		return beanRelationFixer;
	}
}
