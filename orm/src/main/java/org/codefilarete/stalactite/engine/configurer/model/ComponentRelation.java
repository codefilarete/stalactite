package org.codefilarete.stalactite.engine.configurer.model;

import java.util.function.Supplier;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

public abstract class ComponentRelation<SRC, TRGT, S, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINTYPE>
		extends Relation<SRC, S, LEFTTABLE, RIGHTTABLE, JOINTYPE> {
	
	private final BeanRelationFixer<SRC, TRGT> relationFixer;
	
	private final Supplier<S> componentFactory;
	
	protected ComponentRelation(ReadWritePropertyAccessPoint<SRC, S> accessor,
	                            RelationMode relationMode,
	                            boolean fetchSeparately,
	                            RelationJoin join,
	                            BeanRelationFixer<SRC, TRGT> relationFixer,
	                            Supplier<S> componentFactory) {
		super(accessor, relationMode, fetchSeparately, join);
		this.relationFixer = relationFixer;
		this.componentFactory = componentFactory;
	}
	
	@Override
	public BeanRelationFixer<SRC, TRGT> getRelationFixer() {
		return relationFixer;
	}
	
	public Supplier<S> getComponentFactory() {
		return componentFactory;
	}
}
