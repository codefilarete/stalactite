package org.codefilarete.stalactite.engine.configurer.model;

import java.util.Map;
import java.util.function.Supplier;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.Duo;

public class MapRelation<SRC, K, V, S extends Map<K, V>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, SRCID> extends ComponentRelation<SRC, Duo<K, V>, S, LEFTTABLE, RIGHTTABLE, SRCID> {
	
	public MapRelation(ReversibleAccessor<SRC, S> accessor,
					   RelationMode relationMode,
					   boolean fetchSeparately,
					   DirectRelationJoin<LEFTTABLE, RIGHTTABLE, SRCID> join,
					   BeanRelationFixer<SRC, Duo<K, V>> beanRelationFixer,
					   Supplier<S> componentFactory) {
		super(accessor, relationMode, fetchSeparately, join, beanRelationFixer, componentFactory);
	}
}
