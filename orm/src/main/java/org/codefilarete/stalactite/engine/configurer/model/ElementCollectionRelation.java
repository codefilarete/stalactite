package org.codefilarete.stalactite.engine.configurer.model;

import java.util.Collection;
import java.util.Set;
import java.util.function.Supplier;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

/**
 * Support for element-collection configuration
 * @param <SRC> the entity owning the collection of elements
 * @param <TRGT> the type of elements in the collection, can be a simple type (String, Integer, ...) or an embeddable one
 * @param <S> the type of collection
 * @param <RIGHTTABLE> the type of table owning the collection of elements
 */
public class ElementCollectionRelation<SRC, TRGT, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, SRCID> extends ComponentRelation<SRC, TRGT, S, LEFTTABLE, RIGHTTABLE, SRCID> {
	
	private final Key<RIGHTTABLE, SRCID> reverseColumns;
	
	private final Set<Column<RIGHTTABLE, ?>> elementColumns;
	
	private final Class<TRGT> componentType;
	
	private final RIGHTTABLE collectionTable;
	
	public ElementCollectionRelation(ReversibleAccessor<SRC, S> accessor,
									 RelationMode relationMode,
									 boolean fetchSeparately,
									 DirectRelationJoin<LEFTTABLE, RIGHTTABLE, SRCID> join,
									 BeanRelationFixer<SRC, TRGT> beanRelationFixer,
									 Supplier<S> componentFactory,
									 Key<RIGHTTABLE, SRCID> foreignKeyColumns,
									 Set<Column<RIGHTTABLE, ?>> elementColumns,
									 Class<TRGT> componentType,
									 RIGHTTABLE collectionTable) {
		super(accessor, relationMode, fetchSeparately, join, beanRelationFixer, componentFactory);
		this.reverseColumns = foreignKeyColumns;
		this.elementColumns = elementColumns;
		this.componentType = componentType;
		this.collectionTable = collectionTable;
	}
}
