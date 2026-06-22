package org.codefilarete.stalactite.engine.configurer.model;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

/**
 * {@link ComponentRelation} dedicated to many-to-one.
 * Many-to-one relations are always represented with a foreign key on the right table.
 *
 * @param <SRC> the entity type owning the collection
 * @param <TRGT> the entity type stored in the collection
 * @param <TRGTID> the target entity identifier type
 * @param <LEFTTABLE> the source table type
 * @param <RIGHTTABLE> the target table type
 * @author Guillaume Mary
 */
public class ResolvedManyToOneRelation<
		SRC, TRGT,
		TRGTID,
		LEFTTABLE extends Table<LEFTTABLE>,
		RIGHTTABLE extends Table<RIGHTTABLE>>
		extends EntityRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, TRGTID> {
	
	private final boolean nullable;
	
	public ResolvedManyToOneRelation(Entity<TRGT, TRGTID, RIGHTTABLE> targetEntity,
	                                 ReadWritePropertyAccessPoint<SRC, TRGT> accessor,
	                                 RelationMode relationMode,
	                                 boolean fetchSeparately,
	                                 DirectRelationJoin<LEFTTABLE, RIGHTTABLE, TRGTID> join,
	                                 BeanRelationFixer<SRC, TRGT> beanRelationFixer, boolean nullable) {
		super(targetEntity, accessor, relationMode, fetchSeparately, join, beanRelationFixer);
		this.nullable = nullable;
	}
	
	public boolean isNullable() {
		return nullable;
	}
}
