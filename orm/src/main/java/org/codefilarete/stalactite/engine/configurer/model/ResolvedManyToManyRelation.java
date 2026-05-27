package org.codefilarete.stalactite.engine.configurer.model;

import java.util.Collection;
import java.util.function.Supplier;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationTable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

/**
 * {@link ComponentRelation} dedicated to many-to-many.
 * Many-to-many relations always use an intermediary association table, represented as an
 * {@link IntermediaryRelationJoin}.  When the table carries a positional index column
 * ({@link IndexedAssociationTable}), {@link #isOrdered()} returns {@code true}.
 *
 * @param <SRC>        the entity type owning the collection
 * @param <TRGT>       the entity type stored in the collection
 * @param <S>          the collection type
 * @param <SRCID>      the source entity identifier type
 * @param <TRGTID>     the target entity identifier type
 * @param <LEFTTABLE>  the source table type
 * @param <RIGHTTABLE> the target table type
 * @author Guillaume Mary
 */
public class ResolvedManyToManyRelation<
		SRC, TRGT,
		S extends Collection<TRGT>,
		SRCID, TRGTID,
		LEFTTABLE extends Table<LEFTTABLE>,
		RIGHTTABLE extends Table<RIGHTTABLE>>
		extends ComponentRelation<SRC, TRGT, S, LEFTTABLE, RIGHTTABLE, SRCID> {
	
	private final Entity<TRGT, TRGTID, RIGHTTABLE> targetEntity;
	
	public ResolvedManyToManyRelation(Entity<TRGT, TRGTID, RIGHTTABLE> targetEntity,
	                                  ReadWritePropertyAccessPoint<SRC, S> accessor,
	                                  RelationMode relationMode,
	                                  boolean fetchSeparately,
	                                  IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ?, SRCID, TRGTID> join,
	                                  BeanRelationFixer<SRC, TRGT> beanRelationFixer,
	                                  Supplier<S> collectionFactory) {
		super(accessor, relationMode, fetchSeparately, join, beanRelationFixer, collectionFactory);
		this.targetEntity = targetEntity;
	}
	
	public Entity<TRGT, TRGTID, RIGHTTABLE> getTargetEntity() {
		return targetEntity;
	}
	
	/**
	 * Returns {@code true} when the underlying association table carries a positional index column,
	 * meaning the collection order must be preserved during SELECT.
	 */
	public boolean isOrdered() {
		return ((IntermediaryRelationJoin<?, ?, ?, ?, ?>) getJoin()).getJoinTable() instanceof IndexedAssociationTable;
	}
	
	/**
	 * Returns the index column of the association table.
	 * Only meaningful when {@link #isOrdered()} is {@code true}.
	 *
	 * @param <ASSOCIATIONTABLE> the concrete indexed association table type
	 * @return the index {@link Column}, or {@code null} when the relation is not ordered
	 */
	@SuppressWarnings("unchecked")
	public <ASSOCIATIONTABLE extends IndexedAssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	Column<ASSOCIATIONTABLE, Integer> getIndexingAssociationColumn() {
		return ((ASSOCIATIONTABLE) ((IntermediaryRelationJoin<?, ?, ?, ?, ?>) getJoin()).getJoinTable()).getIndexColumn();
	}
}
