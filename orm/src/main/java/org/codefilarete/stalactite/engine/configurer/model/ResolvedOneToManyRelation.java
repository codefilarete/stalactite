package org.codefilarete.stalactite.engine.configurer.model;

import java.util.Collection;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.codefilarete.reflection.PropertyMutator;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationTable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

/**
 * {@link ComponentRelation} dedicated to one-to-many
 *
 * @param <SRC> the entity type owning the relation
 * @param <TRGT> the entity type related to the owning entity type
 * @param <LEFTTABLE> the entity table type owning the relation
 * @param <RIGHTTABLE> the table type of the related entity type
 * @author Guillaume Mary
 */
public class ResolvedOneToManyRelation<SRC, TRGT, S extends Collection<TRGT>, SRCID, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
		extends ComponentRelation<SRC, TRGT, S, LEFTTABLE, RIGHTTABLE, SRCID> {
	
	private final Entity<TRGT, TRGTID, RIGHTTABLE> targetEntity;
	
	private final boolean ownedByReverseSide;
	
	@Nullable
	private final PropertyMutator<TRGT, SRC> mappedByAccessor;
	
	private final Column<RIGHTTABLE, Integer> indexColumn;
	
	public ResolvedOneToManyRelation(Entity<TRGT, TRGTID, RIGHTTABLE> targetEntity,
	                                 ReadWritePropertyAccessPoint<SRC, S> accessor,
	                                 RelationMode relationMode,
	                                 boolean ownedByReverseSide,
	                                 @Nullable PropertyMutator<TRGT, SRC> mappedByAccessor,
	                                 boolean fetchSeparately,
	                                 RelationJoin join,
	                                 BeanRelationFixer<SRC, TRGT> beanRelationFixer,
	                                 Supplier<S> collectionFactory,
									 @Nullable Column<RIGHTTABLE, Integer> indexColumn) {
		super(accessor, relationMode, fetchSeparately, join, beanRelationFixer, collectionFactory);
		this.targetEntity = targetEntity;
		this.mappedByAccessor = mappedByAccessor;
		this.ownedByReverseSide = ownedByReverseSide;
		this.indexColumn = indexColumn;
	}
	
	public Entity<TRGT, TRGTID, RIGHTTABLE> getTargetEntity() {
		return targetEntity;
	}
	
	public boolean isOwnedByReverseSide() {
		return ownedByReverseSide;
	}
	
	@Nullable
	public PropertyMutator<TRGT, SRC> getMappedByAccessor() {
		return mappedByAccessor;
	}
	
	public boolean isOrdered() {
		return indexColumn != null;
	}
	
	/**
	 * Gives the column used to index the related entities.
	 * Only valuable for relations owned by the reverse side because it can be configured by the user. For relations
	 * mapped by an association table, it is automatically generated.
	 * 
	 * @return the column used to index the related entities for relations owned by the reverse side
	 */
	@Nullable
	public Column<RIGHTTABLE, Integer> getIndexingMappedColumn() {
		return indexColumn;
	}
	
	@Nullable
	public <ASSOCIATIONTABLE extends IndexedAssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>> Column<ASSOCIATIONTABLE, Integer> getIndexingAssociationColumn() {
		return ((ASSOCIATIONTABLE) ((IntermediaryRelationJoin) getJoin()).getJoinTable()).getIndexColumn();
	}
	
}
