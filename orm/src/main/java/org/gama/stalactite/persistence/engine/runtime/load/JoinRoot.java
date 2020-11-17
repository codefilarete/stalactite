package org.gama.stalactite.persistence.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

import org.gama.lang.collection.ReadOnlyList;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.EntityInflater;
import org.gama.stalactite.persistence.engine.runtime.load.RelationJoinNode.EntityCache;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.persistence.mapping.IRowTransformer;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.result.Row;

/**
 * Very first table (and its joins) of a from clause
 * 
 * @author Guillaume Mary
 */
public class JoinRoot<C, I, T extends Table> implements JoinNode<T> {
	
	/** Root entity inflater */
	private final EntityInflater<C, I, T> entityInflater;
	
	private final T table;
	
	/** Joins */
	private final List<AbstractJoinNode> joins = new ArrayList<>();
	
	@Nullable
	private String tableAlias;
	
	public JoinRoot(EntityInflater<C, I, T> entityInflater, T table) {
		this.entityInflater = entityInflater;
		this.table = table;
	}
	
	public EntityInflater<C, I, T> getEntityInflater() {
		return entityInflater;
	}
	
	@Override
	public T getTable() {
		return table;
	}
	
	@Override
	public ReadOnlyList<AbstractJoinNode> getJoins() {
		return new ReadOnlyList<>(joins);
	}
	
	@Override
	public void add(AbstractJoinNode node) {
		this.joins.add(node);
	}
	
	@Nullable
	public String getTableAlias() {
		return tableAlias;
	}
	
	JoinRootRowConsumer<C, I> toConsumer(ColumnedRow columnedRow) {
		return new JoinRootRowConsumer<>(entityInflater, entityInflater.copyTransformerWithAliases(columnedRow), columnedRow);
	}
	
	static class JoinRootRowConsumer<C, I> implements JoinRowConsumer {
		
		/** Root entity inflater */
		private final EntityInflater<C, I, Table> entityInflater;
		
		private final IRowTransformer<C> entityBuilder;
		
		private final ColumnedRow columnedRow;
		
		JoinRootRowConsumer(EntityInflater<C, I, ?> entityInflater, IRowTransformer<C> entityBuilder, ColumnedRow columnedRow) {
			this.entityInflater = (EntityInflater<C, I, Table>) entityInflater;
			this.entityBuilder = entityBuilder;
			this.columnedRow = columnedRow;
		}
		
		C createRootInstance(Row row, EntityCache entityCache) {
			Object identifier = entityInflater.giveIdentifier(row, columnedRow);
			if (identifier == null) {
				return null;
			} else {
				return entityCache.computeIfAbsent(entityInflater.getEntityType(), identifier, () -> entityBuilder.transform(row));
			}
		}
	} 
}
