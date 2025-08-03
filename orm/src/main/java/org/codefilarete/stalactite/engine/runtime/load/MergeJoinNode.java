package org.codefilarete.stalactite.engine.runtime.load;

import javax.annotation.Nullable;

import java.util.IdentityHashMap;
import java.util.Set;

import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.engine.runtime.load.PassiveJoinNode.PassiveJoinRowConsumer;
import org.codefilarete.stalactite.mapping.RowTransformer;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Reflections;

/**
 * Join node that fulfill some bean properties from a database row.
 * 
 * @author Guillaume Mary
 */
public class MergeJoinNode<C, T1 extends Fromable, T2 extends Fromable, I> extends AbstractJoinNode<C, T1, T2, I> {
	
	private final EntityMerger<C> merger;
	
	public MergeJoinNode(JoinNode<?, T1> parent,
						 Key<T1, I> leftJoinColumn,
						 Key<T2, I> rightJoinColumn,
						 JoinType joinType,
						 @Nullable String tableAlias,
						 EntityMerger<C> merger) {
		this(parent, leftJoinColumn, rightJoinColumn, joinType, tableAlias, merger, merger.getSelectableColumns());
	}
	
	public MergeJoinNode(JoinNode<?, T1> parent,
						 Key<T1, I> leftJoinColumn,
						 Key<T2, I> rightJoinColumn,
						 JoinType joinType,
						 @Nullable String tableAlias,
						 EntityMerger<C> merger,
						 Set<Selectable<?>> selectableColumns) {
		super(parent, leftJoinColumn, rightJoinColumn, joinType, selectableColumns, tableAlias);
		this.merger = merger;
	}
	
	public MergeJoinNode(JoinNode<?, T1> parent,
						 Key<T1, I> leftJoinColumn,
						 Key<T2, I> rightJoinColumn,
						 JoinType joinType,
						 @Nullable String tableAlias,
						 EntityMerger<C> merger,
						 Set<Selectable<?>> selectableColumns,
						 IdentityHashMap<Selectable<?>, Selectable<?>> columnClones) {
		super(parent, leftJoinColumn, rightJoinColumn, joinType, selectableColumns, tableAlias, columnClones);
		this.merger = merger;
	}
	
	public EntityMerger<C> getMerger() {
		return merger;
	}
	
	@Override
	public MergeJoinRowConsumer<C> toConsumer(JoinNode<C, T2> joinNode) {
		return new MergeJoinRowConsumer<>((MergeJoinNode<C, ?, ?, ?>) joinNode, merger.getRowTransformer());
	}
	
	/**
	 * Design tip : Technically same as {@link PassiveJoinRowConsumer} but goal is different, that's why it has its own class 
	 * 
	 * @param <C>
	 */
	public static class MergeJoinRowConsumer<C> implements JoinRowConsumer {
		
		private final MergeJoinNode<C, ?, ?, ?> node;
		protected final RowTransformer<C> merger;
		
		public MergeJoinRowConsumer(MergeJoinNode<C, ?, ?, ?> node, RowTransformer<C> merger) {
			this.node = node;
			this.merger = merger;
		}
		
		@Override
		public MergeJoinNode<C, ?, ?, ?> getNode() {
			return node;
		}
		
		void mergeProperties(C parentJoinEntity, ColumnedRow row) {
			this.merger.applyRowToBean(row, parentJoinEntity);
		}
		
		/**
		 * Implemented for debug. DO NOT RELY ON IT for anything else.
		 */
		@Override
		public String toString() {
			return Reflections.toString(this.getClass());
		}
	}
}
