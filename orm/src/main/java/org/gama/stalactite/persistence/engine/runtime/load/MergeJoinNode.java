package org.gama.stalactite.persistence.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.Set;

import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.EntityMerger;
import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.JoinType;
import org.gama.stalactite.persistence.engine.runtime.load.PassiveJoinNode.PassiveJoinRowConsumer;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.persistence.mapping.RowTransformer;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.result.Row;

/**
 * Join node that fulfill some bean properties from a database row.
 * 
 * @author Guillaume Mary
 */
class MergeJoinNode<C, T1 extends Table, T2 extends Table, I> extends AbstractJoinNode<C, T1, T2, I> {
	
	private final EntityMerger<C, T2> merger;
	
	MergeJoinNode(JoinNode<T1> parent,
							Column<T1, I> leftJoinColumn,
							Column<T2, I> rightJoinColumn,
							JoinType joinType,
							@Nullable String tableAlias,
							EntityMerger<C, T2> merger) {
		super(parent, leftJoinColumn, rightJoinColumn, joinType, (Set) merger.getSelectableColumns(), tableAlias);
		this.merger = merger;
	}
	
	EntityMerger<C, T2> getMerger() {
		return merger;
	}
	
	@Override
	public JoinRowConsumer toConsumer(ColumnedRow columnedRow) {
		return new MergeJoinRowConsumer<>(merger.copyTransformerWithAliases(columnedRow));
	}
	
	/**
	 * Design tip : Technically same as {@link PassiveJoinRowConsumer} but goal is different, that's why it has its own class 
	 * 
	 * @param <C>
	 */
	static class MergeJoinRowConsumer<C> implements JoinRowConsumer {
		
		private final RowTransformer<C> merger;
		
		public MergeJoinRowConsumer(RowTransformer<C> merger) {
			this.merger = merger;
		}
		
		void mergeProperties(C parentJoinEntity, Row row) {
			this.merger.applyRowToBean(row, parentJoinEntity);
		}
	}
}
