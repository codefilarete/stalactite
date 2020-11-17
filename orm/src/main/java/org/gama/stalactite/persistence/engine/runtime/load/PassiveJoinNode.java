package org.gama.stalactite.persistence.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.Set;

import org.gama.stalactite.persistence.engine.runtime.load.EntityJoinTree.JoinType;
import org.gama.stalactite.persistence.mapping.ColumnedRow;
import org.gama.stalactite.persistence.mapping.IRowTransformer.TransformerListener;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.result.Row;

/**
 * Join node that does nothing particular except eventually triggering a {@link TransformerListener} (if given through
 * {@link #setTransformerListener(TransformerListener)})
 * 
 * @author Guillaume Mary
 */
class PassiveJoinNode<C, T1 extends Table, T2 extends Table, I> extends AbstractJoinNode<C, T1, T2, I> {
	
	@Nullable
	private TransformerListener<C> transformerListener;
	
	PassiveJoinNode(JoinNode<T1> parent,
							  Column<T1, I> leftJoinColumn,
							  Column<T2, I> rightJoinColumn,
							  JoinType joinType,
							  Set<Column<T2, Object>> columnsToSelect,
							  @Nullable String tableAlias) {
		super(parent, leftJoinColumn, rightJoinColumn, joinType, columnsToSelect, tableAlias);
	}
	
	public PassiveJoinNode<C, T1, T2, I> setTransformerListener(@Nullable TransformerListener<C> transformerListener) {
		this.transformerListener = transformerListener;
		return this;
	}
	
	@Nullable
	TransformerListener<C> getTransformerListener() {
		return transformerListener;
	}
	
	@Override
	PassiveJoinRowConsumer<C> toConsumer(ColumnedRow columnedRow) {
		return new PassiveJoinRowConsumer<>(transformerListener, columnedRow);
	}
	
	static class PassiveJoinRowConsumer<C> implements JoinRowConsumer {
		
		/** Optional listener of ResultSet decoding */
		@Nullable
		private final TransformerListener<C> transformerListener;
		/** Used when transformerListener is not null, so could be null, but is never because constructor is always used with not null one */
		@Nullable
		private final ColumnedRow columnedRow;
		
		PassiveJoinRowConsumer(@Nullable TransformerListener<C> transformerListener, @Nullable ColumnedRow columnedRow) {
			this.transformerListener = transformerListener;
			this.columnedRow = columnedRow;
		}
		
		void consume(C parentJoinEntity, Row row) {
			if (this.transformerListener != null) {
				this.transformerListener.onTransform(parentJoinEntity, column -> columnedRow.getValue(column, row));
			}
		}
	}
}
