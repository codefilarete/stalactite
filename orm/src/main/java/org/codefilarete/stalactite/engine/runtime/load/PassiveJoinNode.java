package org.codefilarete.stalactite.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.Set;

import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.RowTransformer.TransformerListener;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.JoinLink;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.result.Row;

/**
 * Join node that does nothing particular except eventually triggering a {@link TransformerListener} (if given through
 * {@link #setTransformerListener(TransformerListener)})
 * 
 * @author Guillaume Mary
 */
public class PassiveJoinNode<C, T1 extends Fromable, T2 extends Fromable, I> extends AbstractJoinNode<C, T1, T2, I> {
	
	PassiveJoinNode(JoinNode<T1> parent,
					JoinLink<T1, I> leftJoinColumn,
					JoinLink<T2, I> rightJoinColumn,
					JoinType joinType,
					Set<? extends Selectable<?>> columnsToSelect,	// of T2
					@Nullable String tableAlias) {
		super(parent, leftJoinColumn, rightJoinColumn, joinType, columnsToSelect, tableAlias);
	}
	
	@Override
	public JoinRowConsumer toConsumer(ColumnedRow columnedRow) {
		return new PassiveJoinRowConsumer<>(getTransformerListener(), columnedRow);
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
