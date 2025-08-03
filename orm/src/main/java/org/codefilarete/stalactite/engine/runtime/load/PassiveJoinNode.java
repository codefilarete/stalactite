package org.codefilarete.stalactite.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.IdentityHashMap;
import java.util.Set;

import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType;
import org.codefilarete.stalactite.mapping.RowTransformer.TransformerListener;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.JoinLink;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Reflections;

/**
 * Join node that does nothing particular except eventually triggering a {@link TransformerListener} (if given through
 * {@link #setConsumptionListener(EntityTreeJoinNodeConsumptionListener)})
 * 
 * @author Guillaume Mary
 */
public class PassiveJoinNode<C, T1 extends Fromable, T2 extends Fromable, I> extends AbstractJoinNode<C, T1, T2, I> {
	
	PassiveJoinNode(JoinNode<?, T1> parent,
					JoinLink<T1, I> leftJoinColumn,
					JoinLink<T2, I> rightJoinColumn,
					JoinType joinType,
					Set<? extends Selectable<?>> columnsToSelect,	// of T2
					@Nullable String tableAlias) {
		super(parent, leftJoinColumn, rightJoinColumn, joinType, columnsToSelect, tableAlias);
	}
	
	PassiveJoinNode(JoinNode<?, T1> parent,
					Key<T1, I> leftJoinColumn,
					Key<T2, I> rightJoinColumn,
					JoinType joinType,
					Set<? extends Selectable<?>> columnsToSelect,	// of T2
					@Nullable String tableAlias) {
		super(parent, leftJoinColumn, rightJoinColumn, joinType, columnsToSelect, tableAlias);
	}
	
	PassiveJoinNode(JoinNode<?, T1> parent,
					Key<T1, I> leftJoinColumn,
					Key<T2, I> rightJoinColumn,
					JoinType joinType,
					Set<? extends Selectable<?>> columnsToSelect,	// of T2
					@Nullable String tableAlias,
					IdentityHashMap<Selectable<?>, Selectable<?>> columnClones) {
		super(parent, leftJoinColumn, rightJoinColumn, joinType, columnsToSelect, tableAlias, columnClones);
	}
	
	@Override
	public JoinRowConsumer toConsumer(JoinNode<C, T2> joinNode) {
		return new PassiveJoinRowConsumer(getConsumptionListener(), joinNode);
	}
	
	public class PassiveJoinRowConsumer implements JoinRowConsumer {
		
		/** Optional listener of ResultSet decoding */
		@Nullable
		private final EntityTreeJoinNodeConsumptionListener<C> consumptionListener;
		/** Used when transformerListener is not null */
		private final JoinNode<C, ?> joinNode;
		
		public PassiveJoinRowConsumer(@Nullable EntityTreeJoinNodeConsumptionListener<C> consumptionListener, JoinNode<C, ?> joinNode) {
			this.consumptionListener = consumptionListener;
			this.joinNode = joinNode;
		}

		@Override
		public JoinNode<C, ?> getNode() {
			return PassiveJoinNode.this;
		}
		
		void consume(C parentJoinEntity, ColumnedRow row) {
			if (this.consumptionListener != null) {
				this.consumptionListener.onNodeConsumption(parentJoinEntity, row);
			}
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
