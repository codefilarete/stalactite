package org.codefilarete.stalactite.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.TreeInflationContext;
import org.codefilarete.stalactite.engine.runtime.load.JoinRowConsumer.RootJoinRowConsumer;
import org.codefilarete.stalactite.mapping.RowTransformer;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.JoinLink;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.ReadOnlyList;

/**
 * Very first table (and its joins) of a from clause
 * 
 * @author Guillaume Mary
 */
public class JoinRoot<C, I, T extends Fromable> implements JoinNode<C, T> {

	private final EntityJoinTree<C, I> tree;
	
	/** Root entity inflater */
	private final EntityInflater<C, I> entityInflater;
	
	private final T table;
	
	/** Joins */
	private final List<AbstractJoinNode<?, ?, ?, ?>> joins = new ArrayList<>();
	
	@Nullable
	private String tableAlias;
	
	@Nullable
	private EntityTreeJoinNodeConsumptionListener<C> consumptionListener;

	private final IdentityHashMap<JoinLink<?, ?>, JoinLink<?, ?>> columnClones;
	
	public JoinRoot(EntityJoinTree<C, I> tree, EntityInflater<C, I> entityInflater, T table) {
		this.tree = tree;
		this.entityInflater = entityInflater;
		this.table = table;
		this.columnClones = new IdentityHashMap<>();
		table.getColumns().forEach(column -> {
			// we clone columns to avoid side effects on the original query
			this.columnClones.put((JoinLink<?, ?>) column, (JoinLink<?, ?>) column);
		});
	}
	
	public JoinRoot(EntityJoinTree<C, I> tree, EntityInflater<C, I> entityInflater, T table, IdentityHashMap<? extends JoinLink<?, ?>, ? extends JoinLink<?, ?>> columnClones) {
		this.tree = tree;
		this.entityInflater = entityInflater;
		this.table = table;
		this.columnClones = (IdentityHashMap<JoinLink<?, ?>, JoinLink<?, ?>>) columnClones;
	}
	
	public EntityInflater<C, I> getEntityInflater() {
		return entityInflater;
	}
	
	@Nullable
	EntityTreeJoinNodeConsumptionListener<C> getConsumptionListener() {
		return consumptionListener;
	}
	
	@Override
	public JoinRoot<C, I, T> setConsumptionListener(@Nullable EntityTreeJoinNodeConsumptionListener<C> consumptionListener) {
		this.consumptionListener = consumptionListener;
		return this;
	}
	
	@Override
	public IdentityHashMap<JoinLink<?, ?>, JoinLink<?, ?>> getOriginalColumnsToLocalOnes() {
		return columnClones;
	}

	@Override
	public EntityJoinTree<C, I> getTree() {
		return tree;
	}
	
	@Override
	public T getTable() {
		return table;
	}
	
	@Override
	public Set<Selectable<?>> getColumnsToSelect() {
		return entityInflater.getSelectableColumns();
	}
	
	@Override
	public ReadOnlyList<AbstractJoinNode<?, ?, ?, ?>> getJoins() {
		return new ReadOnlyList<>(joins);
	}
	
	@Override
	public void add(AbstractJoinNode node) {
		this.joins.add(node);
	}
	
	@Nullable
	@Override
	public String getTableAlias() {
		return tableAlias;
	}
	
	@Override
	public RootJoinRowConsumer<C, I> toConsumer(JoinNode<C, T> joinNode) {
		return new JoinRootRowConsumer(this, entityInflater);
	}
	
	public class JoinRootRowConsumer implements RootJoinRowConsumer<C, I> {

		private final JoinRoot<C, ?, ?> joinNode;
		private final Class<C> entityType;
		
		/** Root entity identifier decoder */
		private final Function<ColumnedRow, I> identifierDecoder;
		
		private final RowTransformer<C> entityBuilder;
		
		public JoinRootRowConsumer(JoinRoot<C, ?, ?> joinNode, EntityInflater<C, I> entityInflater) {
			this.joinNode = joinNode;
			this.entityType = entityInflater.getEntityType();
			this.identifierDecoder = entityInflater::giveIdentifier;
			this.entityBuilder = entityInflater.getRowTransformer();
		}

		@Override
		public JoinRoot<C, ?, ?> getNode() {
			return joinNode;
		}

		@Override
		public EntityReference<C, I> createRootInstance(ColumnedRow row, TreeInflationContext context) {
			I identifier = identifierDecoder.apply(row);
			EntityReference<C, I> entityReference = null;
			if (identifier != null) {
				C entity = context.giveEntityFromCache(entityType, identifier, () -> entityBuilder.transform(row));
				entityReference = new EntityReference<>(entity, identifier);
				if (getConsumptionListener() != null) {
					getConsumptionListener().onNodeConsumption(entity, row);
				}
			}
			return entityReference;
		}
		
		/**
		 * Implemented for debug. DO NOT RELY ON IT for anything else.
		 */
		@Override
		public String toString() {
			return Reflections.toString(this.getClass())
					+ " entityType=" + Reflections.toString(entityType);
		}
	}
}
