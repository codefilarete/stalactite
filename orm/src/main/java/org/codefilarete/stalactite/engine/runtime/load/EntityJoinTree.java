package org.codefilarete.stalactite.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater.TreeInflationContext;
import org.codefilarete.stalactite.engine.runtime.load.MergeJoinNode.MergeJoinRowConsumer;
import org.codefilarete.stalactite.mapping.AbstractTransformer;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.RowTransformer;
import org.codefilarete.stalactite.query.model.Fromable;
import org.codefilarete.stalactite.query.model.JoinLink;
import org.codefilarete.stalactite.query.model.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.Row;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.VisibleForTesting;
import org.codefilarete.tool.bean.Randomizer;
import org.codefilarete.tool.bean.Randomizer.LinearRandomGenerator;
import org.codefilarete.tool.collection.Collections;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.collection.ReadOnlyList;

/**
 * Tree representing joins of a from clause, nodes are {@link JoinNode}.
 * It maintains an index of its joins based on an unique name for each, so they can be referenced outside {@link EntityJoinTree} without
 * depending on classes of this package (since the reference is a {@link String}).
 *
 * @author Guillaume Mary
 */
public class EntityJoinTree<C, I> {
	
	/**
	 * Key of the very first {@link EntityJoinTree} added to the join structure (the one generated by constructor), see {@link #getRoot()}
	 */
	public static final String ROOT_STRATEGY_NAME = "ROOT";
	
	private final JoinRoot<C, I, ?> root;
	
	/**
	 * A mapping between a name and a join to find them when we want to join one with another new one
	 * 
	 * @see #addRelationJoin(String, EntityInflater, Key, Key, String, JoinType, BeanRelationFixer, Set)
	 * @see #indexKeyGenerator
	 */
	private final Map<String, JoinNode<?>> joinIndex = new HashMap<>();
	
	/**
	 * The objet that will help to give node names / keys into the index (no impact on the generated SQL)
	 * 
	 * @see #joinIndex
	 */
	private final NodeKeyGenerator indexKeyGenerator = new NodeKeyGenerator();
	
	private final Set<Table> tablesToBeExcludedFromDDL = Collections.newIdentitySet();
	
	public EntityJoinTree(EntityInflater<C, I> rootEntityInflater, Fromable table) {
		this.root = new JoinRoot<>(this, rootEntityInflater, table);
		this.joinIndex.put(ROOT_STRATEGY_NAME, root);
	}
	
	public JoinRoot<C, I, ?> getRoot() {
		return root;
	}
	
	/**
	 * Returns mapping between {@link JoinNode} and their internal name.
	 *
	 * @return an unmodifiable version of the internal mapping (because its maintenance responsability falls to current class)
	 */
	Map<String, JoinNode> getJoinIndex() {
		return java.util.Collections.unmodifiableMap(joinIndex);
	}
	
	/**
	 * Adds a join to this select.
	 * Use for one-to-one or one-to-many cases when join is used to describe a related bean.
	 *
	 * @param <U> type of bean mapped by the given strategy
	 * @param <T1> joined left table
	 * @param <T2> joined right table
	 * @param <ID> type of joined values
	 * @param leftStrategyName the name of a (previously) registered join. {@code leftJoinColumn} must be a {@link Column} of its left {@link Table}
	 * @param inflater the strategy of the mapped bean. Used to give {@link Column}s and {@link RowTransformer}
	 * @param leftJoinColumn the {@link Column} (of a previously registered join) to be joined with {@code rightJoinColumn}
	 * @param rightJoinColumn the {@link Column} to be joined with {@code leftJoinColumn}
	 * @param rightTableAlias optional alias for right table, if null table name will be used
	 * @param joinType says whether the join must be open
	 * @param beanRelationFixer a function to fulfill relation between beans
	 * @param additionalSelectableColumns columns to be added to SQL select clause out of ones took from given inflater, necessary for indexed relations
	 * @return the name of the created join, to be used as a key for other joins (through this method {@code leftStrategyName} argument)
	 */
	public <U, T1 extends Table<T1>, T2 extends Table<T2>, ID> String addRelationJoin(String leftStrategyName,
																					  EntityInflater<U, ID> inflater,
																					  Key<T1, ID> leftJoinColumn,
																					  Key<T2, ID> rightJoinColumn,
																					  String rightTableAlias,
																					  JoinType joinType,
																					  BeanRelationFixer<C, U> beanRelationFixer,
																					  Set<? extends Column<T2, ?>> additionalSelectableColumns) {
		return addRelationJoin(leftStrategyName, inflater, leftJoinColumn, rightJoinColumn, rightTableAlias, joinType, beanRelationFixer, additionalSelectableColumns, null);
	}
	
	/**
	 * Adds a join to this select.
	 * Use for one-to-one or one-to-many cases when join is used to describe a related bean.
	 * Difference with {@link #addRelationJoin(String, EntityInflater, Key, Key, String, JoinType, BeanRelationFixer, Set)} is last
	 * parameter : an optional function which computes an identifier of a relation between 2 entities, this is required to prevent from fulfilling
	 * twice the relation when SQL returns several times same identifier (when at least 2 collections are implied). By default this function is
	 * made of parentEntityId + childEntityId and can be overwritten here (in particular when relation is a List, entity index is added to computation).
	 * See {@link RelationJoinNode.RelationJoinRowConsumer#applyRelatedEntity(Object, Row, TreeInflationContext)} for usage.
	 *
	 * @param <U> type of bean mapped by the given strategy
	 * @param <T1> joined left table
	 * @param <T2> joined right table
	 * @param <ID> type of joined values
	 * @param leftStrategyName the name of a (previously) registered join. {@code leftJoinColumn} must be a {@link Column} of its left {@link Table}
	 * @param inflater the strategy of the mapped bean. Used to give {@link Column}s and {@link RowTransformer}
	 * @param leftJoinColumn the {@link Column} (of a previously registered join) to be joined with {@code rightJoinColumn}
	 * @param rightJoinColumn the {@link Column} to be joined with {@code leftJoinColumn}
	 * @param rightTableAlias optional alias for right table, if null table name will be used
	 * @param joinType says whether the join must be open
	 * @param beanRelationFixer a function to fulfill relation between beans
	 * @param additionalSelectableColumns columns to be added to SQL select clause out of ones took from given inflater, necessary for indexed relations
	 * @param relationIdentifierProvider relation identifier provider, not null for List cases : necessary because List may contain duplicate
	 * @return the name of the created join, to be used as a key for other joins (through this method {@code leftStrategyName} argument)
	 * @see RelationJoinNode.RelationJoinRowConsumer#applyRelatedEntity(Object, Row, TreeInflationContext)
	 */
	public <U, T1 extends Table<T1>, T2 extends Table<T2>, ID, JOINTYPE> String addRelationJoin(String leftStrategyName,
																								EntityInflater<U, ID> inflater,
																								Key<T1, JOINTYPE> leftJoinColumn,
																								Key<T2, JOINTYPE> rightJoinColumn,
																								@Nullable String rightTableAlias,
																								JoinType joinType,
																								BeanRelationFixer<C, U> beanRelationFixer,
																								Set<? extends Column<T2, ?>> additionalSelectableColumns,
																								@Nullable BiFunction<Row, ColumnedRow, Object> relationIdentifierProvider) {
		return this.addJoin(leftStrategyName, parent -> new RelationJoinNode<>(
				(JoinNode<T1>) (JoinNode) parent,
				leftJoinColumn,
				rightJoinColumn,
				joinType,
				new KeepOrderSet<>(Collections.cat(inflater.getSelectableColumns(), additionalSelectableColumns)),
				rightTableAlias,
				inflater,
				beanRelationFixer,
				relationIdentifierProvider));
	}
	
	
	
	public static class PolymorphicEntityInflater<E, D extends E, I, T extends Table<T>> implements EntityInflater<D, I> {
		
		private final ConfiguredRelationalPersister<E, I> mainPersister;
		
		private final ConfiguredRelationalPersister<D, I> subPersister;
		
		public PolymorphicEntityInflater(ConfiguredRelationalPersister<E, I> mainPersister,
										 ConfiguredRelationalPersister<D, I> subPersister) {
			this.mainPersister = mainPersister;
			this.subPersister = subPersister;
		}
		
		@Override
		public Class<D> getEntityType() {
			return subPersister.getClassToPersist();
		}
		
		@Override
		public I giveIdentifier(Row row, ColumnedRow columnedRow) {
			return mainPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(row, columnedRow);
		}
		
		@Override
		public RowTransformer<D> copyTransformerWithAliases(ColumnedRow columnedRow) {
			return subPersister.getMapping().copyTransformerWithAliases(columnedRow);
		}
		
		@Override
		public Set<Selectable<?>> getSelectableColumns() {
			return (Set) subPersister.getMapping().getSelectableColumns();
		}
	}
	
	public static class PolymorphicMergeJoinRowConsumer<C, D extends C, I> extends MergeJoinRowConsumer<D> {
		
		private final BiFunction<Row, ColumnedRow, I> identifierProvider;
		
		private final ColumnedRow columnedRow;
		
		public PolymorphicMergeJoinRowConsumer(PolymorphicEntityInflater<C, D, I, ?> entityInflater,
											   ColumnedRow columnedRow) {
			super(entityInflater.copyTransformerWithAliases(columnedRow));
			this.identifierProvider = entityInflater::giveIdentifier;
			this.columnedRow = columnedRow;
		}
		
		@Nullable
		public I giveIdentifier(Row row) {
			return identifierProvider.apply(row, columnedRow);
		}
		
		public D transform(Row row) {
			return super.merger.transform(row);
		}
	}
	
	/**
	 * Adds a join to this select.
	 * Use for inheritance cases when joined data are used to complete an existing bean.
	 *
	 * @param <U> type of bean mapped by the given strategy
	 * @param <T1> left table type
	 * @param <T2> right table type
	 * @param <ID> type of joined values
	 * @param leftStrategyName the name of a (previously) registered join. {@code leftJoinColumn} must be a {@link Column} of its left {@link Table}.
	 * 						Right table data will be merged with this "root".
	 * @param inflater the strategy of the mapped bean. Used to give {@link Column}s and {@link RowTransformer}
	 * @param leftJoinColumn the {@link Column} (of previous strategy left table) to be joined with {@code rightJoinColumn}
	 * @param rightJoinColumn the {@link Column} (of the strategy table) to be joined with {@code leftJoinColumn}
	 * @return the name of the created join, to be used as a key for other joins (through this method {@code leftStrategyName} argument)
	 */
	public <U, T1 extends Fromable, T2 extends Fromable, ID> String addMergeJoin(String leftStrategyName,
																				 EntityMerger<U> inflater,
																				 Key<T1, ID> leftJoinColumn,
																				 Key<T2, ID> rightJoinColumn) {
		return this.addJoin(leftStrategyName, parent -> new MergeJoinNode<>((JoinNode<T1>) parent,
				leftJoinColumn, rightJoinColumn, JoinType.INNER,
				null, inflater));
	}
	
	/**
	 * Adds a merge join to this select : no bean will be created by given {@link EntityInflater}, only its
	 * {@link AbstractTransformer#applyRowToBean(Row, Object)} will be used during bean graph loading process.
	 *
	 * @param <T1> left table type
	 * @param <T2> right table type
	 * @param <ID> type of joined values
	 * @param leftStrategyName join name on which join must be created
	 * @param entityMerger strategy to be used to load bean
	 * @param leftJoinColumn left join column, expected to be one of left strategy table
	 * @param rightJoinColumn right join column
	 * @param joinType type of join to create
	 * @return the name of the created join, to be used as a key for other joins (through this method {@code leftStrategyName} argument)
	 */
	public <U, T1 extends Fromable, T2 extends Fromable, ID> String addMergeJoin(String leftStrategyName,
																				 EntityMerger<U> entityMerger,
																				 Key<T1, ID> leftJoinColumn,
																				 Key<T2, ID> rightJoinColumn,
																				 JoinType joinType) {
		return this.addJoin(leftStrategyName, parent -> new MergeJoinNode<>((JoinNode<T1>) parent,
				leftJoinColumn, rightJoinColumn, joinType,
				null, entityMerger));
	}
	
	public <U, T1 extends Fromable, T2 extends Fromable, ID> String addMergeJoin(String leftStrategyName,
																				 EntityMerger<U> entityMerger,
																				 Key<T1, ID> leftJoinColumn,
																				 Key<T2, ID> rightJoinColumn,
																				 JoinType joinType,
																				 Function<ColumnedRow, MergeJoinRowConsumer<U>> columnedRowConsumer) {
		return this.addJoin(leftStrategyName, parent -> new MergeJoinNode<U, T1, T2, ID>((JoinNode<T1>) parent,
				leftJoinColumn, rightJoinColumn, joinType,
				null, entityMerger) {
			@Override
			public MergeJoinRowConsumer<U> toConsumer(ColumnedRow columnedRow) {
				return columnedRowConsumer.apply(columnedRow);
			}
		});
	}
	
	/**
	 * Adds a passive join to this select : this kind of join doesn't take part to bean construction, it aims only at adding an SQL join to
	 * bean graph loading.
	 *
	 * @param leftStrategyName join name on which join must be created
	 * @param leftJoinColumn left join column, expected to be one of left strategy table
	 * @param rightJoinColumn right join column
	 * @param joinType type of join to create
	 * @param columnsToSelect columns that must be added to final select
	 * @param <T1> left table type
	 * @param <T2> right table type
	 * @param <JOINTYPE> type of joined values
	 * @return the name of the created join, to be used as a key for other joins (through this method {@code leftStrategyName} argument)
	 */
	public <T1 extends Table<T1>, T2 extends Table<T2>, JOINTYPE> String addPassiveJoin(String leftStrategyName,
																						Key<T1, JOINTYPE> leftJoinColumn,
																						Key<T2, JOINTYPE> rightJoinColumn,
																						JoinType joinType,
																						Set<? extends JoinLink<T2, Object>> columnsToSelect) {
		return this.addJoin(leftStrategyName, parent -> new PassiveJoinNode<C, T1, T2, JOINTYPE>((JoinNode<T1>) (JoinNode) parent,
				leftJoinColumn, rightJoinColumn, joinType,
				columnsToSelect, null));
	}
	
	public <T1 extends Table<T1>, T2 extends Table<T2>, JOINTYPE> String addPassiveJoin(String leftStrategyName,
																						Key<T1, JOINTYPE> leftJoinColumn,
																						Key<T2, JOINTYPE> rightJoinColumn,
																						JoinType joinType,
																						Set<Column<T2, ?>> columnsToSelect,
																						EntityTreeJoinNodeConsumptionListener<C> consumptionListener) {
		return this.addJoin(leftStrategyName, parent -> new PassiveJoinNode<C, T1, T2, JOINTYPE>((JoinNode<T1>) (JoinNode) parent,
				leftJoinColumn, rightJoinColumn, joinType,
				columnsToSelect, null).setConsumptionListener(consumptionListener));
	}
	
	public <T1 extends Table<T1>, T2 extends Table<T2>, JOINTYPE> String addPassiveJoin(String leftStrategyName,
																						Key<T1, JOINTYPE> leftJoinColumn,
																						Key<T2, JOINTYPE> rightJoinColumn,
																						String tableAlias,
																						JoinType joinType,
																						Set<Column<T2, ?>> columnsToSelect,
																						EntityTreeJoinNodeConsumptionListener<C> consumptionListener,
																						boolean rightTableParticipatesToDDL) {
		if (rightTableParticipatesToDDL) {
			tablesToBeExcludedFromDDL.add(rightJoinColumn.getTable());
		}
		return this.addJoin(leftStrategyName, parent -> new PassiveJoinNode<C, T1, T2, JOINTYPE>((JoinNode<T1>) (JoinNode) parent,
				leftJoinColumn, rightJoinColumn, joinType,
				columnsToSelect, tableAlias).setConsumptionListener(consumptionListener));
	}
	
	public String addJoin(String leftStrategyName, Function<? super JoinNode<Fromable> /* parent node */, ? extends AbstractJoinNode<?, ?, ?, ?>> joinNodeSupplier) {
		JoinNode<Fromable> owningJoin = getJoin(leftStrategyName);
		if (owningJoin == null) {
			throw new IllegalArgumentException("No join named " + leftStrategyName + " exists to add a new join on");
		}
		AbstractJoinNode joinNode = joinNodeSupplier.apply(owningJoin);
		String joinName = this.indexKeyGenerator.generateKey(joinNode);
		this.joinIndex.put(joinName, joinNode);
		return joinName;
	}
	
	/**
	 * Gives a particular node of the joins graph by its name. Joins graph name are given in return of
	 * {@link #addRelationJoin(String, EntityInflater, Key, Key, String, JoinType, BeanRelationFixer, Set)}.
	 * When {@link #ROOT_STRATEGY_NAME} is given, {@link #getRoot()} will be used, meanwhile, be aware that using this method to retreive root node
	 * is not the recommended way : prefer usage of {@link #getRoot()} to prevent exposure of {@link #ROOT_STRATEGY_NAME}
	 *
	 * @param leftStrategyName join node name to be given
	 * @return null if the node doesn't exist
	 * @see #getRoot()
	 */
	@Nullable
	public JoinNode<Fromable> getJoin(String leftStrategyName) {
		return (JoinNode<Fromable>) this.joinIndex.get(leftStrategyName);
	}
	
	/**
	 * Returns node that joins given columns. Comparison between columns and node ones is made using reference checking, not equality, because
	 * there's no limitation on multiple presence of same join in the tree, overall with identical column name, so to enforce finding the matching
	 * join we use reference check.
	 * Mainly done for testing purpose.
	 * 
	 * @param leftKey columns to be compared to left columns of this tree
	 * @param rightKey columns to be compared to right columns of this tree
	 * @return the join that as same left and right column as given ones, null if none exists
	 */
	@VisibleForTesting
	<T1 extends Table<T1>, T2 extends Table<T2>, E, ID, O> AbstractJoinNode<E, T1, T2, ID> giveJoin(Key<T1, O> leftKey, Key<T2, O> rightKey) {
		return (AbstractJoinNode<E, T1, T2, ID>) Iterables.find(joinIterator(), node ->
				node.getLeftJoinLink().getColumns().equals(leftKey.getColumns())
				&& node.getRightJoinLink().getColumns().equals(rightKey.getColumns()));
	}
	
	/**
	 * Gives all tables used by this tree
	 *
	 * @return all joins tables of this tree
	 */
	public Set<Table> giveTables() {
		// because Table implements an hashCode on their name, we can use an HashSet to avoid duplicates
		Set<Table> result = new HashSet<>();
		result.add((Table) root.getTable());
		foreachJoin(node -> {
			if (node.getTable() instanceof Table && !tablesToBeExcludedFromDDL.contains(node.getTable())) {
				result.add((Table) node.getTable());
			}
		});
		return result;
	}
	
	/**
	 * Shortcut for {@code joinIterator().forEachRemaining()}.
	 * Goes down this tree by breadth first. Made to avoid everyone implements node iteration.
	 * Consumer is invoked foreach node <strong>except root</strong> because it usually has a special treatment.
	 * Traversal is made in pre-order : node is consumed first then its children.
	 * 
	 * @param consumer a {@link AbstractJoinNode} consumer
	 */
	public void foreachJoin(Consumer<AbstractJoinNode<?, ?, ?, ?>> consumer) {
		joinIterator().forEachRemaining(consumer);
	}
	
	/**
	 * Copies this tree onto given one under given join node name
	 * 
	 * @param target tree receiving copies of this tree nodes
	 * @param joinNodeName node name under which this tree must be copied, 
	 * @param <E> main entity type of target tree
	 * @param <ID> main entity identifier type of target tree
	 */
	public <E, ID> void projectTo(EntityJoinTree<E, ID> target, String joinNodeName) {
		projectTo(target.getJoin(joinNodeName));
	}
	
	public void projectTo(JoinNode<Fromable> joinNode) {
		EntityJoinTree<?, ?> tree = joinNode.getTree();
		foreachJoinWithDepth(joinNode, (targetOwner, currentNode) -> {
			// cloning each node, the only difference lays on left column : target gets its matching column
			currentNode.getLeftJoinLink().getColumns().forEach(leftColumn -> {
				Selectable<?> projectedLeftColumn = targetOwner.getTable().findColumn(leftColumn.getExpression());
				if (projectedLeftColumn == null) {
					throw new IllegalArgumentException("Expected column "
							+ leftColumn.getExpression() + " to exist in target table " + targetOwner.getTable().getName()
							+ " but couldn't be found among " + Iterables.collect(targetOwner.getTable().getColumns(), Selectable::getExpression, ArrayList::new));
				}
			});
			AbstractJoinNode nodeClone = copyNodeToParent(currentNode, targetOwner, currentNode.getLeftJoinLink());
			// maintaining join names through trees : we add current node name to target one. Then nodes can be found across trees
			Set<Entry<String, JoinNode<?>>> set = this.joinIndex.entrySet();
			Entry<String, JoinNode<?>> nodeName = Iterables.find(set, entry -> entry.getValue() == currentNode);
			tree.joinIndex.put(nodeName.getKey(), nodeClone);
			
			return nodeClone;
		});
	}
	
	/**
	 * Creates an {@link Iterator} that goes down this tree by breadth first. Made to avoid everyone implements node iteration.
	 * Consumer is invoked foreach node <strong>except root</strong> because it usually has a special treatment.
	 * Traversal is made in pre-order : node is consumed first then its children.
	 */
	public Iterator<AbstractJoinNode<?, ?, ?, ?>> joinIterator() {
		return new NodeIterator();
	}
	
	/**
	 * Goes down this tree by breadth first.
	 * Consumer is invoked foreach node <strong>except root</strong> because it usually has a special treatment.
	 * Used to create an equivalent tree of this instance with another type of node. This generally requires knowing current parent to allow child
	 * addition : consumer gets current parent as a first argument
	 * 
	 * @param initialNode very first parent given as first argument to consumer
	 * @param consumer producer of target tree node, gets previous created node (to add created node to it) and node of this tree 
	 * @param <S> type of node of the equivalent tree
	 */
	<S> void foreachJoinWithDepth(S initialNode, BiFunction<S, AbstractJoinNode<?, ?, ?, ?>, S> consumer) {
		Queue<S> targetPath = new ArrayDeque<>();
		targetPath.add(initialNode);
		NodeIteratorWithDepth<S> nodeIterator = new NodeIteratorWithDepth<>(targetPath, consumer);
		// We simply iterate all over the iterator to consume all elements
		// Please note that forEachRemaining can't be used because it is unsupported by NodeIteratorWithDepth 
		while (nodeIterator.hasNext()) {
			nodeIterator.next();
		}
		
	}
	
	/**
	 * Internal class that focuses on nodes. Iteration node is made breadth-first.
	 */
	private class NodeIterator implements Iterator<AbstractJoinNode<?, ?, ?, ?>> {
		
		protected final Queue<AbstractJoinNode> joinStack;
		protected AbstractJoinNode currentNode;
		protected boolean nextDepth = false;
		
		public NodeIterator() {
			joinStack = new ArrayDeque<>(root.getJoins());
		}
		
		@Override
		public boolean hasNext() {
			return !joinStack.isEmpty();
		}
		
		@Override
		@SuppressWarnings("java:S2272")    // NoSuchElementException is manged by Queue#remove()
		public AbstractJoinNode next() {
			// we prefer remove() to poll() because it manages NoSuchElementException which is also in next() contract
			currentNode = joinStack.remove();
			ReadOnlyList<AbstractJoinNode> nextJoins = currentNode.getJoins();
			joinStack.addAll(nextJoins);
			nextDepth = !nextJoins.isEmpty();
			return currentNode;
		}
	}
	
	private class NodeIteratorWithDepth<S> extends NodeIterator {
		
		private final Queue<S> targetPath;
		private final BiFunction<S, AbstractJoinNode<Object, Fromable, Fromable, Object>, S> consumer;
		
		public NodeIteratorWithDepth(Queue<S> targetPath, BiFunction<S, AbstractJoinNode<?, ?, ?, ?>, S> consumer) {
			this.targetPath = targetPath;
			this.consumer = (BiFunction<S, AbstractJoinNode<Object, Fromable, Fromable, Object>, S>) (BiFunction) consumer;
		}
		
		@Override
		public AbstractJoinNode next() {
			super.next();
			S targetOwner = targetPath.peek();
			S nodeClone = (S) consumer.apply(targetOwner, currentNode);
			if (nextDepth) {
				targetPath.add(nodeClone);
			}
			
			// if depth changes, we must remove target depth
			AbstractJoinNode nextIterationNode = joinStack.peek();
			if (nextIterationNode != null && nextIterationNode.getParent() != currentNode.getParent()) {
				targetPath.remove();
			}
			
			return currentNode;
		}
		
		@Override
		public void forEachRemaining(Consumer<? super AbstractJoinNode<?, ?, ?, ?>> action) {
			// this is not supported since a consumer is already given to constructor
			throw new UnsupportedOperationException();
		}
	}
	
	/**
	 * Copies given node and set it as a child of given parent.
	 * Could have been implemented by each node class itself but since this behavior is required only by the tree
	 * and a particular algorithm, decision was made to do it outside of them.
	 * 
	 * @param node node to be cloned
	 * @param parent parent node target of the clone
	 * @param leftColumn columns to be used as the left key of the new node
	 * @return a copy of given node, put as child of parent, using leftColumn
	 */
	public static AbstractJoinNode copyNodeToParent(AbstractJoinNode node, JoinNode parent, Key leftColumn) {
		AbstractJoinNode nodeCopy;
		if (node instanceof RelationJoinNode) {
			nodeCopy = new RelationJoinNode(
					parent,
					leftColumn,
					node.getRightJoinLink(),
					node.getJoinType(),
					node.getColumnsToSelect(),
					node.getTableAlias(),
					((RelationJoinNode) node).getEntityInflater(),
					((RelationJoinNode) node).getBeanRelationFixer(),
					((RelationJoinNode) node).getRelationIdentifierProvider());
		} else if (node instanceof MergeJoinNode) {
			nodeCopy = new MergeJoinNode(
					parent,
					leftColumn,
					node.getRightJoinLink(),
					node.getJoinType(),
					node.getTableAlias(),
					((MergeJoinNode) node).getMerger());
		} else if (node instanceof PassiveJoinNode) {
			nodeCopy = new PassiveJoinNode(
					parent,
					leftColumn,
					node.getRightJoinLink(),
					node.getJoinType(),
					node.getColumnsToSelect(),
					node.getTableAlias());
		} else {
			throw new UnsupportedOperationException("Unexpected type of join : some algorithm has changed, please implement it here or fix it : "
					+ Reflections.toString(node.getClass()));
		}
		nodeCopy.setConsumptionListener(node.getConsumptionListener());
		
		return nodeCopy;
	}
	
	private class NodeKeyGenerator {
		
		/**
		 * We don't use {@link java.security.SecureRandom} because it consumes too much time while computing random values (more than 500ms
		 * for a 6-digit identifier ! ... for each Node !!), and there's no need for security here.
		 */
		@SuppressWarnings("java:S2245" /* no security issue about Random class : it's only used as identifier generator, using SecureRandom is less performant */)
		private final Randomizer keyGenerator = new Randomizer(new LinearRandomGenerator(new Random()));
		
		private String generateKey(JoinNode node) {
			// We generate a name which is unique across trees so node clones can be found outside this class by their name on different trees.
			// This is necessary for particular case of reading indexing column of indexed collection with an association table : the node that needs
			// to use indexing column is not the owner of the association table clone hence it can't use it (see table clone mechanism at EntityTreeQueryBuilder).
			// Said differently the "needer" is the official owner whereas the indexing column is on another node dedicated to the relation table maintenance.
			// The found way for the official node to access data through the indexing column is to use the identifier of the relation table node,
			// because it has it when it is created (see OneToManyWithIndexedAssociationTableEngine), and because the node is cloned through tree
			// the identifier should be "universal".
			// Note that this naming strategy could be more chaotic (totally random) since names are only here to give a unique identifier to joins
			// but the hereafter algorithm can help for debug
			return node.getTable().getAbsoluteName()
					+ "-" + Integer.toHexString(System.identityHashCode(EntityJoinTree.this)) + "-" + keyGenerator.randomHexString(6);
		}
	}
	
	public enum JoinType {
		INNER,
		OUTER
	}
	
}
