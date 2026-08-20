package org.codefilarete.stalactite.engine.configurer.resolver.onetoone;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.EntityPolymorphism;
import org.codefilarete.stalactite.engine.configurer.model.JoinTablePolymorphism;
import org.codefilarete.stalactite.engine.configurer.model.PolymorphicEntity;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.model.SingleTablePolymorphism;
import org.codefilarete.stalactite.engine.configurer.model.TablePerClassPolymorphism;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.RelationStorage;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.engine.runtime.jointable.JoinTablePolymorphismReader;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger.EntityMergerAdapter;
import org.codefilarete.stalactite.engine.runtime.load.JoinNode;
import org.codefilarete.stalactite.engine.runtime.load.JoinTablePolymorphicRelationJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.MergeJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.PolymorphicMergeJoinRowConsumer;
import org.codefilarete.stalactite.engine.runtime.load.SingleTablePolymorphicRelationJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.TablePerClassPolymorphicRelationJoinNode;
import org.codefilarete.stalactite.engine.runtime.singletable.SingleTablePolymorphismReader;
import org.codefilarete.stalactite.engine.runtime.tableperclass.TablePerClassPolymorphismReader;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.query.api.JoinLink;
import org.codefilarete.stalactite.query.api.QueryStatement;
import org.codefilarete.stalactite.query.api.QueryStatement.PseudoTable;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Union;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Key.KeyBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderMap;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.trace.MutableInt;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;
import static org.codefilarete.stalactite.query.Operators.cast;

public class AggregateOneToOneAppender {
	
	/**
	 *
	 * @param relation
	 * @param targetPersister
	 * @param mountPoint
	 * @param aggregateTree
	 * @param <SRC>
	 * @param <SRCID>
	 * @param <TRGT>
	 * @param <TRGTID>
	 * @param <LEFTTABLE>
	 * @param <RIGHTTABLE>
	 * @param <JOINID> either SRCID or TRGTID, depending on the relation owner
	 * @return
	 */
	public <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID, GRAFTTABLE extends Table<GRAFTTABLE>>
	GraftPoint<TRGT, TRGTID, RIGHTTABLE, SRC, SRCID> append(ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> relation,
	                                                        ConfiguredEntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                                                        ConfiguredEntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                                                        String mountPoint,
	                                                        EntityJoinTree<SRC, SRCID> aggregateTree) {
		
		// TODO: do the same for Many-to-one
		GraftPoint<TRGT, TRGTID, RIGHTTABLE, SRC, SRCID> result;
		
		DirectRelationJoin<LEFTTABLE, RIGHTTABLE, JOINID> join = relation.getJoin();
		if (relation.isFetchSeparately()) {
			ThreadLocal<RelationStorage<SRC, TRGTID>> current2PhasesLoadContext = new ThreadLocal<>();
			Function<ColumnedRow, TRGTID> idMapping;
			if (relation.isOwnedByTarget()) {
				// We build a function capable of building the identifier from the association table columns, because,
				// if we give the targetPersister identifier assembler then the runtime fails : identifier takes its values
				// from the target table columns which are missins in the join : the join only contains the right
				// table columns and the association ones (that's separate-load principle)
				KeyMapping<RIGHTTABLE, LEFTTABLE, TRGTID> targetPkToRightKey = new KeyMapping<>(targetPersister.getMapping().getTargetTable().getPrimaryKey(), (Key<LEFTTABLE, TRGTID>) join.getLeftKey());
				KeepOrderMap<JoinLink<RIGHTTABLE, ?>, JoinLink<LEFTTABLE, ?>> targetPkToAssociationTableKey = targetPkToRightKey.getMapping();
				idMapping = columnedRow -> targetPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(new ColumnedRow() {
					@Override
					public <E> E get(Selectable<E> pkColumn) {
						return (E) columnedRow.get(targetPkToAssociationTableKey.get(pkColumn));
					}
				});
			} else {
				idMapping = targetPersister.getMapping().getIdMapping().getIdentifierAssembler()::assemble;
			}
			
			// here is the logic below :
			// - we collect the SRC-Index-TRGTID on the association join: see FirstPhaseIndexedRelationLoader usage hereafter
			// - then we trigger the target entities collect on the afterSelect of the source
			// - just after, we can apply the relation
			aggregateTree.addMergeJoin(mountPoint,
					new org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.FirstPhaseRelationLoader<>(idMapping, (Set) targetPersister.getMapping().getTargetTable().getPrimaryKey().getColumns(), current2PhasesLoadContext),
					join.getLeftKey(),
					join.getRightKey(),
					OUTER);
			
			// adding second phase loader
			sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
				@Override
				public void beforeSelect(Iterable<SRCID> ids) {
					current2PhasesLoadContext.set(new RelationStorage<>());
				}
				
				@Override
				public void afterSelect(Set<? extends SRC> result) {
					// we load all the target entities (of all sources, for efficiency)
					Map<SRC, Set<TRGTID>> targetIdPerSource = current2PhasesLoadContext.get().getTargetIdPerSource();
					Set<TRGTID> trgtids = targetIdPerSource.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
					Set<TRGT> targets = targetPersister.select(trgtids);
					Map<TRGTID, TRGT> targetPerId = new HashMap<>(Iterables.map(targets, targetPersister.getMapping()::getId));
					
					// we sow the relations
					result.forEach(src -> {
						// filling final collection with a sorted collection
						Set<TRGTID> targetIdPerIndex = targetIdPerSource.get(src);
						if (targetIdPerIndex != null) {  // targetIdPerIndex can be null if there's no associated entity in the database
							TRGTID trgtId = Iterables.first(targetIdPerIndex);
							TRGT trgt = targetPerId.get(trgtId);
							relation.getAccessor().set(src, trgt);
						}
					});
					
					clearContext();
				}
				
				@Override
				public void onSelectError(Iterable<SRCID> ids, RuntimeException exception) {
					clearContext();
				}
				
				private void clearContext() {
					current2PhasesLoadContext.remove();
				}
			});
			
			// Note that because the relation is loaded separately, next joins should be appended to the target entity join tree,
			// not the given as argument one, so we return a GraftPoint with the target persister and its join tree. And it should be grafted on ROOT_JOIN_NAME
			result = new GraftPoint(relation.getTargetEntity(), targetPersister, ROOT_JOIN_NAME, targetPersister.getEntityJoinTree());
		} else {
			// we join the relation onto the aggregate root to build the whole select tree
			
			String joinName = null;
			if (relation.getTargetEntity() instanceof PolymorphicEntity) {
				EntityPolymorphism<TRGT, Object> polymorphism = ((PolymorphicEntity<TRGT, Object, RIGHTTABLE>) relation.getTargetEntity()).getPolymorphism();
				Set<Column<RIGHTTABLE, ?>> columns = targetPersister.<RIGHTTABLE>getMainTable().getColumns();
				
				if (polymorphism instanceof SingleTablePolymorphism) {
					SingleTablePolymorphism<TRGT, TRGTID, Object, RIGHTTABLE> polymorphismPolicy = (SingleTablePolymorphism<TRGT, TRGTID, Object, RIGHTTABLE>) polymorphism;
					Column<RIGHTTABLE, Object> discriminatorColumn = polymorphismPolicy.getDiscriminatorColumn();
					SingleTablePolymorphismReader<TRGT, TRGTID, RIGHTTABLE, Object> targetPersister1 = (SingleTablePolymorphismReader<TRGT, TRGTID, RIGHTTABLE, Object>) targetPersister;
				
					joinName = aggregateTree.addJoin(mountPoint, parent -> new SingleTablePolymorphicRelationJoinNode<>(
							(JoinNode<SRC, LEFTTABLE>) (JoinNode) parent,
							relation.getAccessor(),
							join.getLeftKey(),
							join.getRightKey(),
							OUTER,
							columns,
							null,
							new EntityMappingAdapter<>(targetPersister.getMapping()),
							(BeanRelationFixer<Object, TRGT>) relation.getRelationFixer(),
							discriminatorColumn,
							new HashSet<>(targetPersister1.getSubEntitiesPersisters().values()),
							polymorphismPolicy));
				}
				if (polymorphism instanceof TablePerClassPolymorphism) {
					TablePerClassPolymorphismReader<TRGT, TRGTID, RIGHTTABLE> targetPersister1 = (TablePerClassPolymorphismReader<TRGT, TRGTID, RIGHTTABLE>) targetPersister;
					
					// we build a union of all sub queries that will be joined in the main query
					// To build the union we need the columns that are common to all persisters
					Set<JoinLink<?, ?>> commonColumns = new KeepOrderSet<>();
					commonColumns.addAll(targetPersister1.getMapping().getSelectableColumns());
					
					Set<String> commonColumnsNames = commonColumns.stream().map(JoinLink::getExpression).collect(Collectors.toSet());
					
					Set<ConfiguredEntityReader<? extends TRGT, TRGTID, ?>> subPersisters = new HashSet<>(targetPersister1.getSubEntitiesPersisters().values());
					
					KeepOrderSet<Column<?, ?>> nonCommonColumns = new KeepOrderSet<>();
					subPersisters.forEach(subPersister -> {
						nonCommonColumns.addAll(subPersister.getMainTable().getColumns());
					});
					nonCommonColumns.removeIf(c -> commonColumnsNames.contains(c.getName()));
					
					Union subPersistersUnion = new Union();
					String entityTypeDiscriminatorName = "clazz_";
					QueryStatement.PseudoColumn<Integer> discriminatorPseudoColumn = subPersistersUnion.registerColumn(entityTypeDiscriminatorName, Integer.class);
					MutableInt discriminatorComputer = new MutableInt();
					
					subPersisters.forEach(subPersister -> {
						Query subEntityQuery = new Query(subPersister.getMapping().getTargetTable());
						subEntityQuery.select(String.valueOf(discriminatorComputer.increment()), Integer.class, entityTypeDiscriminatorName);
						subPersistersUnion.unionAll(subEntityQuery);
						
						commonColumns.forEach(column -> {
							subEntityQuery.select(column.getExpression(), column.getJavaType());
							subPersistersUnion.registerColumn(column.getExpression(), column.getJavaType());
						});
						
						nonCommonColumns.forEach(column -> {
							Selectable<?> expression;
							if (subPersister.getMapping().getSelectableColumns().contains(column)) {
								expression = new Selectable.SimpleSelectable<>(column.getName(), column.getJavaType());
							} else {
								expression = cast((String) null, column.getJavaType());
							}
							// we put an alias else cast(..) as no name which makes it doesn't match official-column name, and then
							// may cause an error since SQL in kind of invalid 
							subEntityQuery.select(expression, column.getName());
							subPersistersUnion.registerColumn(column.getName(), column.getJavaType());
						});
					});
					
					Holder<TablePerClassPolymorphicRelationJoinNode<TRGT, LEFTTABLE, JOINID, TRGTID>> createdJoinHolder = new Holder<>();
					joinName = aggregateTree.addJoin(mountPoint, parent -> {
						TablePerClassPolymorphicRelationJoinNode<TRGT, LEFTTABLE, JOINID, TRGTID> relationJoinNode = new TablePerClassPolymorphicRelationJoinNode<>(
								(JoinNode<SRC, LEFTTABLE>) (JoinNode) parent,
								subPersistersUnion,
								relation.getAccessor(),
								join.getLeftKey(),
								join.getRightKey(),
								OUTER,
								subPersistersUnion.getColumns(),
								// we set a table alias else it is null which is transformed as "null" into the SQL which works
								// but is not very clean and may cause problems if we have several table-per-class (all would
								// be named "null") in the aggregate
								relation.getTargetEntity().getEntityType().getSimpleName(),
								new EntityMappingAdapter<>(targetPersister.getMapping()),
								(BeanRelationFixer<Object, TRGT>) relation.getRelationFixer(),
								discriminatorPseudoColumn);
						
						createdJoinHolder.set(relationJoinNode);
						return relationJoinNode;
					});
					
					addTablePerClassPolymorphicSubPersistersJoins(aggregateTree, relation.getTargetEntity().getTable(), joinName, createdJoinHolder.get(), subPersisters);
				}
				if (polymorphism instanceof JoinTablePolymorphism) {
					JoinTablePolymorphism<TRGT, TRGTID, RIGHTTABLE> polymorphismPolicy = (JoinTablePolymorphism<TRGT, TRGTID, RIGHTTABLE>) polymorphism;
					JoinTablePolymorphismReader<TRGT, TRGTID, RIGHTTABLE> targetPersister1 = (JoinTablePolymorphismReader<TRGT, TRGTID, RIGHTTABLE>) targetPersister;
					
					Holder<JoinTablePolymorphicRelationJoinNode<TRGT, LEFTTABLE, RIGHTTABLE, JOINID, TRGTID>> createdJoinHolder = new Holder<>();
					joinName = aggregateTree.addJoin(mountPoint, parent -> {
						JoinTablePolymorphicRelationJoinNode<TRGT, LEFTTABLE, RIGHTTABLE, JOINID, TRGTID> polymorphicRelationJoinNode = new JoinTablePolymorphicRelationJoinNode<>(
								(JoinNode<SRC, LEFTTABLE>) (JoinNode) parent,
								relation.getAccessor(),
								join.getLeftKey(),
								join.getRightKey(),
								OUTER,
								targetPersister.<LEFTTABLE>getMainTable().getColumns(),
								null,
								new EntityMappingAdapter<>(targetPersister.getMapping()),
								(BeanRelationFixer<Object, TRGT>) relation.getRelationFixer(),
								null);
						createdJoinHolder.set(polymorphicRelationJoinNode);
						return polymorphicRelationJoinNode;
					});
					
					addJoinTablePolymorphicSubPersistersJoins(aggregateTree, joinName, targetPersister.getMainTable(), createdJoinHolder.get(), new HashSet<>(targetPersister1.getSubEntitiesPersisters().values()));
				}
			} else {
				joinName = aggregateTree.addRelationJoin(
						mountPoint,
						new EntityMappingAdapter<>(targetPersister.getMapping()),
						relation.getAccessor(),
						join.getLeftKey(),
						join.getRightKey(),
						null,
						OUTER,
						relation.getRelationFixer(),
						Collections.emptySet());
			}
			result = new GraftPoint<>(relation.getTargetEntity(), targetPersister, joinName, aggregateTree);
		}
		return result;
	}
	
	private <SRC, SRCID, C, I, V extends C, T extends Table<T>, SUBTABLE extends Table<SUBTABLE>> void addTablePerClassPolymorphicSubPersistersJoins(
			EntityJoinTree<SRC, SRCID> entityJoinTree,
			Table<?> templateTable,
			String mainPolymorphicJoinNodeName,
			TablePerClassPolymorphicRelationJoinNode<C, T, ?, I> mainPersisterJoin,
			Set<ConfiguredEntityReader<? extends C, I, ?>> subPersisters) {
		
		// The join is made on the Union as left table, and the column we must get is mainPersister's primaryKey (which table is not in the tree since
		// we are the table-per-class case), so we have to create an equivalent of the primary key, based on the columns of the union
		KeyBuilder<PseudoTable, I> leftKey = Key.from(mainPersisterJoin.getRightTable());
		templateTable.getPrimaryKey().getColumns().forEach(pkCol -> {
			JoinLink<PseudoTable, ?> selectable = (JoinLink<PseudoTable, ?>) Iterables.find(mainPersisterJoin.getColumnsToSelect(), selectableColumn -> selectableColumn.getExpression().equals(pkCol.getName()));
			leftKey.addColumn(selectable);
		});
		MutableInt discriminatorComputer = new MutableInt();
		subPersisters.forEach(subPersister -> {
			ConfiguredEntityReader<V, I, ?> localSubPersister = (ConfiguredEntityReader<V, I, ?>) subPersister;
			entityJoinTree.addMergeJoin(mainPolymorphicJoinNodeName,
					new EntityMergerAdapter<>(localSubPersister.<T>getMapping()),
					leftKey.build(),
					localSubPersister.getMainTable().getPrimaryKey(),
					OUTER,
					joinNode -> {
						PolymorphicMergeJoinRowConsumer<V, I> joinRowConsumer = new PolymorphicMergeJoinRowConsumer<>(
								(MergeJoinNode) joinNode,
								localSubPersister.getMapping());
						mainPersisterJoin.addSubPersisterJoin(joinRowConsumer, discriminatorComputer.increment());
						return joinRowConsumer;
					});
		});
	}
	
	private <SRC, SRCID, C, I, T extends Table<T>, D extends C, SUBTABLE extends Table<SUBTABLE>> void addJoinTablePolymorphicSubPersistersJoins(
			EntityJoinTree<SRC, SRCID> entityJoinTree,
			String mainPolymorphicJoinNodeName,
			T templateTable,
			JoinTablePolymorphicRelationJoinNode<C, T, SUBTABLE, ?, I> mainPersisterJoin,
			Set<ConfiguredEntityReader<? extends C, I, ?>> subPersisters) {
		
		subPersisters.forEach(subPersister -> {
			ConfiguredEntityReader<D, I, SUBTABLE> localSubPersister = (ConfiguredEntityReader<D, I, SUBTABLE>) subPersister;
			entityJoinTree.addJoin(mainPolymorphicJoinNodeName, parent -> new MergeJoinNode<D, T, SUBTABLE, I>(
					(JoinNode<SRC, T>) (JoinNode) parent,
					templateTable.getPrimaryKey(),
					subPersister.<SUBTABLE>getMainTable().getPrimaryKey(),
					OUTER,
					null,
					new EntityMergerAdapter<>((EntityMapping<D, I, ?>) localSubPersister.getMapping())) {
				@Override
				public MergeJoinRowConsumer<D> toConsumer(JoinNode<D, SUBTABLE> joinNode) {
					PolymorphicMergeJoinRowConsumer<D, I> joinRowConsumer = new PolymorphicMergeJoinRowConsumer<>(
							(MergeJoinNode<D, ?, ?, I>) joinNode,
							localSubPersister.getMapping());
					mainPersisterJoin.addSubPersisterJoin(joinRowConsumer);
					return joinRowConsumer;
				}
			});
		});
	}
}
