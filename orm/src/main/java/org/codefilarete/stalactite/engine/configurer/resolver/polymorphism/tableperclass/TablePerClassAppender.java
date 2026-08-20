package org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.tableperclass;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.codefilarete.stalactite.engine.SelectExecutor;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger.EntityMergerAdapter;
import org.codefilarete.stalactite.engine.runtime.load.JoinNode;
import org.codefilarete.stalactite.engine.runtime.load.MergeJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.PolymorphicMergeJoinRowConsumer;
import org.codefilarete.stalactite.engine.runtime.load.TablePerClassPolymorphicRelationJoinNode;
import org.codefilarete.stalactite.engine.runtime.tableperclass.TablePerClassPolymorphismReader;
import org.codefilarete.stalactite.query.api.JoinLink;
import org.codefilarete.stalactite.query.api.QueryStatement;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.query.api.Selectable.SimpleSelectable;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Union;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.trace.MutableInt;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.stalactite.query.Operators.cast;

public class TablePerClassAppender {
	
	public static final String ENTITY_TYPE_DISCRIMINATOR_NAME = "clazz_";
	
	public TablePerClassAppender() {
	}
	
	public <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	GraftPoint append(EntityJoinTree<SRC, SRCID> aggregateTree,
					  TablePerClassPolymorphismReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
					  ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> relation,
	                  String mountPoint) {
		
		Set<ConfiguredEntityReader<? extends TRGT, TRGTID, ?>> subPersisters = new HashSet<>(targetPersister.getSubEntitiesPersisters().values());
		
		DirectRelationJoin<LEFTTABLE, RIGHTTABLE, JOINID> join = relation.getJoin();
		
		TablePerClassUnion<?, TRGTID> union = buildUnion(subPersisters, targetPersister.getMapping().getSelectableColumns());
		
		Holder<TablePerClassPolymorphicRelationJoinNode<TRGT, LEFTTABLE, JOINID, TRGTID>> createdJoinHolder = new Holder<>();
		String joinName = aggregateTree.addJoin(mountPoint, parent -> {
			TablePerClassPolymorphicRelationJoinNode<TRGT, LEFTTABLE, JOINID, TRGTID> relationJoinNode = new TablePerClassPolymorphicRelationJoinNode<>(
					(JoinNode<SRC, LEFTTABLE>) (JoinNode) parent,
					union,
					relation.getAccessor(),
					join.getLeftKey(),
					join.getRightKey(),
					OUTER,
					union.getColumns(),
					// we set a table alias else it is null which is transformed as "null" into the SQL which works
					// but is not very clean and may cause problems if we have several table-per-class (all would
					// be named "null") in the aggregate
					relation.getTargetEntity().getEntityType().getSimpleName(),
					new EntityInflater.EntityMappingAdapter<>(targetPersister.getMapping()),
					(BeanRelationFixer<Object, TRGT>) relation.getRelationFixer(),
					union.getDiscriminatorColumn());
			
			createdJoinHolder.set(relationJoinNode);
			return relationJoinNode;
		});
		
		addTablePerClassPolymorphicSubPersistersJoins(aggregateTree, relation.getTargetEntity().getTable(), joinName, createdJoinHolder.get(), subPersisters);
		
		return new GraftPoint(relation.getTargetEntity(), targetPersister, joinName, aggregateTree);
		
	}
	
	private <SRC, SRCID, C, I, V extends C, T extends Table<T>> void addTablePerClassPolymorphicSubPersistersJoins(
			EntityJoinTree<SRC, SRCID> entityJoinTree,
			Table<?> templateTable,
			String mainPolymorphicJoinNodeName,
			TablePerClassPolymorphicRelationJoinNode<C, T, ?, I> mainPersisterJoin,
			Set<ConfiguredEntityReader<? extends C, I, ?>> subPersisters) {
		
		// The join is made on the Union as left table, and the column we must get is mainPersister's primaryKey (which table is not in the tree since
		// we are the table-per-class case), so we have to create an equivalent of the primary key, based on the columns of the union
		Key.KeyBuilder<QueryStatement.PseudoTable, I> leftKey = Key.from(mainPersisterJoin.getRightTable());
		templateTable.getPrimaryKey().getColumns().forEach(pkCol -> {
			JoinLink<QueryStatement.PseudoTable, ?> selectable = (JoinLink<QueryStatement.PseudoTable, ?>) Iterables.find(mainPersisterJoin.getColumnsToSelect(), selectableColumn -> selectableColumn.getExpression().equals(pkCol.getName()));
			leftKey.addColumn(selectable);
		});
		MutableInt discriminatorComputer = new MutableInt();
		subPersisters.forEach(subPersister -> {
			ConfiguredEntityReader<V, I, ?> localSubPersister = (ConfiguredEntityReader<V, I, ?>) subPersister;
			entityJoinTree.addMergeJoin(mainPolymorphicJoinNodeName,
					new EntityMergerAdapter<>(localSubPersister.getMapping()),
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
	
	private <TRGT, TRGTID, RIGHTTABLE extends Table<RIGHTTABLE>, SUBTRGT extends TRGT>
	TablePerClassUnion<SUBTRGT, TRGTID> buildUnion(Iterable<? extends ConfiguredEntityReader<? extends TRGT, TRGTID, ?>> subPersisters,
												   Set<Column<RIGHTTABLE, ?>> persisterColumns) {
		// Union will contain only 3 columns :
		// - discriminator
		// - entity primary key
		// - join column
		TablePerClassUnion<SUBTRGT, TRGTID> result = new TablePerClassUnion<>(ENTITY_TYPE_DISCRIMINATOR_NAME);
		
		// we build a union of all sub queries that will be joined in the main query
		// To build the union we need the columns that are common to all persisters
		Set<JoinLink<?, ?>> commonColumns = new KeepOrderSet<>();
		commonColumns.addAll(persisterColumns);

		Set<String> commonColumnsNames = commonColumns.stream().map(JoinLink::getExpression).collect(Collectors.toSet());
		
		KeepOrderSet<Column<?, ?>> nonCommonColumns = new KeepOrderSet<>();
		subPersisters.forEach(subPersister -> {
			nonCommonColumns.addAll(subPersister.getMapping().getSelectableColumns());
		});
		nonCommonColumns.removeIf(c -> commonColumnsNames.contains(c.getName()));
		
		MutableInt discriminatorComputer = new MutableInt();
		
		subPersisters.forEach(subPersister -> {
			Query subEntityQuery = new Query(subPersister.getMainTable());
			int discriminatorValue = discriminatorComputer.increment();
			result.getSubtypeSelectorPerDiscriminatorValue().put(discriminatorValue, (SelectExecutor<SUBTRGT, TRGTID>) subPersister);
			subEntityQuery.select(String.valueOf(discriminatorValue), Integer.class, ENTITY_TYPE_DISCRIMINATOR_NAME);
			result.unionAll(subEntityQuery);
			
			commonColumns.forEach(column -> {
				subEntityQuery.select(column.getExpression(), column.getJavaType());
				result.registerColumn(column.getExpression(), column.getJavaType());
			});

			nonCommonColumns.forEach(column -> {
				Selectable<?> expression;
				if (subPersister.getMapping().getSelectableColumns().contains(column)) {
					expression = new SimpleSelectable<>(column.getName(), column.getJavaType());
				} else {
					expression = cast((String) null, column.getJavaType());
				}
				// we put an alias else cast(..) as no name which makes it doesn't match official-column name, and then
				// may cause an error since SQL is kind of invalid
				subEntityQuery.select(expression, column.getName());
				result.registerColumn(column.getName(), column.getJavaType());
			});
		});
		return result;
	}
	
	private static class TablePerClassUnion<SUBTRGT, TRGTID> extends Union {
		
		private final PseudoColumn<Integer> discriminatorColumn;
		
		private final Map<Integer, SelectExecutor<SUBTRGT, TRGTID>> subtypeSelectorPerDiscriminatorValue = new HashMap<>();
		
		private TablePerClassUnion(String discriminatorColumnName) {
			this.discriminatorColumn = registerColumn(discriminatorColumnName, Integer.class);
		}
		
		public PseudoColumn<Integer> getDiscriminatorColumn() {
			return discriminatorColumn;
		}
		
		public Map<Integer, SelectExecutor<SUBTRGT, TRGTID>> getSubtypeSelectorPerDiscriminatorValue() {
			return subtypeSelectorPerDiscriminatorValue;
		}
	}
}
