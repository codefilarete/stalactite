package org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.tableperclass;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.GraftableRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.FirstPhaseRelationLoader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.RelationStorage;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.ThreadLocalStorage;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger.EntityMergerAdapter;
import org.codefilarete.stalactite.engine.runtime.load.JoinNode;
import org.codefilarete.stalactite.engine.runtime.load.MergeJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.PolymorphicMergeJoinRowConsumer;
import org.codefilarete.stalactite.engine.runtime.load.TablePerClassPolymorphicRelationJoinNode;
import org.codefilarete.stalactite.engine.runtime.tableperclass.TablePerClassPolymorphismReader;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.query.api.QualifiedSelectable;
import org.codefilarete.stalactite.query.api.QueryStatement.PseudoColumn;
import org.codefilarete.stalactite.query.api.QueryStatement.PseudoTable;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.query.api.Selectable.SimpleSelectable;
import org.codefilarete.stalactite.query.model.Query;
import org.codefilarete.stalactite.query.model.Union;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Key.KeyBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.collection.Collections;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.function.Hanger.Holder;
import org.codefilarete.tool.trace.MutableInt;

import static java.util.Comparator.comparing;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.stalactite.query.Operators.cast;

public class TablePerClassAppender {
	
	public static final String ENTITY_TYPE_DISCRIMINATOR_NAME = "clazz_";
	
	public TablePerClassAppender() {
	}
	
	public <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, SUBTRGT extends TRGT, JOINID>
	GraftPoint append(EntityJoinTree<SRC, SRCID> aggregateTree,
					  TablePerClassPolymorphismReader<TRGT, TRGTID, RIGHTTABLE, SUBTRGT> targetReader,
					  GraftableRelation<SRC, TRGT, TRGTID, LEFTTABLE, RIGHTTABLE, JOINID> relation,
					  String mountPoint) {
		
		// we sort the sub persister in a stable way to have a deterministic order of the discriminator values for tests stability
		Set<ConfiguredEntityReader<SUBTRGT, TRGTID, ?>> subPersisters = new TreeSet<>(comparing(subPersister -> subPersister.getMapping().getTargetTable().getName()));
		subPersisters.addAll(targetReader.getSubEntitiesPersisters().values());
		
		// We add the right join key which can be either the primary key (if relation is owned by source) or a foreign key (if relation is owned by target)
		// If it is the primary key it will be a duplicate because templateColumns already contains it but won't
		// be added since we use a Set
		Set<Column<RIGHTTABLE, ?>> unionColumns = Collections.cat(KeepOrderSet::new, targetReader.getMapping().getSelectableColumns(), relation.getRightKey().getColumns());
		TablePerClassUnion<?, TRGTID> union = buildUnion(subPersisters, unionColumns);
		PseudoTable pseudoTable = union.asPseudoTable(targetReader.getClassToPersist().getSimpleName());
		PseudoColumn<Integer> discriminatorPseudoColumn = pseudoTable.findColumn(union.getDiscriminatorColumn().getExpression());
		
		Holder<TablePerClassPolymorphicRelationJoinNode<TRGT, LEFTTABLE, JOINID, TRGTID>> createdJoinHolder = new Holder<>();
		String joinName = aggregateTree.addJoin(mountPoint, parent -> {
			TablePerClassPolymorphicRelationJoinNode<TRGT, LEFTTABLE, JOINID, TRGTID> relationJoinNode = new TablePerClassPolymorphicRelationJoinNode<>(
					(JoinNode<SRC, LEFTTABLE>) (JoinNode) parent,
					union,
					relation.getAccessor(),
					relation.getLeftKey(),
					relation.getRightKey(),
					OUTER,
					pseudoTable.getColumns(),
					// we set a table alias else it is null which is transformed as "null" into the SQL which works
					// but is not very clean and may cause problems if we have several table-per-class (all would
					// be named "null") in the aggregate
					targetReader.getClassToPersist().getSimpleName(),
					new EntityMappingAdapter<>(targetReader.getMapping()),
					relation.getRelationFixer(),
					discriminatorPseudoColumn);
			
			createdJoinHolder.set(relationJoinNode);
			return relationJoinNode;
		});
		
		addTablePerClassPolymorphicSubPersistersJoins(aggregateTree, relation.getTargetEntity().getTable(), joinName, createdJoinHolder.get(), subPersisters);
		
		return new GraftPoint(relation.getTargetEntity(), targetReader, joinName, aggregateTree);
	}
	
	private <SRC, SRCID, C, I, V extends C, T extends Table<T>, SUBTRGT extends C> void addTablePerClassPolymorphicSubPersistersJoins(
			EntityJoinTree<SRC, SRCID> entityJoinTree,
			Table<?> templateTable,
			String mainPolymorphicJoinNodeName,
			TablePerClassPolymorphicRelationJoinNode<C, T, ?, I> mainPersisterJoin,
			Set<ConfiguredEntityReader<SUBTRGT, I, ?>> subPersisters) {
		
		// The join is made on the Union as left table, and the column we must get is mainPersister's primaryKey (which table is not in the tree since
		// we are the table-per-class case), so we have to create an equivalent of the primary key, based on the columns of the union
		KeyBuilder<PseudoTable, I> leftKey = Key.from(mainPersisterJoin.getRightTable());
		templateTable.getPrimaryKey().getColumns().forEach(pkCol -> {
			QualifiedSelectable<PseudoTable, ?> selectable = (QualifiedSelectable<PseudoTable, ?>) Iterables.find(mainPersisterJoin.getColumnsToSelect(), selectableColumn -> selectableColumn.getExpression().equals(pkCol.getName()));
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
	
	private static <TRGT, TRGTID, RIGHTTABLE extends Table<RIGHTTABLE>, SUBTRGT extends TRGT>
	TablePerClassUnion<SUBTRGT, TRGTID> buildUnion(
			Collection<? extends ConfiguredEntityReader<SUBTRGT, TRGTID, ?>> subPersistersInput,
			Collection<Column<RIGHTTABLE, ?>> commonColumns) {
		
		// we reuse the base of the union build and amend it with the columns of the sub-persisters
		TablePerClassUnion<SUBTRGT, TRGTID> result = buildUnionForIdentifierRead(subPersistersInput, commonColumns);
		
		// adding sub-persisters columns
		Set<String> commonColumnsNames = Iterables.collect(commonColumns, QualifiedSelectable::getExpression, HashSet::new);
		KeepOrderSet<Column<?, ?>> nonCommonColumns = new KeepOrderSet<>();
		subPersistersInput.forEach(subPersister -> {
			nonCommonColumns.addAll(subPersister.getMapping().getSelectableColumns());
		});
		nonCommonColumns.removeIf(c -> commonColumnsNames.contains(c.getName()));
		
		subPersistersInput.forEach(subPersister -> {
			Query subQuery = result.findSubQuery(subPersister);
			nonCommonColumns.forEach(column -> {
				Selectable<?> expression;
				if (subPersister.getMapping().getSelectableColumns().contains(column)) {
					expression = new SimpleSelectable<>(column.getName(), column.getJavaType());
				} else {
					expression = cast((String) null, column.getJavaType());
				}
				// we put an alias else cast(..) as no name which makes it doesn't match official-column name, and then
				// may cause an error since SQL is kind of invalid
				subQuery.select(expression, column.getName());
				result.registerColumn(column.getName(), column.getJavaType());
			});
		});
		return result;
	}
	
	public <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, SUBTRGT extends TRGT, JOINID>
	String appendForSeparateLoad(EntityJoinTree<?, ?> aggregateTree,
								 TablePerClassPolymorphismReader<TRGT, TRGTID, RIGHTTABLE, SUBTRGT> targetReader,
								 String mountPoint,
								 ThreadLocalStorage<SRC, Set<TRGTID>, RelationStorage<SRC, TRGTID>> relationIdsHolder,
								 KeyMapping<LEFTTABLE, RIGHTTABLE, JOINID> join) {
		
		Collection<ConfiguredEntityReader<SUBTRGT, TRGTID, ?>> subPersisters = targetReader.getSubEntitiesPersisters().values();
		
		// We add the right join key which can be either the primary key (if relation is owned by source) or a foreign key (if relation is owned by target)
		// If it is the primary key it will be a duplicate because templateColumns already contains it but won't
		// be added since we use a Set
		Set<Column<RIGHTTABLE, ?>> unionColumns = Collections.cat(KeepOrderSet::new, targetReader.getMapping().getSelectableColumns(), join.getReferencedKey().getColumns());
		TablePerClassUnion<SUBTRGT, TRGTID> union = buildUnionForIdentifierRead(subPersisters, unionColumns);
		PseudoTable pseudoTable = union.asPseudoTable(targetReader.getClassToPersist().getSimpleName());
		Map<String, PseudoColumn<?>> unionColumnsPerName = pseudoTable.mapColumnsOnName();
		IdentifierAssembler<TRGTID, RIGHTTABLE> targetIdentifierAssembler = targetReader.getMapping().getIdMapping().getIdentifierAssembler();
		Function<ColumnedRow, TRGTID> idMapping = columnedRow -> targetIdentifierAssembler.assemble(new ColumnedRow() {
			@Override
			public <E> E get(Selectable<E> pkColumn) {
				// we get the column from the union by its name, not from the target table, because the join is made on the union
				Selectable<?> joinLink = unionColumnsPerName.get(pkColumn.getExpression());
				if (joinLink == null) {
					throw new IllegalStateException("No column found in union for " + pkColumn);
				}
				return (E) columnedRow.get(joinLink);
			}
		});
		
		// we build a key for the right side of the join, based on the pseudo table that is the union of all sub persisters
		KeyBuilder<PseudoTable, JOINID> rightKey = Key.from(pseudoTable);
		join.getReferencedKey().getColumns().forEach(referencedColumn -> {
			rightKey.addColumn(unionColumnsPerName.get(referencedColumn.getName()));
		});
		
		String joinName = aggregateTree.addMergeJoin(mountPoint,
				new FirstPhaseRelationLoader<>(idMapping, pseudoTable.getColumns(), relationIdsHolder),
				join.getSourceKey(),
				rightKey.build(),
				OUTER);
		
		pseudoTable.getColumns().forEach(column -> {
			((MergeJoinNode) aggregateTree.getJoin(joinName)).getOriginalColumnsToLocalOnes().put(column, column);
		});
		return joinName;
	}
	
	public <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, SUBTRGT extends TRGT, JOINID>
	String appendForSeparateLoad(EntityJoinTree<?, ?> aggregateTree,
								 TablePerClassPolymorphismReader<TRGT, TRGTID, RIGHTTABLE, SUBTRGT> targetReader,
								 String mountPoint,
								 BiFunction<Function<ColumnedRow, TRGTID>, Set<? extends Selectable<?>>, EntityMerger<SRC>> relationIdsHolderFactory,
								 KeyMapping<LEFTTABLE, RIGHTTABLE, JOINID> join) {
		
		Collection<ConfiguredEntityReader<SUBTRGT, TRGTID, ?>> subPersisters = targetReader.getSubEntitiesPersisters().values();
		
		TablePerClassUnion<SUBTRGT, TRGTID> union = buildUnionForIdentifierRead(subPersisters, targetReader.getMapping().getSelectableColumns());
		PseudoTable pseudoTable = union.asPseudoTable(targetReader.getClassToPersist().getSimpleName());
		Map<String, PseudoColumn<?>> unionColumnsPerName = pseudoTable.mapColumnsOnName();
		IdentifierAssembler<TRGTID, RIGHTTABLE> targetIdentifierAssembler = targetReader.getMapping().getIdMapping().getIdentifierAssembler();
		Function<ColumnedRow, TRGTID> idMapping = columnedRow -> targetIdentifierAssembler.assemble(new ColumnedRow() {
			@Override
			public <E> E get(Selectable<E> pkColumn) {
				// we get the column from the union by its name, not from the target table, because the join is made on the union
				Selectable<?> joinLink = unionColumnsPerName.get(pkColumn.getExpression());
				if (joinLink == null) {
					throw new IllegalStateException("No column found in union for " + pkColumn);
				}
				return (E) columnedRow.get(joinLink);
			}
		});
		
		// we build a key for the right side of the join, based on the pseudo table that is the union of all sub persisters
		KeyBuilder<PseudoTable, JOINID> rightKey = Key.from(pseudoTable);
		join.getReferencedKey().getColumns().forEach(referencedColumn -> {
			rightKey.addColumn(unionColumnsPerName.get(referencedColumn.getName()));
		});
		
		String joinName = aggregateTree.addMergeJoin(mountPoint,
				relationIdsHolderFactory.apply(idMapping, pseudoTable.getColumns()),
				join.getSourceKey(),
				rightKey.build(),
				OUTER);
		
		pseudoTable.getColumns().forEach(column -> {
			((MergeJoinNode) aggregateTree.getJoin(joinName)).getOriginalColumnsToLocalOnes().put(column, column);
		});
		return joinName;
	}
	
	public static <TRGT, TRGTID, RIGHTTABLE extends Table<RIGHTTABLE>, SUBTRGT extends TRGT>
	TablePerClassUnion<SUBTRGT, TRGTID> buildUnionForIdentifierRead(
			Collection<? extends ConfiguredEntityReader<SUBTRGT, TRGTID, ?>> subPersistersInput,
			Collection<Column<RIGHTTABLE, ?>> commonColumns) {
		
		// Union will contain only 2 columns :
		// - discriminator
		// - entity primary key
		TablePerClassUnion<SUBTRGT, TRGTID> result = new TablePerClassUnion<>(ENTITY_TYPE_DISCRIMINATOR_NAME);
		
		// we sort the sub persister in a stable way to have a deterministic order of the discriminator values for tests stability
		Set<ConfiguredEntityReader<SUBTRGT, TRGTID, ?>> subPersisters = new TreeSet<>(comparing(subPersister -> subPersister.getMainTable().getName()));
		subPersisters.addAll(subPersistersInput);
		
		MutableInt discriminatorComputer = new MutableInt();
		subPersisters.forEach(subPersister -> {
			Query subEntityQuery = new Query(subPersister.getMainTable());
			
			// Adding primary key column (we need only them because other sub-entity columns are not used in the union,
			// they are given through a join on the sub-entity table, see addTablePerClassPolymorphicSubPersistersJoins(..))
			// Please note that sub-entity primary key should also be added through the commonColumns because
			// it is a common column, but we add it for robustness. THIS CODE MIGHT BE REMOVED if we are sure that the
			// commonColumns always contains the primary key columns
			subPersister.getMainTable().getPrimaryKey().getColumns().forEach(pkColumn -> {
				subEntityQuery.select(pkColumn, pkColumn.getName());
				result.registerColumn(pkColumn.getName(), pkColumn.getJavaType());
			});
			
			commonColumns.forEach(commonColumn -> {
				// we look for the equivalent / projected column in the sub-persister table because we're building
				// the sub-entity query
				Column subPersisterColumn = subPersister.getMainTable().findColumn(commonColumn.getExpression());
				subEntityQuery.select(subPersisterColumn, subPersisterColumn.getName());
				result.registerColumn(subPersisterColumn.getName(), subPersisterColumn.getJavaType());
			});
			
			// Adding discriminator column
			int discriminatorValue = discriminatorComputer.increment();
			result.addSubQueryPersister(subPersister, subEntityQuery, discriminatorValue);
		});
		return result;
	}
	
	public static class TablePerClassUnion<SUBTRGT, TRGTID> extends Union {
		
		private final SimpleSelectable<Integer> discriminatorColumn;
		
		private final Map<Integer, ConfiguredEntityReader<SUBTRGT, TRGTID, ?>> subtypeSelectorPerDiscriminatorValue = new HashMap<>();
		
		private final Map<ConfiguredEntityReader<SUBTRGT, TRGTID, ?>, Query> subQueryPerSubtypeSelector = new HashMap<>();
		
		public TablePerClassUnion(String discriminatorColumnName) {
			this.discriminatorColumn = registerColumn(discriminatorColumnName, Integer.class);
		}
		
		public SimpleSelectable<Integer> getDiscriminatorColumn() {
			return discriminatorColumn;
		}
		
		private void addSubQueryPersister(ConfiguredEntityReader<SUBTRGT, TRGTID, ?> subPersister, Query subQuery, Integer discriminatorValue) {
			unionAll(subQuery);
			subQuery.select(String.valueOf(discriminatorValue), Integer.class, discriminatorColumn.getExpression());
			// we register the info for latter use
			subQueryPerSubtypeSelector.put(subPersister, subQuery);
			subtypeSelectorPerDiscriminatorValue.put(discriminatorValue, subPersister);
		}
		
		public Map<Integer, ConfiguredEntityReader<SUBTRGT, TRGTID, ?>> getSubtypeSelectorPerDiscriminatorValue() {
			return subtypeSelectorPerDiscriminatorValue;
		}
		
		private Query findSubQuery(ConfiguredEntityReader<SUBTRGT, TRGTID, ?> subPersister) {
			return subQueryPerSubtypeSelector.get(subPersister);
		}
	}
}
