package org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.jointable;

import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.GraftableRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.FirstPhaseRelationLoader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.RelationStorage;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.ThreadLocalStorage;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.engine.runtime.jointable.JoinTablePolymorphismReader;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger;
import org.codefilarete.stalactite.engine.runtime.load.JoinNode;
import org.codefilarete.stalactite.engine.runtime.load.JoinTablePolymorphicRelationJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.MergeJoinNode;
import org.codefilarete.stalactite.engine.runtime.load.PolymorphicMergeJoinRowConsumer;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.function.Hanger.Holder;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;

public class JoinTableAppender {
	
	public <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	GraftPoint append(EntityJoinTree<SRC, SRCID> aggregateTree,
										JoinTablePolymorphismReader<TRGT, TRGTID, RIGHTTABLE, ?> targetPersister,
										GraftableRelation<SRC, TRGT, TRGTID, LEFTTABLE, RIGHTTABLE, JOINID> relation,
										String mountPoint) {
		
		Holder<JoinTablePolymorphicRelationJoinNode<TRGT, LEFTTABLE, RIGHTTABLE, JOINID, TRGTID>> createdJoinHolder = new Holder<>();
		String joinName = aggregateTree.addJoin(mountPoint, parent -> {
			JoinTablePolymorphicRelationJoinNode<TRGT, LEFTTABLE, RIGHTTABLE, JOINID, TRGTID> polymorphicRelationJoinNode = new JoinTablePolymorphicRelationJoinNode<>(
					(JoinNode<SRC, LEFTTABLE>) (JoinNode) parent,
					relation.getAccessor(),
					relation.getLeftKey(),
					relation.getRightKey(),
					OUTER,
					targetPersister.<LEFTTABLE>getMainTable().getColumns(),
					null,
					new EntityInflater.EntityMappingAdapter<>(targetPersister.getMapping()),
					relation.getRelationFixer(),
					null);
			createdJoinHolder.set(polymorphicRelationJoinNode);
			return polymorphicRelationJoinNode;
		});
		
		Set<ConfiguredEntityReader<? extends TRGT, TRGTID, ?>> subPersisters = new HashSet<>(targetPersister.getSubEntitiesPersisters().values());
		addJoinTablePolymorphicSubPersistersJoins(aggregateTree, joinName, targetPersister.getMainTable(), createdJoinHolder.get(), subPersisters);
		
		return new GraftPoint(relation.getTargetEntity(), targetPersister, joinName, aggregateTree);
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
					new EntityMerger.EntityMergerAdapter<>((EntityMapping<D, I, ?>) localSubPersister.getMapping())) {
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
	
	public <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	String appendForSeparateLoad(EntityJoinTree<?, ?> aggregateTree,
								 JoinTablePolymorphismReader<TRGT, TRGTID, RIGHTTABLE, ?> targetReader,
								 String mountPoint,
								 ThreadLocalStorage<SRC, Set<TRGTID>, RelationStorage<SRC, TRGTID>> relationIdsHolder,
								 KeyMapping<LEFTTABLE, RIGHTTABLE, JOINID> join) {
		
		return aggregateTree.addMergeJoin(mountPoint,
				new FirstPhaseRelationLoader<>(targetReader.getMapping().getIdMapping().getIdentifierAssembler()::assemble, targetReader.getMapping().getSelectableColumns(), relationIdsHolder),
				join.getSourceKey(),
				join.getReferencedKey(),
				OUTER);
	}
	
	public <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	String appendForSeparateLoad(EntityJoinTree<?, ?> aggregateTree,
								 JoinTablePolymorphismReader<TRGT, TRGTID, RIGHTTABLE, ?> targetReader,
								 String mountPoint,
								 BiFunction<Function<ColumnedRow, TRGTID>, Set<? extends Selectable<?>>, EntityMerger<SRC>> relationIdsHolderFactory,
								 KeyMapping<LEFTTABLE, RIGHTTABLE, JOINID> join) {
		
		return aggregateTree.addMergeJoin(mountPoint,
				relationIdsHolderFactory.apply(targetReader.getMapping().getIdMapping().getIdentifierAssembler()::assemble, targetReader.getMapping().getSelectableColumns()),
				join.getSourceKey(),
				join.getReferencedKey(),
				OUTER);
	}
}
