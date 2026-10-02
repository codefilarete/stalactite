package org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.singletable;

import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.configurer.model.SingleTablePolymorphism;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.GraftableRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.FirstPhaseRelationLoader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.RelationStorage;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.ThreadLocalStorage;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger;
import org.codefilarete.stalactite.engine.runtime.load.JoinNode;
import org.codefilarete.stalactite.engine.runtime.load.SingleTablePolymorphicRelationJoinNode;
import org.codefilarete.stalactite.engine.runtime.singletable.SingleTablePolymorphismReader;
import org.codefilarete.stalactite.query.api.Selectable;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;

public class SingleTableAppender {
	
	public <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	GraftPoint append(EntityJoinTree<SRC, SRCID> aggregateTree,
										SingleTablePolymorphismReader<TRGT, TRGTID, RIGHTTABLE, ?> targetReader,
										GraftableRelation<SRC, TRGT, TRGTID, LEFTTABLE, RIGHTTABLE, JOINID> relation,
										SingleTablePolymorphism<TRGT, TRGTID, Object, RIGHTTABLE> polymorphismPolicy,
										String mountPoint) {
		Column<RIGHTTABLE, Object> discriminatorColumn = polymorphismPolicy.getDiscriminatorColumn();
		
		Set<ConfiguredEntityReader<? extends TRGT, TRGTID, ?>> subPersisters = new HashSet<>(targetReader.getSubEntitiesPersisters().values());
		
		Set<Column<RIGHTTABLE, ?>> columns = targetReader.<RIGHTTABLE>getMainTable().getColumns();
		
		String joinName = aggregateTree.addJoin(mountPoint, parent -> new SingleTablePolymorphicRelationJoinNode<>(
				(JoinNode<SRC, LEFTTABLE>) (JoinNode) parent,
				relation.getAccessor(),
				relation.getLeftKey(),
				relation.getRightKey(),
				OUTER,
				columns,
				null,
				new EntityInflater.EntityMappingAdapter<>(targetReader.getMapping()),
				relation.getRelationFixer(),
				discriminatorColumn,
				subPersisters,
				polymorphismPolicy));
		
		return new GraftPoint<>(relation.getTargetEntity(), targetReader, joinName, aggregateTree);
	}
	
	public <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	String appendForSeparateLoad(EntityJoinTree<?, ?> aggregateTree,
								 SingleTablePolymorphismReader<TRGT, TRGTID, RIGHTTABLE, ?> targetReader,
								 String mountPoint,
								 ThreadLocalStorage<SRC, Set<TRGTID>, RelationStorage<SRC, TRGTID>> relationIdsHolder,
								 KeyMapping<LEFTTABLE, RIGHTTABLE, JOINID> join) {
		
		return aggregateTree.addMergeJoin(mountPoint,
				new FirstPhaseRelationLoader<>(
						targetReader.getMapping().getIdMapping().getIdentifierAssembler()::assemble,
						targetReader.getMapping().getSelectableColumns(),
						relationIdsHolder),
				join.getSourceKey(),
				join.getReferencedKey(),
				OUTER);
	}
	
	public <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	String appendForSeparateLoad(EntityJoinTree<?, ?> aggregateTree,
								 SingleTablePolymorphismReader<TRGT, TRGTID, RIGHTTABLE, ?> targetReader,
								 String mountPoint,
								 BiFunction<Function<ColumnedRow, TRGTID>, Set<? extends Selectable<?>>, EntityMerger<SRC>> relationIdsHolderFactory,
								 KeyMapping<LEFTTABLE, RIGHTTABLE, JOINID> join) {
		
		return aggregateTree.addMergeJoin(mountPoint,
				relationIdsHolderFactory.apply(
						targetReader.getMapping().getIdMapping().getIdentifierAssembler()::assemble,
						targetReader.getMapping().getSelectableColumns()
				),
				join.getSourceKey(),
				join.getReferencedKey(),
				OUTER);
	}
}
