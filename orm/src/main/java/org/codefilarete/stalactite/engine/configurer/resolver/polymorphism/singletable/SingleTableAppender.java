package org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.singletable;

import java.util.HashSet;
import java.util.Set;

import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.model.SingleTablePolymorphism;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.FirstPhaseRelationLoader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.RelationStorage;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.JoinNode;
import org.codefilarete.stalactite.engine.runtime.load.SingleTablePolymorphicRelationJoinNode;
import org.codefilarete.stalactite.engine.runtime.singletable.SingleTablePolymorphismReader;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;

public class SingleTableAppender {
	
	public <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	GraftPoint append(EntityJoinTree<SRC, SRCID> aggregateTree,
					  SingleTablePolymorphismReader<TRGT, TRGTID, RIGHTTABLE, ?> targetPersister,
					  ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> relation,
					  SingleTablePolymorphism<TRGT, TRGTID, Object, RIGHTTABLE> polymorphismPolicy,
					  String mountPoint) {
		Column<RIGHTTABLE, Object> discriminatorColumn = polymorphismPolicy.getDiscriminatorColumn();
		
		Set<ConfiguredEntityReader<? extends TRGT, TRGTID, ?>> subPersisters = new HashSet<>(targetPersister.getSubEntitiesPersisters().values());
		
		Set<Column<RIGHTTABLE, ?>> columns = targetPersister.<RIGHTTABLE>getMainTable().getColumns();
		DirectRelationJoin<LEFTTABLE, RIGHTTABLE, JOINID> join = relation.getJoin();
		
		String joinName = aggregateTree.addJoin(mountPoint, parent -> new SingleTablePolymorphicRelationJoinNode<>(
				(JoinNode<SRC, LEFTTABLE>) (JoinNode) parent,
				relation.getAccessor(),
				join.getLeftKey(),
				join.getRightKey(),
				OUTER,
				columns,
				null,
				new EntityInflater.EntityMappingAdapter<>(targetPersister.getMapping()),
				(BeanRelationFixer<Object, TRGT>) relation.getRelationFixer(),
				discriminatorColumn,
				subPersisters,
				polymorphismPolicy));
		
		return new GraftPoint<>(relation.getTargetEntity(), targetPersister, joinName, aggregateTree);
	}
	
	public <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	String appendForSeparateLoad(EntityJoinTree<SRC, SRCID> aggregateTree,
								 SingleTablePolymorphismReader<TRGT, TRGTID, RIGHTTABLE, ?> targetReader,
								 ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> relation,
								 String mountPoint,
								 ThreadLocal<RelationStorage<SRC, TRGTID>> relationIdsHolder) {
		
		DirectRelationJoin<LEFTTABLE, RIGHTTABLE, JOINID> join = relation.getJoin();
		
		return aggregateTree.addMergeJoin(mountPoint,
				new FirstPhaseRelationLoader<>(targetReader.getMapping().getIdMapping().getIdentifierAssembler()::assemble, targetReader.getMapping().getSelectableColumns(), relationIdsHolder),
				join.getLeftKey(),
				join.getRightKey(),
				OUTER);
	}
}
