package org.codefilarete.stalactite.engine.configurer.resolver.polymorphism;

import org.codefilarete.stalactite.engine.configurer.model.EntityPolymorphism;
import org.codefilarete.stalactite.engine.configurer.model.JoinTablePolymorphism;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.model.SingleTablePolymorphism;
import org.codefilarete.stalactite.engine.configurer.model.TablePerClassPolymorphism;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.jointable.JoinTableAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.singletable.SingleTableAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.tableperclass.TablePerClassAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.RelationStorage;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.engine.runtime.jointable.JoinTablePolymorphismReader;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.singletable.SingleTablePolymorphismReader;
import org.codefilarete.stalactite.engine.runtime.tableperclass.TablePerClassPolymorphismReader;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

public class PolymorphicSkeletonAppender {
	
	private final SingleTableAppender singleTableAppender = new SingleTableAppender();
	private final TablePerClassAppender tablePerClassAppender = new TablePerClassAppender();
	private final JoinTableAppender joinTableAppender = new JoinTableAppender();
	
	public <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	GraftPoint append(EntityJoinTree<SRC, SRCID> aggregateTree,
					  EntityPolymorphism<TRGT, Object> polymorphism,
					  ConfiguredEntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
					  ResolvedOneToOneRelation<SRC, TRGT, LEFTTABLE, RIGHTTABLE, JOINID> relation,
					  String mountPoint) {
		GraftPoint result = null;
		if (polymorphism instanceof SingleTablePolymorphism) {
			SingleTablePolymorphismReader<TRGT, TRGTID, RIGHTTABLE, Object> persisterAsSingleTable = (SingleTablePolymorphismReader<TRGT, TRGTID, RIGHTTABLE, Object>) targetPersister;
			
			result = singleTableAppender.append(
					aggregateTree,
					persisterAsSingleTable,
					relation,
					(SingleTablePolymorphism<TRGT, TRGTID, Object, RIGHTTABLE>) polymorphism,
					mountPoint);
		} else if (polymorphism instanceof TablePerClassPolymorphism) {
			TablePerClassPolymorphismReader<TRGT, TRGTID, RIGHTTABLE> persisterAsTablePerClass = (TablePerClassPolymorphismReader<TRGT, TRGTID, RIGHTTABLE>) targetPersister;
			
			result = tablePerClassAppender.append(aggregateTree, persisterAsTablePerClass, relation, mountPoint);
		} else if (polymorphism instanceof JoinTablePolymorphism) {
			JoinTablePolymorphismReader<TRGT, TRGTID, RIGHTTABLE> persisterAsJoinTable = (JoinTablePolymorphismReader<TRGT, TRGTID, RIGHTTABLE>) targetPersister;
			
			result = joinTableAppender.append(aggregateTree, persisterAsJoinTable, relation, mountPoint);
		}
		return result;
	}
	
	public <SRC, SRCID, TRGT, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	String appendForSeparateLoad(EntityJoinTree<?, ?> aggregateTree,
								 EntityPolymorphism<TRGT, Object> polymorphism,
								 ConfiguredEntityReader<TRGT, TRGTID, RIGHTTABLE> targetReader,
								 String mountPoint,
								 ThreadLocal<RelationStorage<SRC, TRGTID>> relationIdsHolder,
								 KeyMapping<LEFTTABLE, RIGHTTABLE, JOINID> join) {
		String result = null;
		if (polymorphism instanceof SingleTablePolymorphism) {
			SingleTablePolymorphismReader<TRGT, TRGTID, RIGHTTABLE, Object> persisterAsSingleTable = (SingleTablePolymorphismReader<TRGT, TRGTID, RIGHTTABLE, Object>) targetReader;
			result = singleTableAppender.appendForSeparateLoad(aggregateTree, persisterAsSingleTable, mountPoint, relationIdsHolder, join);
		} else if (polymorphism instanceof TablePerClassPolymorphism) {
			TablePerClassPolymorphismReader<TRGT, TRGTID, RIGHTTABLE> persisterAsTablePerClass = (TablePerClassPolymorphismReader<TRGT, TRGTID, RIGHTTABLE>) targetReader;
			result = tablePerClassAppender.appendForSeparateLoad(aggregateTree, persisterAsTablePerClass, mountPoint, relationIdsHolder, join);
		} else if (polymorphism instanceof JoinTablePolymorphism) {
			JoinTablePolymorphismReader<TRGT, TRGTID, RIGHTTABLE> persisterAsJoinTable = (JoinTablePolymorphismReader<TRGT, TRGTID, RIGHTTABLE>) targetReader;
			result = joinTableAppender.appendForSeparateLoad(aggregateTree, persisterAsJoinTable, mountPoint, relationIdsHolder, join);
		}
		return result;
	}
}
