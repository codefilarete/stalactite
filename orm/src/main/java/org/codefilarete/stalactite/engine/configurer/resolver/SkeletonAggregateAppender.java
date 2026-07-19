package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.Set;

import org.codefilarete.stalactite.engine.EntityReadWriteExecutor;
import org.codefilarete.stalactite.engine.EntityWriteExecutor;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.engine.runtime.load.EntityMerger.EntityMergerAdapter;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.INNER;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;


/**
 * Class aiming at grafting the collected persisters from the structural "bones" of an {@link Entity}, which are
 * - inheritance (ancestors, not polymorphism)
 * - extra tables
 */
public class SkeletonAggregateAppender {
	
	public <B, C extends B, I, RIGHTTABLE extends Table<RIGHTTABLE>>
	void appendInheritance(CreatedPersisterCollector<C, I> persisterCollector, EntityJoinTree<?, ?> aggregateTree) {
		
		// First we deal with the root persister and its extra-tables
		EntityReadWriteExecutor<C, I> rootPersister = persisterCollector.getPersister();
		KeepOrderSet<EntityWriteExecutor<C, I>> extraPersisters = persisterCollector.getExtraPersisters().get(rootPersister);
		if (extraPersisters != null) {
			graftExtraPersisters(aggregateTree, rootPersister, extraPersisters, ROOT_JOIN_NAME);
		}
		
		// Special case: the already-assigned identifier pattern expects to set up the entity persistence status on
		// load, else, the status is always "not persisted" due to the instantiation default value. This is particularly
		// true for composite identifiers or wrapping ones (less for single-value). Thus, we transfer it to the persister.
		IdentifierInsertionManager<C, I> identifierInsertionManager = rootPersister.getMapping().getIdMapping().getIdentifierInsertionManager();
		if (identifierInsertionManager instanceof AlreadyAssignedIdentifierManager) {
			// Transferring identifier manager SelectListener to here
			rootPersister.addSelectListener(((AlreadyAssignedIdentifierManager<C, I>) identifierInsertionManager).getSelectListener());
		}
		
		// Then we deal with its ancestors and their extra-table too
		persisterCollector.getAncestorPersisters().forEach(ancestorPersister -> {
			// we add a merge join between the root persister and its ancestors so that it can load the ancestor table when loading the entity
			String mergeJoinName = aggregateTree.addMergeJoin(ROOT_JOIN_NAME,
					new EntityMergerAdapter<C, RIGHTTABLE>(ancestorPersister.getMapping()),
					rootPersister.getMapping().getTargetTable().getPrimaryKey(),
					ancestorPersister.getMapping().getTargetTable().getPrimaryKey(),
					// the join is mandatory, because we deal with ancestors
					INNER
			);
			KeepOrderSet<EntityWriteExecutor<C, I>> ancestorExtraPersisters = persisterCollector.getExtraPersisters().get(ancestorPersister);
			if (ancestorExtraPersisters != null) {
				graftExtraPersisters(aggregateTree, ancestorPersister, ancestorExtraPersisters, mergeJoinName);
			}
		});
	}
	
	private <B, C extends B, I, RIGHTTABLE extends Table<RIGHTTABLE>>
	void graftExtraPersisters(EntityJoinTree<?, ?> aggregateTree,
	                          EntityWriteExecutor<C, I> persister,
	                          Set<EntityWriteExecutor<C, I>> extraPersisters,
	                          String joinPointName) {
		extraPersisters.forEach(extraPersister ->
				// When dealing with extra-tables, a merge join is sufficient to load the extended properties
				aggregateTree.addMergeJoin(joinPointName,
						new EntityMergerAdapter<C, RIGHTTABLE>(extraPersister.getMapping()),
						persister.getMapping().getTargetTable().getPrimaryKey(),
						extraPersister.getMapping().getTargetTable().getPrimaryKey(),
						OUTER
				));
	}
}
