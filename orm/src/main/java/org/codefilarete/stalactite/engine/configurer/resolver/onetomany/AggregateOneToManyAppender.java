package org.codefilarete.stalactite.engine.configurer.resolver.onetomany;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.EntityReader;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.collection.Iterables;

public class AggregateOneToManyAppender {
	
	private final AggregateOneToManyWithAssociationTableAppender associationTableGrafter = new AggregateOneToManyWithAssociationTableAppender();
	private final AggregateOneToManyWithIndexedAssociationTableAppender indexedAssociationTableGrafter = new AggregateOneToManyWithIndexedAssociationTableAppender();
	private final AggregateOneToManyWithMappedAssociationAppender mappedAssociationGrafter = new AggregateOneToManyWithMappedAssociationAppender();
	private final AggregateOneToManyWithIndexedMappedAssociationAppender indexedMappedAssociationGrafter = new AggregateOneToManyWithIndexedMappedAssociationAppender();
	
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	GraftPoint append(ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                  EntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                  EntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                  String mountPoint,
	                  EntityJoinTree<SRC, SRCID> aggregateTree) {
		
		// we dispatch the logic to dedicated classes due to their own complexity (especially the indexed ones) and to avoid long and complex methods
		GraftPoint result;
		if (relation.isOwnedByReverseSide()) {
			if (relation.isOrdered()) {
				result = indexedMappedAssociationGrafter.append(relation, sourcePersister, targetPersister, mountPoint, aggregateTree);
			} else {
				result = mappedAssociationGrafter.append(relation, sourcePersister, targetPersister, mountPoint, aggregateTree);
			}
		} else {
			if (relation.isOrdered()) {
				result = indexedAssociationTableGrafter.append(relation, sourcePersister, targetPersister, mountPoint, aggregateTree);
			} else {
				result = associationTableGrafter.append(relation, sourcePersister, targetPersister, mountPoint, aggregateTree);
			}
		}
		
		SelectListener<TRGT, TRGTID> targetSelectListener = targetPersister.getSelectListener();
		sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
				// since ids are not those of its entities, we should not pass them as argument, this will only initialize things if needed
				targetSelectListener.beforeSelect(Collections.emptyList());
			}
			
			@Override
			public void afterSelect(Set<? extends SRC> result) {
				Set<TRGT> collect = Iterables.stream(result).flatMap(src -> Nullable.nullable(relation.getAccessor().get(src))
								.map(Collection::stream)
								.getOr(Stream.empty()))
						.collect(Collectors.toSet());
				targetSelectListener.afterSelect(collect);
			}
			
			@Override
			public void onSelectError(Iterable<SRCID> ids, RuntimeException exception) {
				// since ids are not those of its entities, we should not pass them as argument
				targetSelectListener.onSelectError(Collections.emptyList(), exception);
			}
		});
		return result;
	}
}
