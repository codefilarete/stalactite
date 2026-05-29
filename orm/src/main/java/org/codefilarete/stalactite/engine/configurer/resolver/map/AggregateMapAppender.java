package org.codefilarete.stalactite.engine.configurer.resolver.map;

import java.util.Collections;
import java.util.Map;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedMapRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.AssemblyPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.map.MapResolver.KeyValueRecordPersister;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

public class AggregateMapAppender {
	
	private final MapResolver mapResolver;
	
	public AggregateMapAppender(Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.mapResolver = new MapResolver(dialect, connectionConfiguration);
	}
	
	public <SRC, SRCID, K, V, M extends Map<K, V>, LEFTTABLE extends Table<LEFTTABLE>, MAPTABLE extends Table<MAPTABLE>>
	void append(ConfiguredRelationalPersister<SRC, SRCID> rootPersister,
	            ResolvedMapRelation<SRC, K, V, M, SRCID, LEFTTABLE, MAPTABLE> resolvedRelation,
	            AssemblyPoint<SRC, SRCID, ?, LEFTTABLE> assemblyPawn) {
		
		PropertyAccessor<SRC, M> accessor;
		if (assemblyPawn.getParentJoinPoint().equals(ROOT_JOIN_NAME)) {
			accessor = resolvedRelation.getAccessor();
		} else {
			AccessorChain<SRC, M> shifter = new AccessorChain<>(assemblyPawn.getAccessor(), resolvedRelation.getAccessor());
			shifter.setNullValueHandler(AccessorChain.RETURN_NULL);
			accessor = shifter;
		}
		
		KeyValueRecordPersister<K, V, SRCID, MAPTABLE> keyValueRecordPersister = mapResolver.resolve(
				resolvedRelation,
				assemblyPawn.getRelationOwnerPersister());
		
		rootPersister.getEntityJoinTree().addRelationJoin(
				assemblyPawn.getParentJoinPoint(),
				new EntityMappingAdapter<>(keyValueRecordPersister.getMapping()),
				accessor,
				resolvedRelation.getJoin().getLeftKey(),
				resolvedRelation.getJoin().getRightKey(),
				null,
				OUTER,
				resolvedRelation.getRelationFixer(),
				Collections.emptySet(),
				null);
	}
}

