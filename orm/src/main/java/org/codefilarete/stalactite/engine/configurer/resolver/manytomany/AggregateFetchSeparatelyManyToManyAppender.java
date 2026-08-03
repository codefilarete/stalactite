package org.codefilarete.stalactite.engine.configurer.resolver.manytomany;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.codefilarete.reflection.PropertyAccessPoint;
import org.codefilarete.stalactite.engine.configurer.AssociationRecordMapping;
import org.codefilarete.stalactite.engine.configurer.model.IntermediaryRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.AssociationTableLoader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.SecondPhaseSelectListener;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.ThreadLocalRelationStorage;
import org.codefilarete.stalactite.engine.runtime.AssociationRecord;
import org.codefilarete.stalactite.engine.runtime.AssociationTable;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.query.api.JoinLink;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

/**
 * Loads an unordered many-to-many relation in 2 phases : the aggregate's {@link EntityJoinTree} is left untouched, so
 * the main query doesn't return the cartesian product of all the aggregate relations. Instead, a dedicated
 * {@link AssociationTableLoader}, which owns its own {@link EntityJoinTree} rooted on the association table, selects
 * the association records joined with their target entities from the identifiers of the already-loaded owning
 * entities. Target entities are then sewn onto their owner.
 * <p>
 * Note that the 2-phase load is not a lazy loading : no query is triggered in the background when accessing the
 * collection, everything is loaded eagerly so that the whole aggregate is available and coherent when returned.
 *
 * @author Guillaume Mary
 * @see AssociationTableLoader
 * @see AggregateJoinedManyToManyAppender
 */
public class AggregateFetchSeparatelyManyToManyAppender {
	
	/**
	 * Registers the second phase of the load onto the source persister: once the owning entities are loaded, a single
	 * query, built from the {@link EntityJoinTree} of a dedicated {@link AssociationTableLoader}, selects the
	 * association records joined with their target entities from the identifiers of the owning entities. Target
	 * entities are gathered in memory while the result set is read, then sewn onto their owner.
	 *
	 * @return the graft point of the target entity : the loader's tree, so that the relations of the target entity are
	 * 		   loaded by that very same second-phase query
	 */
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>,
			LEFTTABLE extends Table<LEFTTABLE>,
			RIGHTTABLE extends Table<RIGHTTABLE>,
			ASSOCIATIONTABLE extends AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	GraftPoint append(ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                  ConfiguredEntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                  ConfiguredEntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                  Dialect dialect,
	                  ConnectionProvider connectionProvider) {
		
		IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join =
				(IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID>) relation.getJoin();
		ASSOCIATIONTABLE associationTable = join.getJoinTable();
		AssociationRecordMapping<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID> associationRecordMapping = new AssociationRecordMapping<>(
				associationTable,
				sourcePersister.getMapping().getIdMapping().getIdentifierAssembler(),
				targetPersister.getMapping().getIdMapping().getIdentifierAssembler());
		
		Map<JoinLink<LEFTTABLE, ?>, JoinLink<ASSOCIATIONTABLE, ?>> sourcePkToAssociationTableKey =
				new KeyMapping<>(join.getLeftKey(), join.getLeftAssociationKey()).getMapping();
		
		AssociationTableLoader<AssociationRecord, AssociationRecord, SRC, SRCID, LEFTTABLE, ASSOCIATIONTABLE> associationRecordLoader =
				new AssociationTableLoader<>(
						sourcePersister.getMapping().getIdMapping(),
						associationRecordMapping,
						sourcePkToAssociationTableKey,
						dialect,
						connectionProvider);
		
		// the target entity is joined onto the loader's tree so that association records and target entities are
		// loaded by one and only one query. Entities are gathered in memory to be sewn onto their owner afterward,
		// since the owner is not part of that query.
		ThreadLocalRelationStorage<SRCID, TRGT> targetEntityHolder = new ThreadLocalRelationStorage<>();
		String targetJoinName = associationRecordLoader.getEntityJoinTree().addRelationJoin(
				ROOT_JOIN_NAME,
				new EntityMappingAdapter<>(targetPersister.getMapping()),
				(PropertyAccessPoint) relation.getAccessor(),
				join.getRightAssociationKey(),
				join.getRightKey(),
				null,
				OUTER,
				(BeanRelationFixer<AssociationRecord, TRGT>) (record, target) -> targetEntityHolder.storeRelation((SRCID) record.getLeft(), target),
				Collections.emptySet(),
				null);
		
		sourcePersister.addSelectListener(new SecondPhaseSelectListener<>(
				sourcePersister.getMapping()::getId,
				associationRecordLoader,
				targetEntityHolder,
				(srcId, src) -> {
					// targets can be null if there's no associated entity in the database
					Collection<TRGT> targets = targetEntityHolder.giveRelatedEntities(srcId);
					if (targets != null) {
						targets.forEach(target -> relation.getRelationFixer().apply(src, target));
					}
				}));
		
		// Note that because the relation is loaded separately, next joins should be appended to the loader's join tree,
		// not the aggregate one, so that the target entity relations are loaded by the second-phase query too
		return new GraftPoint(relation.getTargetEntity(), targetPersister, targetJoinName, associationRecordLoader.getEntityJoinTree());
	}
}
