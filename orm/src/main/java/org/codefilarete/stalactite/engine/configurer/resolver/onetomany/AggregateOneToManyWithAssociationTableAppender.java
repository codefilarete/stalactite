package org.codefilarete.stalactite.engine.configurer.resolver.onetomany;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.reflection.PropertyAccessPoint;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.configurer.AssociationRecordMapping;
import org.codefilarete.stalactite.engine.configurer.model.IntermediaryRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.EntityReader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.AssociationTableLoader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.ThreadLocalRelationStorage;
import org.codefilarete.stalactite.engine.listener.SelectListener;
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
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.collection.Iterables;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

/**
 * Wires a one-to-many relation owned by an association table onto the aggregate's {@link EntityJoinTree}.
 * <p>
 * Two strategies are available, depending on {@link ResolvedOneToManyRelation#isFetchSeparately()}:
 * <ul>
 * <li>the joined one: the association table and the target table are joined onto the aggregate's tree, hence
 * everything is loaded by the very same query</li>
 * <li>the fetch-separately one (2-phase load): the aggregate's tree is left untouched, so the main query doesn't
 * return the cartesian product of all the aggregate relations. Instead, a dedicated {@link AssociationTableLoader},
 * which owns its own {@link EntityJoinTree} rooted on the association table, selects the association records from
 * the identifiers of the already-loaded owning entities. Target entities are then loaded from their own persister
 * and finally sewn onto their owner.</li>
 * </ul>
 * Note that the 2-phase load is not a lazy loading: no query is triggered in the background when accessing the
 * collection, everything is loaded eagerly so that the whole aggregate is available and coherent when returned.
 *
 * @author Guillaume Mary
 * @see AggregateOneToManyAppender
 * @see AssociationTableLoader
 */
public class AggregateOneToManyWithAssociationTableAppender {
	
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	GraftPoint append(ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                  ConfiguredEntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                  ConfiguredEntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                  String mountPoint,
	                  EntityJoinTree<SRC, SRCID> aggregateTree,
	                  Dialect dialect,
	                  ConnectionProvider connectionProvider) {
		
		// Preparing for next iteration
		// Note that we can't set the correct generics types to the GraftPoint instance
		// because we go a step further in the relation by shifting the types from SRC to TRGT
		return appendAssociation(sourcePersister, targetPersister, relation, relation.getAccessor(), aggregateTree, mountPoint, dialect, connectionProvider);
	}
	
	private <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, ASSOCIATIONTABLE extends AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	GraftPoint appendAssociation(ConfiguredEntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                             ConfiguredEntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                             ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                             PropertyAccessor<SRC, S> accessor,
	                             EntityJoinTree<SRC, SRCID> entityJoinTree,
	                             String mountPoint,
	                             Dialect dialect,
	                             ConnectionProvider connectionProvider) {
		Function<ColumnedRow, Object> duplicateIdentifierProvider = null;
		
		IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join = (IntermediaryRelationJoin) relation.getJoin();
		if (relation.isFetchSeparately()) {
			// Note that the aggregate tree is not modified at all: the association table is the root of a dedicated
			// tree owned by the loader below, which prevents the main query from returning too many rows
			return appendSeparatelyFetchedAssociation(sourcePersister, targetPersister, relation, join, dialect, connectionProvider);
		} else {
			// we join on the association table
			String associationTableJoinName = entityJoinTree.addPassiveJoin(
					mountPoint,
					join.getLeftKey(),
					join.getLeftAssociationKey(),
					OUTER,
					Collections.emptySet());
			String manyJoinName = entityJoinTree.addRelationJoin(
					associationTableJoinName,
					new EntityMappingAdapter<>(targetPersister.<RIGHTTABLE>getMapping()),
					accessor,
					join.getRightAssociationKey(),
					join.getRightKey(),
					null,
					OUTER,
					relation.getRelationFixer(),
					Collections.emptySet(),
					duplicateIdentifierProvider);
			return new GraftPoint(relation.getTargetEntity(), targetPersister, manyJoinName, entityJoinTree);
		}
	}
	
	/**
	 * Registers the second phase of the load onto the source persister: once the owning entities are loaded, a single
	 * query, built from the {@link EntityJoinTree} of a dedicated {@link AssociationTableLoader}, selects the
	 * association records joined with their target entities from the identifiers of the owning entities. Target
	 * entities are gathered in memory while the result set is read, then sewn onto their owner.
	 *
	 * @return the graft point of the target entity : the loader's tree, so that the relations of the target entity are
	 * loaded by that very same second-phase query
	 */
	private <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, ASSOCIATIONTABLE extends AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	GraftPoint appendSeparatelyFetchedAssociation(ConfiguredEntityReader<SRC, SRCID, LEFTTABLE> sourcePersister,
	                                              ConfiguredEntityReader<TRGT, TRGTID, RIGHTTABLE> targetPersister,
	                                              ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                                              IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join,
	                                              Dialect dialect,
	                                              ConnectionProvider connectionProvider) {
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
		
		sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
			
			@Override
			public void afterSelect(Set<? extends SRC> result) {
				try {
					targetEntityHolder.init();
					
					Map<SRCID, ? extends SRC> sourcePerId = Iterables.map(result, sourcePersister.getMapping()::getId);
					
					// loading the association records: target entities are collected in memory by the join relation fixer
					associationRecordLoader.select(sourcePerId.keySet());
					
					// we sew the relations
					sourcePerId.forEach((srcId, src) -> {
						Collection<TRGT> targets = targetEntityHolder.giveRelatedEntities(srcId);
						if (targets != null) {
							targets.forEach(target -> relation.getRelationFixer().apply(src, target));
						}
					});
				} finally {
					// we remove the internal ThreadLocal
					targetEntityHolder.clear();
				}
			}
		});
		
		// Note that because the relation is loaded separately, next joins should be appended to the loader's join tree,
		// not the given as argument one, so that the target entity relations are loaded by the second-phase query too
		return new GraftPoint(relation.getTargetEntity(), targetPersister, targetJoinName, associationRecordLoader.getEntityJoinTree());
	}
}
