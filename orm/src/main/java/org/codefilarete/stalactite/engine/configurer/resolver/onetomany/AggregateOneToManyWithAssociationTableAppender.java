package org.codefilarete.stalactite.engine.configurer.resolver.onetomany;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.configurer.AssociationRecordMapping;
import org.codefilarete.stalactite.engine.configurer.model.EntityPolymorphism;
import org.codefilarete.stalactite.engine.configurer.model.IntermediaryRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.PolymorphicEntity;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.GraftPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.GraftableRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.polymorphism.PolymorphicSkeletonAppender;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.AssociationTableLoader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.FirstPhaseRelationLoader;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.SecondPhaseRelationSelectListener;
import org.codefilarete.stalactite.engine.configurer.resolver.separatefetch.ThreadLocalRelationStorage;
import org.codefilarete.stalactite.engine.runtime.AssociationRecord;
import org.codefilarete.stalactite.engine.runtime.AssociationTable;
import org.codefilarete.stalactite.engine.runtime.ConfiguredEntityReader;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater.EntityMappingAdapter;
import org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree;
import org.codefilarete.stalactite.query.api.QualifiedSelectable;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

/**
 * Wires a one-to-many relation owned by an association table onto the aggregate's {@link EntityJoinTree}.
 * <p>
 * Two strategies are available, depending on {@link ResolvedOneToManyRelation#isFetchedSeparately()}:
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
	
	private final PolymorphicSkeletonAppender polymorphicSkeletonAppender = new PolymorphicSkeletonAppender();
	
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, ASSOCIATIONTABLE extends AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	GraftPoint append(ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                  ConfiguredEntityReader<SRC, SRCID, LEFTTABLE> sourceReader,
	                  ConfiguredEntityReader<TRGT, TRGTID, RIGHTTABLE> targetReader,
	                  String mountPoint,
	                  EntityJoinTree<SRC, SRCID> aggregateTree,
	                  Dialect dialect,
	                  ConnectionProvider connectionProvider) {
		
		GraftPoint<TRGT, TRGTID, RIGHTTABLE, SRC, SRCID> result;
		
		IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join = (IntermediaryRelationJoin) relation.getJoin();
		if (relation.isFetchedSeparately()) {
			// Note that the aggregate tree is not modified at all: the association table is the root of a dedicated
			// tree owned by the loader below, which prevents the main query from returning too many rows
			result = appendWithSeparateFetch(sourceReader, targetReader, relation, join, mountPoint, aggregateTree, dialect, connectionProvider);
		} else {
			if (relation.getTargetEntity() instanceof PolymorphicEntity) {
				EntityPolymorphism<TRGT, TRGTID> polymorphism = ((PolymorphicEntity<TRGT, TRGTID, RIGHTTABLE>) relation.getTargetEntity()).getPolymorphism();
				
				GraftableRelation<SRC, TRGT, TRGTID, ASSOCIATIONTABLE, RIGHTTABLE, TRGTID> graftableRelation = GraftableRelation.forOneToManyRelationWithAssociationTable(relation);
				String associationTableJoinName = aggregateTree.addPassiveJoin(
						mountPoint,
						join.getLeftKey(),
						join.getLeftAssociationKey(),
						OUTER,
						Collections.emptySet());
				result = polymorphicSkeletonAppender.append(
						aggregateTree,
						polymorphism,
						targetReader,
						graftableRelation,
						associationTableJoinName);
			} else {
				// we join on the association table
				String associationTableJoinName = aggregateTree.addPassiveJoin(
						mountPoint,
						join.getLeftKey(),
						join.getLeftAssociationKey(),
						OUTER,
						Collections.emptySet());
				String manyJoinName = aggregateTree.addRelationJoin(
						associationTableJoinName,
						new EntityMappingAdapter<>(targetReader.getMapping()),
						relation.getAccessor(),
						join.getRightAssociationKey(),
						join.getRightKey(),
						null,
						OUTER,
						relation.getRelationFixer(),
						Collections.emptySet(),
						null);
				result = new GraftPoint(relation.getTargetEntity(), targetReader, manyJoinName, aggregateTree);
			}
		}
		
		return result;
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
	GraftPoint appendWithSeparateFetch(ConfiguredEntityReader<SRC, SRCID, LEFTTABLE> sourceReader,
									   ConfiguredEntityReader<TRGT, TRGTID, RIGHTTABLE> targetReader,
									   ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
									   IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join,
									   String mountPoint,
									   EntityJoinTree<SRC, SRCID> aggregateTree,
									   Dialect dialect,
									   ConnectionProvider connectionProvider) {
		ASSOCIATIONTABLE associationTable = join.getJoinTable();
		AssociationRecordMapping<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID> associationRecordMapping = new AssociationRecordMapping<>(
				associationTable,
				sourceReader.getMapping().getIdMapping().getIdentifierAssembler(),
				targetReader.getMapping().getIdMapping().getIdentifierAssembler());
		
		Map<QualifiedSelectable<LEFTTABLE, ?>, QualifiedSelectable<ASSOCIATIONTABLE, ?>> sourcePkToAssociationTableKey =
				new KeyMapping<>(join.getLeftKey(), join.getLeftAssociationKey()).getMapping();
		
		AssociationTableLoader<AssociationRecord, AssociationRecord, SRC, SRCID, LEFTTABLE, ASSOCIATIONTABLE> associationRecordLoader =
				new AssociationTableLoader<>(
						sourceReader.getMapping().getIdMapping(),
						associationRecordMapping,
						sourcePkToAssociationTableKey,
						dialect,
						connectionProvider);
		
		// the target entity is joined onto the loader's tree so that association records and target entities are
		// loaded by one and only one query. Entities are gathered in memory to be sewn onto their owner afterward,
		// since the owner is not part of that query.
		if (relation.getTargetEntity() instanceof PolymorphicEntity) {
			ThreadLocalRelationStorage<SRC, TRGTID> current2PhasesLoadContext = new ThreadLocalRelationStorage<>();
			
			EntityPolymorphism<TRGT, Object> polymorphism = ((PolymorphicEntity<TRGT, Object, RIGHTTABLE>) relation.getTargetEntity()).getPolymorphism();
			
			String associationTableJoinName = sourceReader.getEntityJoinTree().addPassiveJoin(
					mountPoint,
					join.getLeftKey(),
					join.getLeftAssociationKey(),
					OUTER,
					Collections.emptySet(),
					null);
			
			polymorphicSkeletonAppender.appendForSeparateLoad(
					sourceReader.getEntityJoinTree(),
					polymorphism,
					targetReader,
					associationTableJoinName,
					current2PhasesLoadContext,
					new KeyMapping<>(join.getRightAssociationKey(), join.getRightKey()));
			
			sourceReader.addSelectListener(new SecondPhaseRelationSelectListener<>(
					current2PhasesLoadContext,
					targetReader.getMapping()::getId,
					targetReader,
					relation.getRelationFixer()::apply
			));
		} else {
			ThreadLocalRelationStorage<SRC, TRGTID> current2PhasesLoadContext = new ThreadLocalRelationStorage<>();
			Function<ColumnedRow, TRGTID> associationTableRecordAssembler = row -> {
				AssociationRecord associationRecord = associationRecordMapping.getIdMapping().getIdentifierAssembler().assemble(row);
				return associationRecord == null ? null : (TRGTID) associationRecord.getRight();
			};
			// we get all the columns of the association table to have the target id columns and make the target assembler able to assemble its id
			Set<Column<ASSOCIATIONTABLE, ?>> associationTableColumns = join.getLeftAssociationKey().getTable().getColumns();
			FirstPhaseRelationLoader<SRC, TRGTID> firstPhaseCollector = new FirstPhaseRelationLoader<>(
					associationTableRecordAssembler,
					associationTableColumns,
					current2PhasesLoadContext);
			aggregateTree.addMergeJoin(mountPoint,
					firstPhaseCollector,
					join.getLeftKey(),
					join.getLeftAssociationKey(),
					OUTER);
			
			// adding second phase loader
			sourceReader.addSelectListener(new SecondPhaseRelationSelectListener<>(
					current2PhasesLoadContext,
					targetReader.getMapping()::getId,
					targetReader,
					relation.getRelationFixer()::apply
			));
		}
		
		// Note that because the relation is loaded separately, next joins should be appended to the loader's join tree,
		// not the given as argument one, so that the target entity relations are loaded by the second-phase query too
		return new GraftPoint(relation.getTargetEntity(), targetReader, ROOT_JOIN_NAME, associationRecordLoader.getEntityJoinTree());
	}
}
