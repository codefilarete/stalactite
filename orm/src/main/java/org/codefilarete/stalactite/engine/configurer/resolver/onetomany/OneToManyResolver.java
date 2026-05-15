package org.codefilarete.stalactite.engine.configurer.resolver.onetomany;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codefilarete.stalactite.engine.configurer.AssociationRecordMapping;
import org.codefilarete.stalactite.engine.configurer.IndexedAssociationRecordMapping;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.IntermediaryRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.SkeletonAggregateResolver;
import org.codefilarete.stalactite.engine.runtime.AssociationRecord;
import org.codefilarete.stalactite.engine.runtime.AssociationRecordPersister;
import org.codefilarete.stalactite.engine.runtime.AssociationTable;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationRecord;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationTable;
import org.codefilarete.stalactite.engine.runtime.onetomany.AbstractOneToManyEngine;
import org.codefilarete.stalactite.engine.runtime.onetomany.IndexedAssociationTableManyRelationDescriptor;
import org.codefilarete.stalactite.engine.runtime.onetomany.IndexedMappedManyRelationDescriptor;
import org.codefilarete.stalactite.engine.runtime.onetomany.ManyRelationDescriptor;
import org.codefilarete.stalactite.engine.runtime.onetomany.MappedManyRelationDescriptor;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithAssociationTableEngine;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithIndexedAssociationTableEngine;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithIndexedMappedAssociationEngine;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithMappedAssociationEngine;
import org.codefilarete.stalactite.mapping.id.assembly.IdentifierAssembler;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Maps;

import static org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL;
import static org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode.ASSOCIATION_ONLY;
import static org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode.READ_ONLY;

public class OneToManyResolver {
	
	private final SkeletonAggregateResolver skeletonAggregateResolver;
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	
	public OneToManyResolver(SkeletonAggregateResolver skeletonAggregateResolver, Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.skeletonAggregateResolver = skeletonAggregateResolver;
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
	}
	
	/**
	 * Appends the direct one-to-many relations to given {@link ConfiguredRelationalPersister}
	 * @param resolvedRelation the entity to collect one-to-manys from
	 * @param aggregatePersister the persister to append one-to-many relations to
	 * @param createdPersisterConsumer a consumer that processes the resolved one-to-many relationship along with the configured persister
	 *                                  for the target entity after it has been created.
	 * @param <SRC> type of the source entity
	 * @param <SRCID> type of the source entity identifier
	 * @param <TRGT> type of the target entity
	 * @param <TRGTID> type of the target entity identifier
	 * @param <S> type of the collection of target entities
	 * @param <LEFTTABLE> type of the source entity table
	 * @param <RIGHTTABLE> type of the target entity table
	 * @param <JOINID> type of the join table identifier
	 */
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID>
	void resolve(ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> resolvedRelation,
	             ConfiguredRelationalPersister<SRC, SRCID> aggregatePersister,
	             Consumer<ConfiguredRelationalPersister<TRGT, TRGTID>> createdPersisterConsumer) {
		ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister = skeletonAggregateResolver.buildPersister(resolvedRelation.getTargetEntity());
		createdPersisterConsumer.accept(targetPersister);
		
		AbstractOneToManyEngine<SRC, TRGT, SRCID, TRGTID, S> oneToManyEngine;
		if (resolvedRelation.isOwnedByReverseSide()) {
			
			DirectRelationJoin<LEFTTABLE, RIGHTTABLE, SRCID> join = (DirectRelationJoin<LEFTTABLE, RIGHTTABLE, SRCID>) resolvedRelation.getJoin();
			KeyMapping<LEFTTABLE, RIGHTTABLE, SRCID> foreignKeyColumnsMapping = join.getLeftKey().reference(join.getRightKey());
			
			Function<SRCID, Map<Column<RIGHTTABLE, ?>, ?>> reverseColumnsValueProvider;
			reverseColumnsValueProvider = srcid -> {
				IdentifierAssembler<SRCID, LEFTTABLE> identifierAssembler = aggregatePersister.getMapping().getIdMapping().getIdentifierAssembler();
				Map<Column<LEFTTABLE, ?>, ?> columnValues = identifierAssembler.getColumnValues(srcid);
				return Maps.innerJoin(foreignKeyColumnsMapping.getMapping(), columnValues);
			};
			Set<Column<RIGHTTABLE, ?>> reverseColumns = join.getRightKey().getColumns();
			
			if (resolvedRelation.isOrdered()) {
				IndexedMappedManyRelationDescriptor<SRC, TRGT, S, SRCID, TRGTID, RIGHTTABLE> manyRelationDescriptor = new IndexedMappedManyRelationDescriptor<>(
						resolvedRelation.getAccessor(),
						resolvedRelation.getComponentFactory(),
						resolvedRelation.getMappedByAccessor(),
						join.getRightKey(),
						resolvedRelation.getIndexingMappedColumn(),
						aggregatePersister.getMapping()::getId,
						targetPersister.getMapping()::getId,
						resolvedRelation.getRelationMode() == ASSOCIATION_ONLY,
						resolvedRelation.getRelationMode() == ALL_ORPHAN_REMOVAL);
				
				oneToManyEngine = new OneToManyWithIndexedMappedAssociationEngine<SRC, TRGT, SRCID, TRGTID, S, LEFTTABLE, RIGHTTABLE>(
						targetPersister,
						manyRelationDescriptor,
						aggregatePersister,
						reverseColumns,
						reverseColumnsValueProvider);
			} else {
				MappedManyRelationDescriptor<SRC, TRGT, S, SRCID, RIGHTTABLE> manyRelationDescriptor = new MappedManyRelationDescriptor<>(
						resolvedRelation.getAccessor(),
						resolvedRelation.getComponentFactory(),
						resolvedRelation.getMappedByAccessor(),
						join.getRightKey(),
						resolvedRelation.getRelationMode() == ASSOCIATION_ONLY,
						resolvedRelation.getRelationMode() == ALL_ORPHAN_REMOVAL);
				
				oneToManyEngine = new OneToManyWithMappedAssociationEngine<>(
						targetPersister,
						manyRelationDescriptor,
						aggregatePersister,
						reverseColumns,
						reverseColumnsValueProvider);
			}
		} else {
			if (resolvedRelation.isOrdered()) {
				oneToManyEngine = buildIndexedAssociationTableEngine(aggregatePersister, resolvedRelation, targetPersister);
			} else {
				oneToManyEngine = buildAssociationTableEngine(aggregatePersister, resolvedRelation, targetPersister);
			}
		}
		
		boolean writeAuthorized = resolvedRelation.getRelationMode() != READ_ONLY;
		if (writeAuthorized) {
			oneToManyEngine.addInsertCascade(targetPersister);
			oneToManyEngine.addUpdateCascade(targetPersister);
			oneToManyEngine.addDeleteCascade(targetPersister);
		}
	}
	
	private <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>,
			LEFTTABLE extends Table<LEFTTABLE>,
			RIGHTTABLE extends Table<RIGHTTABLE>,
			ASSOCIATIONTABLE extends AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	AbstractOneToManyEngine<SRC, TRGT, SRCID, TRGTID, S>
	buildAssociationTableEngine(ConfiguredRelationalPersister<SRC, SRCID> result, ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> resolvedRelation, ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
		
		IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join = (IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID>) resolvedRelation.getJoin();
		ManyRelationDescriptor<SRC, TRGT, S> manyRelationDescriptor = new ManyRelationDescriptor<>(
				resolvedRelation.getAccessor(),
				resolvedRelation.getComponentFactory(),
				resolvedRelation.getMappedByAccessor(),
				resolvedRelation.getRelationMode() == ASSOCIATION_ONLY,
				resolvedRelation.getRelationMode() == ALL_ORPHAN_REMOVAL);
		
		
		AssociationRecordPersister<AssociationRecord, ASSOCIATIONTABLE> associationPersister = new AssociationRecordPersister<>(
				new AssociationRecordMapping<>(
						join.getJoinTable(),
						result.getMapping().getIdMapping().getIdentifierAssembler(),
						targetPersister.getMapping().getIdMapping().getIdentifierAssembler()),
				dialect,
				connectionConfiguration);
		
		return new OneToManyWithAssociationTableEngine<>(
				result,
				targetPersister,
				manyRelationDescriptor,
				associationPersister,
				dialect.getWriteOperationFactory(),
				dialect);
	}
	
	private <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>,
			LEFTTABLE extends Table<LEFTTABLE>,
			RIGHTTABLE extends Table<RIGHTTABLE>,
			ASSOCIATIONTABLE extends IndexedAssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	AbstractOneToManyEngine<SRC, TRGT, SRCID, TRGTID, S> buildIndexedAssociationTableEngine(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
	                                                                                        ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> resolvedRelation,
	                                                                                        ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
		
		IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join = (IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID>) resolvedRelation.getJoin();
		IndexedAssociationTableManyRelationDescriptor<SRC, TRGT, S, SRCID> manyRelationDescriptor = new IndexedAssociationTableManyRelationDescriptor<>(
				resolvedRelation.getAccessor(),
				resolvedRelation.getComponentFactory(),
				resolvedRelation.getMappedByAccessor(),
				sourcePersister.getMapping()::getId,
				resolvedRelation.getRelationMode() == ASSOCIATION_ONLY,
				resolvedRelation.getRelationMode() == ALL_ORPHAN_REMOVAL);
		
		AssociationRecordPersister<IndexedAssociationRecord, ASSOCIATIONTABLE> indexedAssociationPersister =
				new AssociationRecordPersister<>(
						new IndexedAssociationRecordMapping<>(
								join.getJoinTable(),
								sourcePersister.getMapping().getIdMapping().getIdentifierAssembler(),
								targetPersister.getMapping().getIdMapping().getIdentifierAssembler(),
								join.getJoinTable().getLeftIdentifierColumnMapping(),
								join.getJoinTable().getRightIdentifierColumnMapping()),
						dialect,
						connectionConfiguration);
		
		return new OneToManyWithIndexedAssociationTableEngine<>(
				sourcePersister,
				targetPersister,
				manyRelationDescriptor,
				indexedAssociationPersister,
				resolvedRelation.getIndexingAssociationColumn(),
				dialect.getWriteOperationFactory(),
				dialect);
	}
}
