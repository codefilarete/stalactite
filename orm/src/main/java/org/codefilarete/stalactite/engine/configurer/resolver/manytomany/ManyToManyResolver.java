package org.codefilarete.stalactite.engine.configurer.resolver.manytomany;

import java.util.Collection;
import java.util.function.Consumer;

import org.codefilarete.stalactite.engine.configurer.AssociationRecordMapping;
import org.codefilarete.stalactite.engine.configurer.IndexedAssociationRecordMapping;
import org.codefilarete.stalactite.engine.configurer.model.IntermediaryRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.SkeletonAggregateResolver;
import org.codefilarete.stalactite.engine.runtime.AssociationRecord;
import org.codefilarete.stalactite.engine.runtime.AssociationRecordPersister;
import org.codefilarete.stalactite.engine.runtime.AssociationTable;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationRecord;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationTable;
import org.codefilarete.stalactite.engine.runtime.onetomany.AbstractOneToManyEngine;
import org.codefilarete.stalactite.engine.runtime.onetomany.IndexedAssociationTableManyRelationDescriptor;
import org.codefilarete.stalactite.engine.runtime.onetomany.ManyRelationDescriptor;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithAssociationTableEngine;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithIndexedAssociationTableEngine;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

import static org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL;
import static org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode.ASSOCIATION_ONLY;
import static org.codefilarete.stalactite.dsl.property.CascadeOptions.RelationMode.READ_ONLY;

/**
 * Handles write-cascade wiring for a resolved {@link ResolvedManyToManyRelation}.
 * Many-to-many relations always use an intermediary association table; there is no "owned by reverse side" variant.
 * Two sub-paths are supported:
 * <ul>
 *   <li>Non-indexed: {@link AssociationTable} + {@link OneToManyWithAssociationTableEngine}</li>
 *   <li>Indexed: {@link IndexedAssociationTable} + {@link OneToManyWithIndexedAssociationTableEngine}</li>
 * </ul>
 * The SELECT join-tree wiring is handled separately by {@link AggregateManyToManyAppender}.
 *
 * @author Guillaume Mary
 */
public class ManyToManyResolver {
	
	private final SkeletonAggregateResolver skeletonAggregateResolver;
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	
	public ManyToManyResolver(SkeletonAggregateResolver skeletonAggregateResolver,
	                          Dialect dialect,
	                          ConnectionConfiguration connectionConfiguration) {
		this.skeletonAggregateResolver = skeletonAggregateResolver;
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
	}
	
	/**
	 * Resolves the given many-to-many relation by building a persister for the target entity and wiring write cascades
	 * (INSERT / UPDATE / DELETE) onto the source persister via the appropriate association-table engine.
	 *
	 * @param resolvedRelation      the resolved model relation carrying the join structure and cascade options
	 * @param sourcePersister       the persister that owns the collection
	 * @param createdPersisterConsumer a consumer that receives the freshly built target persister
	 */
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>,
			LEFTTABLE extends Table<LEFTTABLE>,
			RIGHTTABLE extends Table<RIGHTTABLE>>
	void resolve(ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> resolvedRelation,
	             ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
	             Consumer<ConfiguredRelationalPersister<TRGT, TRGTID>> createdPersisterConsumer) {
		
		ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister =
				skeletonAggregateResolver.buildPersister(resolvedRelation.getTargetEntity());
		createdPersisterConsumer.accept(targetPersister);
		
		AbstractOneToManyEngine<SRC, TRGT, SRCID, TRGTID, S> engine;
		if (resolvedRelation.isOrdered()) {
			engine = buildIndexedAssociationTableEngine(sourcePersister, resolvedRelation, targetPersister);
		} else {
			engine = buildAssociationTableEngine(sourcePersister, resolvedRelation, targetPersister);
		}
		
		boolean writeAuthorized = resolvedRelation.getRelationMode() != READ_ONLY;
		if (writeAuthorized) {
			engine.addInsertCascade(targetPersister);
			engine.addUpdateCascade(targetPersister);
			engine.addDeleteCascade(targetPersister);
		}
	}
	
	/**
	 * Builds the engine for the non-indexed case.
	 * The {@link ManyRelationDescriptor} is built with the pre-assembled {@link org.codefilarete.stalactite.sql.result.BeanRelationFixer}
	 * from the model relation, which already handles optional bidirectionality.
	 */
	@SuppressWarnings("unchecked")
	private <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>,
			LEFTTABLE extends Table<LEFTTABLE>,
			RIGHTTABLE extends Table<RIGHTTABLE>,
			ASSOCIATIONTABLE extends AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	AbstractOneToManyEngine<SRC, TRGT, SRCID, TRGTID, S>
	buildAssociationTableEngine(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
	                            ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> resolvedRelation,
	                            ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
		
		IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join =
				(IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID>) resolvedRelation.getJoin();
		
		ManyRelationDescriptor<SRC, TRGT, S> manyRelationDescriptor = new ManyRelationDescriptor<>(
				resolvedRelation.getAccessor(),
				resolvedRelation.getComponentFactory(),
				null,   // bidirectionality is pre-baked in the relation fixer below
				resolvedRelation.getRelationFixer(),
				resolvedRelation.getRelationMode() == ASSOCIATION_ONLY,
				resolvedRelation.getRelationMode() == ALL_ORPHAN_REMOVAL);
		
		AssociationRecordPersister<AssociationRecord, ASSOCIATIONTABLE> associationPersister = new AssociationRecordPersister<>(
				new AssociationRecordMapping<>(
						join.getJoinTable(),
						sourcePersister.getMapping().getIdMapping().getIdentifierAssembler(),
						targetPersister.getMapping().getIdMapping().getIdentifierAssembler()),
				dialect,
				connectionConfiguration);
		
		return new OneToManyWithAssociationTableEngine<>(
				sourcePersister,
				targetPersister,
				manyRelationDescriptor,
				associationPersister,
				dialect.getWriteOperationFactory(),
				dialect);
	}
	
	/**
	 * Builds the engine for the indexed case (association table carries a positional index column).
	 */
	@SuppressWarnings("unchecked")
	private <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>,
			LEFTTABLE extends Table<LEFTTABLE>,
			RIGHTTABLE extends Table<RIGHTTABLE>,
			ASSOCIATIONTABLE extends IndexedAssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	AbstractOneToManyEngine<SRC, TRGT, SRCID, TRGTID, S>
	buildIndexedAssociationTableEngine(ConfiguredRelationalPersister<SRC, SRCID> sourcePersister,
	                                   ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> resolvedRelation,
	                                   ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister) {
		
		IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join =
				(IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID>) resolvedRelation.getJoin();
		
		IndexedAssociationTableManyRelationDescriptor<SRC, TRGT, S, SRCID> manyRelationDescriptor =
				new IndexedAssociationTableManyRelationDescriptor<>(
						resolvedRelation.getAccessor(),
						resolvedRelation.getComponentFactory(),
						null,   // bidirectionality is pre-baked in the relation fixer
						sourcePersister.getMapping()::getId,
						resolvedRelation.getRelationMode() == ASSOCIATION_ONLY,
						resolvedRelation.getRelationMode() == ALL_ORPHAN_REMOVAL);
		
		Column<ASSOCIATIONTABLE, Integer> indexColumn = resolvedRelation.getIndexingAssociationColumn();
		
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
				indexColumn,
				dialect.getWriteOperationFactory(),
				dialect);
	}
}
