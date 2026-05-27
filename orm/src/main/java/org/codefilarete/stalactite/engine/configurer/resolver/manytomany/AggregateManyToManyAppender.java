package org.codefilarete.stalactite.engine.configurer.resolver.manytomany;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.engine.configurer.IndexedAssociationRecordMapping;
import org.codefilarete.stalactite.engine.configurer.model.IntermediaryRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedManyToManyRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.AggregateResolver.AssemblyPoint;
import org.codefilarete.stalactite.engine.configurer.resolver.SkeletonAggregateResolver;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.runtime.AssociationTable;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationRecord;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationTable;
import org.codefilarete.stalactite.engine.runtime.load.EntityInflater;
import org.codefilarete.stalactite.engine.runtime.load.EntityTreeInflater;
import org.codefilarete.stalactite.engine.runtime.load.JoinNode;
import org.codefilarete.stalactite.engine.runtime.onetomany.IndexedAssociationTableManyRelationDescriptor.InMemoryRelationHolder;
import org.codefilarete.stalactite.query.api.Fromable;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.function.Hanger.Holder;

import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.JoinType.OUTER;
import static org.codefilarete.stalactite.engine.runtime.load.EntityJoinTree.ROOT_JOIN_NAME;

/**
 * Handles SELECT-path join-tree wiring for a {@link ResolvedManyToManyRelation}.
 * Write cascades are delegated to {@link ManyToManyResolver}.
 *
 * <p>Since many-to-many relations always use an intermediary association table, two join segments are always needed:
 * <ol>
 *   <li>A <em>passive join</em> from the source table to the association table (to filter rows).</li>
 *   <li>A <em>relation join</em> from the association table to the target table (to hydrate target beans).</li>
 * </ol>
 *
 * <p>When the association table carries an index column ({@link IndexedAssociationTable}), an
 * {@link InMemoryRelationHolder} is used instead of the plain relation fixer so that collection order is preserved.
 *
 * @author Guillaume Mary
 */
public class AggregateManyToManyAppender {
	
	private final ManyToManyResolver manyToManyResolver;
	
	public AggregateManyToManyAppender(SkeletonAggregateResolver skeletonAggregateResolver,
	                                   Dialect dialect,
	                                   ConnectionConfiguration connectionConfiguration) {
		this.manyToManyResolver = new ManyToManyResolver(skeletonAggregateResolver, dialect, connectionConfiguration);
	}
	
	/**
	 * Appends the given many-to-many relation to the aggregate persister by:
	 * <ol>
	 *   <li>Delegating write-cascade setup to {@link ManyToManyResolver}.</li>
	 *   <li>Adding the two necessary join segments to the root persister's join tree.</li>
	 *   <li>Forwarding SELECT lifecycle events from the source persister to the target persister.</li>
	 * </ol>
	 *
	 * @return an {@link AssemblyPoint} for the target entity, ready to be pushed onto the assembly queue
	 *         so that deeper relations are also resolved
	 */
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>,
			LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>>
	AssemblyPoint<?, ?, ?, ?> append(ConfiguredRelationalPersister<SRC, SRCID> rootPersister,
	                                 ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                                 AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE> assemblyPawn) {
		
		Holder<AssemblyPoint> resultHolder = new Holder<>();
		manyToManyResolver.resolve(
				relation,
				assemblyPawn.getRelationOwnerPersister(),
				targetPersister -> {
					
					PropertyAccessor<SRC, S> accessor;
					if (assemblyPawn.getParentJoinPoint().equals(ROOT_JOIN_NAME)) {
						accessor = relation.getAccessor();
					} else {
						AccessorChain<SRC, S> shifter = new AccessorChain<>(assemblyPawn.getAccessor(), relation.getAccessor());
						shifter.setNullValueHandler(AccessorChain.RETURN_NULL);
						accessor = shifter;
					}
					
					@SuppressWarnings("unchecked")
					IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ?, SRCID, TRGTID> join =
							(IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ?, SRCID, TRGTID>) relation.getJoin();
					
					String manyJoinName;
					if (relation.isOrdered()) {
						manyJoinName = appendIndexedAssociation(rootPersister, targetPersister, relation, assemblyPawn, join, accessor);
					} else {
						manyJoinName = appendAssociation(rootPersister, targetPersister, relation, assemblyPawn, join, accessor);
					}
					
					// Forward SELECT lifecycle events from the source entity's persister down to the target persister
					SelectListener<TRGT, TRGTID> targetSelectListener = targetPersister.getPersisterListener().getSelectListener();
					assemblyPawn.getRelationOwnerPersister().addSelectListener(new SelectListener<SRC, SRCID>() {
						@Override
						public void beforeSelect(Iterable<SRCID> ids) {
							targetSelectListener.beforeSelect(Collections.emptyList());
						}
						
						@Override
						public void afterSelect(Set<? extends SRC> result) {
							Set<TRGT> targets = Nullable.nullable(result)
									.map(r -> r.stream()
											.flatMap(src -> Nullable.nullable(relation.getAccessor().get(src))
													.map(Collection::stream)
													.getOr(Stream.empty()))
											.collect(Collectors.toSet()))
									.getOr(Collections.emptySet());
							targetSelectListener.afterSelect(targets);
						}
						
						@Override
						public void onSelectError(Iterable<SRCID> ids, RuntimeException exception) {
							targetSelectListener.onSelectError(Collections.emptyList(), exception);
						}
					});
					
					// Preparing for next iteration
					// Note that we can't set the correct generics types to the AssemblyPoint instance
					// because we go a step further in the relation by shifting the types from SRC to TRGT
					resultHolder.set(new AssemblyPoint(relation.getTargetEntity(), targetPersister, manyJoinName, accessor));
				});
		
		return resultHolder.get();
	}
	
	/**
	 * Adds two join segments for the non-indexed association-table case:
	 * <ol>
	 *   <li>Passive join from source table to the association table.</li>
	 *   <li>Relation join from the association table to the target table, using the pre-built
	 *       {@link org.codefilarete.stalactite.sql.result.BeanRelationFixer} (which encodes optional
	 *       bidirectionality).</li>
	 * </ol>
	 */
	private <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>,
			LEFTTABLE extends Table<LEFTTABLE>,
			RIGHTTABLE extends Table<RIGHTTABLE>,
			ASSOCIATIONTABLE extends AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	String appendAssociation(ConfiguredRelationalPersister<SRC, SRCID> rootPersister,
	                         ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
	                         ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                         AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE> assemblyPawn,
	                         IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ?, SRCID, TRGTID> rawJoin,
	                         PropertyAccessor<SRC, S> accessor) {
		
		IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join =
				(IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID>) rawJoin;
		
		String associationTableJoinName = rootPersister.getEntityJoinTree().addPassiveJoin(
				assemblyPawn.getParentJoinPoint(),
				join.getLeftKey(),
				join.getLeftAssociationKey(),
				OUTER,
				Collections.emptySet());
		
		return rootPersister.getEntityJoinTree().addRelationJoin(
				associationTableJoinName,
				new EntityInflater.EntityMappingAdapter<>(targetPersister.<RIGHTTABLE>getMapping()),
				accessor,
				join.getRightAssociationKey(),
				join.getRightKey(),
				null,
				OUTER,
				relation.getRelationFixer(),   // pre-built fixer handles bidirectionality if configured
				Collections.emptySet(),
				null);
	}
	
	/**
	 * Adds two join segments for the indexed association-table case.
	 * An {@link InMemoryRelationHolder} is used as the relation fixer so that collection ordering is
	 * restored in-memory after the flat SQL result is accumulated.
	 */
	@SuppressWarnings("unchecked")
	private <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>,
			LEFTTABLE extends Table<LEFTTABLE>,
			RIGHTTABLE extends Table<RIGHTTABLE>,
			ASSOCIATIONTABLE extends IndexedAssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>>
	String appendIndexedAssociation(ConfiguredRelationalPersister<SRC, SRCID> rootPersister,
	                                ConfiguredRelationalPersister<TRGT, TRGTID> targetPersister,
	                                ResolvedManyToManyRelation<SRC, TRGT, S, SRCID, TRGTID, LEFTTABLE, RIGHTTABLE> relation,
	                                AssemblyPoint<SRC, SRCID, TRGT, LEFTTABLE> assemblyPawn,
	                                IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ?, SRCID, TRGTID> rawJoin,
	                                PropertyAccessor<SRC, S> accessor) {
		
		IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID> join =
				(IntermediaryRelationJoin<LEFTTABLE, RIGHTTABLE, ASSOCIATIONTABLE, SRCID, TRGTID>) rawJoin;
		Column<ASSOCIATIONTABLE, Integer> indexingColumn = join.getJoinTable().getIndexColumn();
		
		Holder<String> associationTableJoinNodeNameHolder = new Holder<>();
		Function<ColumnedRow, Object> duplicateIdentifierProvider = columnedRow -> {
			TRGTID identifier = targetPersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(columnedRow);
			JoinNode<IndexedAssociationRecord, Fromable> joinNode =
					(JoinNode<IndexedAssociationRecord, Fromable>) rootPersister.getEntityJoinTree()
							.getJoin(associationTableJoinNodeNameHolder.get());
			ColumnedRow rowDecoder = EntityTreeInflater.currentContext().getDecoder(joinNode);
			Integer targetEntityIndex = rowDecoder.get(indexingColumn);
			return identifier + "-" + targetEntityIndex;
		};
		
		// Passive join: source table → association table (include all columns for index decoding)
		String associationTableJoinName = rootPersister.getEntityJoinTree().addPassiveJoin(
				assemblyPawn.getParentJoinPoint(),
				join.getLeftKey(),
				join.getLeftAssociationKey(),
				OUTER,
				join.getJoinTable().getColumns());
		associationTableJoinNodeNameHolder.set(associationTableJoinName);
		
		// The InMemoryRelationHolder buffers entities with their index and applies the sorted order after select
		// Note: getMappedByAccessor() is null in model.ManyToManyRelation; bidirectionality on the read path is a
		// known limitation for indexed M2M (consistent with indexed OneToMany with association table).
		InMemoryRelationHolder<SRC, SRCID, TRGT, S> inMemoryRelationFixer = new InMemoryRelationHolder<>(
				assemblyPawn.getRelationOwnerPersister()::getId,
				relation.getAccessor(),
				relation.getComponentFactory(),
				null);
		
		rootPersister.addSelectListener(new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
				inMemoryRelationFixer.init();
			}
			
			@Override
			public void afterSelect(Set<? extends SRC> result) {
				inMemoryRelationFixer.applySort(result);
				inMemoryRelationFixer.clear();
			}
			
			@Override
			public void onSelectError(Iterable<SRCID> ids, RuntimeException exception) {
				inMemoryRelationFixer.clear();
			}
		});
		
		// Relation join: association table → target table
		String manyJoinName = rootPersister.getEntityJoinTree().addRelationJoin(
				associationTableJoinName,
				new EntityInflater.EntityMappingAdapter<>(targetPersister.<RIGHTTABLE>getMapping()),
				accessor,
				join.getRightAssociationKey(),
				join.getRightKey(),
				null,
				OUTER,
				inMemoryRelationFixer,
				Collections.emptySet(),
				duplicateIdentifierProvider);
		
		// Attach a consumption listener: each time a target row is consumed, record its position from the association table row
		JoinNode<TRGT, Fromable> joinNode = (JoinNode<TRGT, Fromable>) rootPersister.getEntityJoinTree().getJoin(manyJoinName);
		JoinNode<?, Fromable> associationJoinNode = rootPersister.getEntityJoinTree().getJoin(associationTableJoinName);
		IndexedAssociationRecordMapping<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID> associationRecordMapping =
				new IndexedAssociationRecordMapping<>(
						join.getJoinTable(),
						assemblyPawn.getRelationOwnerPersister().getMapping().getIdMapping().getIdentifierAssembler(),
						targetPersister.getMapping().getIdMapping().getIdentifierAssembler(),
						join.getJoinTable().getLeftIdentifierColumnMapping(),
						join.getJoinTable().getRightIdentifierColumnMapping());
		joinNode.setConsumptionListener((trgt, columnValueProvider) -> {
			ColumnedRow rowDecoder = EntityTreeInflater.currentContext().getDecoder(associationJoinNode);
			IndexedAssociationRecord associationRecord = associationRecordMapping.getRowTransformer().transform(rowDecoder);
			inMemoryRelationFixer.addIndex((SRCID) associationRecord.getLeft(), trgt, associationRecord.getIndex());
		});
		
		return manyJoinName;
	}
}
