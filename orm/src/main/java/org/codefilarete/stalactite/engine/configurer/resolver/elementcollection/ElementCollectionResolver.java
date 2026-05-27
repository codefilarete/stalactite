package org.codefilarete.stalactite.engine.configurer.resolver.elementcollection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.function.Function;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.stalactite.engine.EntityPersister;
import org.codefilarete.stalactite.engine.cascade.AfterInsertCollectionCascader;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementRecord;
import org.codefilarete.stalactite.engine.configurer.elementcollection.ElementRecordMapping;
import org.codefilarete.stalactite.engine.configurer.elementcollection.IndexedElementRecord;
import org.codefilarete.stalactite.engine.configurer.elementcollection.IndexedElementRecordMapping;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedElementCollectionRelation;
import org.codefilarete.stalactite.engine.runtime.CollectionUpdater;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithMappedAssociationEngine.AfterUpdateTrigger;
import org.codefilarete.stalactite.engine.runtime.onetomany.OneToManyWithMappedAssociationEngine.DeleteTargetEntitiesBeforeDeleteCascader;
import org.codefilarete.stalactite.mapping.DefaultEntityMapping;
import org.codefilarete.stalactite.mapping.EmbeddedClassMapping;
import org.codefilarete.stalactite.mapping.IdAccessor;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.KeyMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.ColumnedRow;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.collection.Iterables;

import static org.codefilarete.tool.Nullable.nullable;

public class ElementCollectionResolver {
	
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	
	public ElementCollectionResolver(Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
	}
	
	public <SRC, SRCID, TRGT, TRGTID, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, COLLECTIONTABLE extends Table<COLLECTIONTABLE>,
			ER extends ElementRecord<TRGT, SRCID>>
	ElementRecordPersister<TRGT, SRCID, COLLECTIONTABLE, ER> resolve(ResolvedElementCollectionRelation<SRC, TRGT, S, SRCID, LEFTTABLE, COLLECTIONTABLE, ER> resolvedRelation,
	             ConfiguredRelationalPersister<SRC, SRCID> sourcePersister) {
		
		ElementCollectionMapping<SRC, SRCID, TRGT, S, LEFTTABLE, COLLECTIONTABLE, ER> elementCollectionMapping = (ElementCollectionMapping<SRC, SRCID, TRGT, S, LEFTTABLE, COLLECTIONTABLE, ER>) buildCollectionMapping(resolvedRelation, sourcePersister);
		
		// Note that table will be added to schema thanks to select cascade because join is added to source persister
		ElementRecordPersister<TRGT, SRCID, COLLECTIONTABLE, ER> collectionPersister =
				new ElementRecordPersister<>(elementCollectionMapping.elementRecordMapping, dialect, connectionConfiguration);
		
		Accessor<SRC, Collection<ER>> collectionProviderForInsert = elementCollectionMapping.collectionProvider(resolvedRelation.getAccessor(), sourcePersister.getMapping(), false);
		sourcePersister.addInsertListener(new TargetInstancesInsertCascader<>(collectionPersister, collectionProviderForInsert));
		
		Mutator<Duo<SRC, SRC>, Boolean> collectionUpdater = new CollectionUpdater<SRC, ER, Collection<ER>>(
				elementCollectionMapping.collectionProvider(resolvedRelation.getAccessor(), sourcePersister.getMapping(), true),
				collectionPersister,
				(o, i) -> { /* no reverse setter because we store only raw values */ },
				true,
				// we base our id policy on a particular identifier because Id is all the same for ElementCollection (it is source bean id)
				ElementRecord::footprint) {
			
			/**
			 * Overridden to force insertion of added entities because as a difference with default behavior (parent class), collection elements are
			 * not entities, so they can't be moved from a collection to another, hence they don't need to be updated, therefore there's no need to
			 * use {@code getElementPersister().persist(..)} mechanism. Even more : it is counterproductive (meaning false) because
			 * {@code persist(..)} uses {@code update(..)} when entities are considered already persisted (not {@code isNew()}), which is always the
			 * case for new {@link ElementRecord}
			 */
			@Override
			protected void insertTargets(UpdateContext updateContext) {
				collectionPersister.insert(updateContext.getAddedElements());
			}
		};
		sourcePersister.addUpdateListener(new AfterUpdateTrigger<>(collectionUpdater));
		
		// delete management (we provide persisted instances so they are perceived as deletable)
		sourcePersister.addDeleteListener(new DeleteTargetEntitiesBeforeDeleteCascader<>(collectionPersister, elementCollectionMapping.collectionProvider(resolvedRelation.getAccessor(), sourcePersister.getMapping(), true)));
		
		return collectionPersister;
	}
	
	private <SRC, SRCID, TRGT, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, COLLECTIONTABLE extends Table<COLLECTIONTABLE>, ER extends ElementRecord<TRGT, SRCID>>
	ElementCollectionMapping<SRC, SRCID, TRGT, S, LEFTTABLE, COLLECTIONTABLE, ElementRecord<TRGT, SRCID>> buildCollectionMapping(ResolvedElementCollectionRelation<SRC, TRGT, S, SRCID, LEFTTABLE, COLLECTIONTABLE, ER> resolvedRelation,
	                                                                                                                             ConfiguredRelationalPersister<SRC, SRCID> sourcePersister) {
		ElementCollectionMapping<SRC, SRCID, TRGT, S, LEFTTABLE, COLLECTIONTABLE, ElementRecord<TRGT, SRCID>> elementCollectionMapping;
		if (resolvedRelation.isOrdered()) {
			elementCollectionMapping = buildIndexedCollectionMapping((ResolvedElementCollectionRelation<SRC, TRGT, S, SRCID, LEFTTABLE, COLLECTIONTABLE, IndexedElementRecord<TRGT, SRCID>>) resolvedRelation, sourcePersister);
		} else {
			elementCollectionMapping = buildNonIndexedCollectionMapping(resolvedRelation, sourcePersister);
		}
		return elementCollectionMapping;
	}
	
	private <SRC, SRCID, TRGT, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, COLLECTIONTABLE extends Table<COLLECTIONTABLE>, ER extends ElementRecord<TRGT, SRCID>>
	ElementCollectionMapping<SRC, SRCID, TRGT, S, LEFTTABLE, COLLECTIONTABLE, ElementRecord<TRGT, SRCID>> buildNonIndexedCollectionMapping(ResolvedElementCollectionRelation<SRC, TRGT, S, SRCID, LEFTTABLE, COLLECTIONTABLE, ER> resolvedRelation,
	                                                                                                                                       ConfiguredRelationalPersister<SRC, SRCID> sourcePersister) {
		EmbeddedClassMapping<ER, COLLECTIONTABLE> elementRecordMappingStrategy = new EmbeddedClassMapping<ER, COLLECTIONTABLE>((Class) ElementRecord.class,
				resolvedRelation.getJoin().getRightKey().getTable(),
				resolvedRelation.getColumnMapping(),
				Collections.emptyMap(),
				new Function<ColumnedRow, ER>() {
					@Override
					public ER apply(ColumnedRow columnedRow) {
						// todo: get the id from the ColumnedRow, instantiate a ElementRecord if id is not null
						SRCID srcid = sourcePersister.getMapping().getIdMapping().getIdentifierAssembler().assemble(columnedRow);
						if (srcid != null) {
							return (ER) new ElementRecord<>(srcid, null).setPersisted(true);
						} else {
							return null;
						}
					}
				});
		
		ElementRecordMapping<TRGT, SRCID, COLLECTIONTABLE, ER> elementRecordMapping = new ElementRecordMapping<>(
				resolvedRelation.getJoin().getRightKey().getTable(),
				elementRecordMappingStrategy,
				sourcePersister.getMapping().getIdMapping().getIdentifierAssembler(),
				resolvedRelation.getPrimaryKeyForeignKeyColumnMapping());
		
		ElementCollectionMapping<SRC, SRCID, TRGT, S, LEFTTABLE, COLLECTIONTABLE, ER> elementCollectionMapping = new ElementCollectionMapping<>(resolvedRelation.getJoin().getKeyMapping(), elementRecordMapping);
		return (ElementCollectionMapping<SRC, SRCID, TRGT, S, LEFTTABLE, COLLECTIONTABLE, ElementRecord<TRGT, SRCID>>) elementCollectionMapping;
	}
	
	private <SRC, SRCID, TRGT, S extends Collection<TRGT>, LEFTTABLE extends Table<LEFTTABLE>, COLLECTIONTABLE extends Table<COLLECTIONTABLE>, ER extends IndexedElementRecord<TRGT, SRCID>>
	ElementCollectionMapping<SRC, SRCID, TRGT, S, LEFTTABLE, COLLECTIONTABLE, ElementRecord<TRGT, SRCID>> buildIndexedCollectionMapping(ResolvedElementCollectionRelation<SRC, TRGT, S, SRCID, LEFTTABLE, COLLECTIONTABLE, ER> resolvedRelation, ConfiguredRelationalPersister<SRC, SRCID> sourcePersister) {
		EmbeddedClassMapping<ER, COLLECTIONTABLE> elementRecordMappingStrategy = new EmbeddedClassMapping<ER, COLLECTIONTABLE>((Class) IndexedElementRecord.class,
				resolvedRelation.getJoin().getRightKey().getTable(),
				resolvedRelation.getColumnMapping());
		IndexedElementRecordMapping<TRGT, SRCID, COLLECTIONTABLE, ER> elementRecordMapping = new IndexedElementRecordMapping<>(
				resolvedRelation.getJoin().getRightKey().getTable(),
				elementRecordMappingStrategy,
				resolvedRelation.getIndexColumn(),
				sourcePersister.getMapping().getIdMapping().getIdentifierAssembler(),
				resolvedRelation.getPrimaryKeyForeignKeyColumnMapping());
		
		IndexedElementCollectionMapping<SRC, SRCID, TRGT, S, LEFTTABLE, COLLECTIONTABLE, ? extends ElementRecord<TRGT, SRCID>> indexedElementCollectionMapping = new IndexedElementCollectionMapping<>(
				resolvedRelation.getJoin().getKeyMapping(),
				elementRecordMapping);
		return (ElementCollectionMapping<SRC, SRCID, TRGT, S, LEFTTABLE, COLLECTIONTABLE, ElementRecord<TRGT, SRCID>>) indexedElementCollectionMapping;
	}
	
	private static class ElementCollectionMapping<SRC, SRCID, TRGT, S extends Collection<TRGT>, SRCTABLE extends Table<SRCTABLE>, COLLECTIONTABLE extends Table<COLLECTIONTABLE>, ER extends ElementRecord<TRGT, SRCID>> {
		public final KeyMapping<SRCTABLE, COLLECTIONTABLE, SRCID> joinMapping;
		public final DefaultEntityMapping<ER, ER, COLLECTIONTABLE> elementRecordMapping;
		
		public ElementCollectionMapping(KeyMapping<SRCTABLE, COLLECTIONTABLE, SRCID> joinMapping, DefaultEntityMapping<ER, ER, COLLECTIONTABLE> elementRecordMapping) {
			this.joinMapping = joinMapping;
			this.elementRecordMapping = elementRecordMapping;
		}
		
		protected Accessor<SRC, Collection<ER>> collectionProvider(Accessor<SRC, S> collectionAccessor,
		                                                           IdAccessor<SRC, SRCID> idAccessor,
		                                                           boolean markAsPersisted) {
			return src -> Iterables.collect(Nullable.nullable(collectionAccessor.get(src)).getOr(() -> (S) new ArrayList<>()),
					trgt -> (ER) new ElementRecord<>(idAccessor.getId(src), trgt).setPersisted(markAsPersisted),
					HashSet::new);
		}
	}
	
	private static class IndexedElementCollectionMapping<SRC, SRCID, TRGT, S extends Collection<TRGT>, SRCTABLE extends Table<SRCTABLE>, COLLECTIONTABLE extends Table<COLLECTIONTABLE>, ER extends IndexedElementRecord<TRGT, SRCID>>
			extends ElementCollectionMapping<SRC, SRCID, TRGT, S, SRCTABLE, COLLECTIONTABLE, ER> {
		
		public IndexedElementCollectionMapping(KeyMapping<SRCTABLE, COLLECTIONTABLE, SRCID> joinMapping, DefaultEntityMapping<ER, ER, COLLECTIONTABLE> elementRecordMapping) {
			super(joinMapping, elementRecordMapping);
		}
		
		@Override
		protected Accessor<SRC, Collection<ER>> collectionProvider(Accessor<SRC, S> collectionAccessor,
		                                                           IdAccessor<SRC, SRCID> idAccessor,
		                                                           boolean markAsPersisted) {
			return src -> {
				S collection = nullable(collectionAccessor.get(src)).getOr(() -> (S) new ArrayList<>());
				return Iterables.collect(collection,
						trgt -> (ER) new IndexedElementRecord<>(idAccessor.getId(src), trgt, Iterables.indexOf(collection, trgt)).setPersisted(markAsPersisted),
						HashSet::new);
			};
		}
	}
	
	private static class TargetInstancesInsertCascader<SRC, TRGT, SRCID, ER extends ElementRecord<TRGT, SRCID>> extends AfterInsertCollectionCascader<SRC, ER> {
		
		private final Accessor<SRC, ? extends Collection<ER>> collectionGetter;
		
		public TargetInstancesInsertCascader(EntityPersister<ER, ER> targetPersister, Accessor<SRC, ? extends Collection<ER>> collectionGetter) {
			super(targetPersister);
			this.collectionGetter = collectionGetter;
		}
		
		@Override
		protected void postTargetInsert(Iterable<? extends ER> entities) {
			// Nothing to do. Identified#isPersisted flag should be fixed by target persister
		}
		
		@Override
		protected Collection<ER> getTargets(SRC source) {
			return collectionGetter.get(source);
		}
	}
	
	public static class ElementRecordPersister<TRGT, SRCID, T extends Table<T>, ER extends ElementRecord<TRGT, SRCID>> extends SimpleRelationalEntityPersister<ER, ER, T> {
		
		public ElementRecordPersister(DefaultEntityMapping<ER, ER, T> elementRecordMapping, Dialect dialect, ConnectionConfiguration connectionConfiguration) {
			super(elementRecordMapping, dialect, connectionConfiguration);
		}
	}
}
