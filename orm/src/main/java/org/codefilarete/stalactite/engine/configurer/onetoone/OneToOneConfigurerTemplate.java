package org.codefilarete.stalactite.engine.configurer.onetoone;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.ValueAccessPoint;
import org.codefilarete.stalactite.engine.CascadeOptions.RelationMode;
import org.codefilarete.stalactite.engine.MappingConfigurationException;
import org.codefilarete.stalactite.engine.NotYetSupportedOperationException;
import org.codefilarete.stalactite.engine.RuntimeMappingException;
import org.codefilarete.stalactite.engine.configurer.CascadeConfigurationResult;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneRelationConfigurer.FirstPhaseCycleLoadListener;
import org.codefilarete.stalactite.engine.listener.InsertListener;
import org.codefilarete.stalactite.engine.listener.SelectListener;
import org.codefilarete.stalactite.engine.listener.UpdateListener;
import org.codefilarete.stalactite.engine.runtime.ConfiguredJoinedTablesPersister;
import org.codefilarete.stalactite.engine.runtime.EntityConfiguredJoinedTablesPersister;
import org.codefilarete.stalactite.engine.runtime.EntityConfiguredPersister;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.collection.Iterables;

public abstract class OneToOneConfigurerTemplate<SRC, TRGT, SRCID, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID> {
	
	protected final EntityConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister;
	
	protected final OneToOneRelation<SRC, TRGT, TRGTID> oneToOneRelation;
	
	protected OneToOneConfigurerTemplate(EntityConfiguredJoinedTablesPersister<SRC, SRCID> sourcePersister, OneToOneRelation<SRC, TRGT, TRGTID> oneToOneRelation) {
		this.sourcePersister = sourcePersister;
		this.oneToOneRelation = oneToOneRelation;
	}
	
	public void configure(String tableAlias,
						  EntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister,
						  boolean loadSeparately) {
		assertConfigurationIsSupported();
		
		// Finding joined columns
		EntityMapping<TRGT, TRGTID, RIGHTTABLE> targetMappingStrategy = targetPersister.getMapping();
		Duo<Key<LEFTTABLE, JOINID>, Key<RIGHTTABLE, JOINID>> foreignKeyColumns = determineForeignKeyColumns(sourcePersister.getMapping(), targetMappingStrategy);
		
		BeanRelationFixer<SRC, TRGT> beanRelationFixer = determineRelationFixer();
		
		addSelectJoin(tableAlias, targetPersister, foreignKeyColumns.getLeft(), foreignKeyColumns.getRight(), beanRelationFixer, loadSeparately);
		addWriteCascades(targetPersister);
	}
	
	public CascadeConfigurationResult<SRC, TRGT> configureWithSelectIn2Phases(String tableAlias,
																			  EntityConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister,
																			  FirstPhaseCycleLoadListener<SRC, TRGTID> firstPhaseCycleLoadListener) {
		assertConfigurationIsSupported();
		
		// Finding joined columns
		EntityMapping<TRGT, TRGTID, RIGHTTABLE> targetMappingStrategy = targetPersister.getMapping();
		Duo<Key<LEFTTABLE, JOINID>, Key<RIGHTTABLE, JOINID>> foreignKeyColumns = determineForeignKeyColumns(sourcePersister.getMapping(), targetMappingStrategy);
		
		BeanRelationFixer<SRC, TRGT> beanRelationFixer = determineRelationFixer();
		
		addSelectIn2Phases(tableAlias, targetPersister, foreignKeyColumns.getLeft(), foreignKeyColumns.getRight(), firstPhaseCycleLoadListener);
		addWriteCascades(targetPersister);
		return new CascadeConfigurationResult<>(beanRelationFixer, sourcePersister);
	}
	
	private void assertConfigurationIsSupported() {
		
		RelationMode maintenanceMode = oneToOneRelation.getRelationMode();
		if (maintenanceMode == RelationMode.ASSOCIATION_ONLY) {
			throw new MappingConfigurationException(RelationMode.ASSOCIATION_ONLY + " is only relevant for one-to-many association");
		}
		EntityMapping<SRC, SRCID, LEFTTABLE> mappingStrategy = sourcePersister.getMapping();
		if (mappingStrategy.getTargetTable().getPrimaryKey().getColumns().size() > 1) {
			throw new NotYetSupportedOperationException("Joining tables on a composed primary key is not (yet) supported");
		}
	}
	
	protected abstract Duo<Key<LEFTTABLE, JOINID>, Key<RIGHTTABLE, JOINID>> determineForeignKeyColumns(EntityMapping<SRC, SRCID, LEFTTABLE> mappingStrategy,
																									   EntityMapping<TRGT, TRGTID, RIGHTTABLE> targetMappingStrategy);
	
	protected abstract BeanRelationFixer<SRC, TRGT> determineRelationFixer();
	
	protected void addWriteCascades(EntityConfiguredPersister<TRGT, TRGTID> targetPersister) {
		boolean orphanRemoval = oneToOneRelation.getRelationMode() == RelationMode.ALL_ORPHAN_REMOVAL;
		boolean writeAuthorized = oneToOneRelation.getRelationMode() != RelationMode.READ_ONLY;
		if (writeAuthorized) {
			// NB: "delete removed" will be treated internally by updateCascade() and deleteCascade()
			addInsertCascade(targetPersister);
			addUpdateCascade(targetPersister, orphanRemoval);
			addDeleteCascade(targetPersister, orphanRemoval);
		}
	}
	
	@SuppressWarnings("squid:S1172")    // argument targetPersister is used by subclasses
	protected void addInsertCascade(EntityConfiguredPersister<TRGT, TRGTID> targetPersister) {
		// if cascade is mandatory, then adding nullability checking before insert
		if (!oneToOneRelation.isNullable()) {
			sourcePersister.addInsertListener(new MandatoryRelationAssertBeforeInsertListener<>(oneToOneRelation.getTargetProvider()));
		}
	}
	
	protected void addUpdateCascade(EntityConfiguredPersister<TRGT, TRGTID> targetPersister, boolean orphanRemoval) {
		// if cascade is mandatory, then adding nullability checking before insert
		if (!oneToOneRelation.isNullable()) {
			sourcePersister.addUpdateListener(new MandatoryRelationAssertBeforeUpdateListener<>(oneToOneRelation.getTargetProvider()));
		}
	}
	
	protected abstract void addDeleteCascade(EntityConfiguredPersister<TRGT, TRGTID> targetPersister, boolean orphanRemoval);
	
	protected void addSelectJoin(
			String tableAlias,
			ConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister,
			Key<LEFTTABLE, JOINID> leftKey,
			Key<RIGHTTABLE, JOINID> rightKey,
			BeanRelationFixer<SRC, TRGT> beanRelationFixer,
			boolean loadSeparately) {
		// we add target subgraph joins to the one that was created
		targetPersister.joinAsOne(sourcePersister, leftKey, rightKey, tableAlias, beanRelationFixer, oneToOneRelation.isNullable(), loadSeparately);
		
		// We trigger subgraph load event (via targetSelectListener) on loading of our graph.
		// Done for instance for event consumers that initialize some things, because given ids of methods are those of source entity
		SelectListener targetSelectListener = targetPersister.getPersisterListener().getSelectListener();
		sourcePersister.addSelectListener(new SelectListener<SRC, SRCID>() {
			@Override
			public void beforeSelect(Iterable<SRCID> ids) {
				// since ids are not those of its entities, we should not pass them as argument, this will only initialize things if needed
				targetSelectListener.beforeSelect(Collections.emptyList());
			}
			
			@Override
			public void afterSelect(Iterable<? extends SRC> result) {
				List collect = Iterables.collectToList(result, oneToOneRelation.getTargetProvider()::get);
				// NB: entity can be null when loading relation, we skip nulls to prevent a NPE
				collect.removeIf(Objects::isNull);
				targetSelectListener.afterSelect(collect);
			}
			
			@Override
			public void onError(Iterable<SRCID> ids, RuntimeException exception) {
				// since ids are not those of its entities, we should not pass them as argument
				targetSelectListener.onError(Collections.emptyList(), exception);
			}
		});
	}
	
	abstract protected void addSelectIn2Phases(
			String tableAlias,
			ConfiguredJoinedTablesPersister<TRGT, TRGTID> targetPersister,
			Key<LEFTTABLE, JOINID> leftKey,
			Key<RIGHTTABLE, JOINID> rightKey,
			FirstPhaseCycleLoadListener<SRC, TRGTID> firstPhaseCycleLoadListener);
	
	
	public static class MandatoryRelationAssertBeforeInsertListener<C> implements InsertListener<C> {
		
		private final Accessor<C, ?> targetAccessor;
		
		public MandatoryRelationAssertBeforeInsertListener(Accessor<C, ?> targetAccessor) {
			this.targetAccessor = targetAccessor;
		}
		
		@Override
		public void beforeInsert(Iterable<? extends C> entities) {
			for (C pawn : entities) {
				Object modifiedTarget = targetAccessor.get(pawn);
				if (modifiedTarget == null) {
					throw newRuntimeMappingException(pawn, targetAccessor);
				}
			}
		}
	}
	
	public static class MandatoryRelationAssertBeforeUpdateListener<C> implements UpdateListener<C> {
		
		private final Accessor<C, ?> targetAccessor;
		
		public MandatoryRelationAssertBeforeUpdateListener(Accessor<C, ?> targetAccessor) {
			this.targetAccessor = targetAccessor;
		}
		
		@Override
		public void beforeUpdate(Iterable<? extends Duo<? extends C, ? extends C>> payloads, boolean allColumnsStatement) {
			for (Duo<? extends C, ? extends C> payload : payloads) {
				C modifiedEntity = payload.getLeft();
				Object modifiedTarget = targetAccessor.get(modifiedEntity);
				if (modifiedTarget == null) {
					throw newRuntimeMappingException(modifiedEntity, targetAccessor);
				}
			}
		}
	}
	
	public static RuntimeMappingException newRuntimeMappingException(Object pawn, ValueAccessPoint accessor) {
		return new RuntimeMappingException("Non null value expected for relation "
				+ AccessorDefinition.toString(accessor) + " on object " + pawn);
	}
}
