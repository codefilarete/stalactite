package org.gama.stalactite.persistence.engine;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.danekja.java.util.function.serializable.SerializableBiFunction;
import org.gama.lang.Duo;
import org.gama.lang.Reflections;
import org.gama.lang.collection.Iterables;
import org.gama.reflection.IReversibleAccessor;
import org.gama.stalactite.persistence.engine.ColumnOptions.IdentifierPolicy;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupport.EntityDecoratedEmbeddableConfigurationSupport;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupport.EntityMappingBuilder;
import org.gama.stalactite.persistence.engine.cascade.AfterUpdateCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteCascader;
import org.gama.stalactite.persistence.engine.cascade.BeforeInsertCascader;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelectExecutor;
import org.gama.stalactite.persistence.engine.listening.DeleteByIdListener;
import org.gama.stalactite.persistence.engine.listening.UpdateByIdListener;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IdAccessor;
import org.gama.stalactite.persistence.mapping.SinglePropertyIdAccessor;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
public class InheritedEntityMappingBuilder<C, I> {
	
	// En cas d'héritage par jointure:
	// - Une classe sur 2 tables
	// - identifier accessor : pas modif d'algo de recherche
	// - la colonne d'id est sur la table parente
	// - 2 EntityMapping : 1 pour la mère, une autre pour la fille
	// - la fille est TOUJOURS en already assigned
	// - attention si un persister existe déjà pour la mère, que faire ? le mieux, faire un diff
	// - créer un JoinedTablePersister avec les insert/update/delete cascade
	// - n'est possible que sur build(PeristenceContext)
	
	private final FluentEntityMappingConfigurationSupport<C, I> configurationSupport;
	
	public InheritedEntityMappingBuilder(FluentEntityMappingConfigurationSupport<C, I> configurationSupport) {
		this.configurationSupport = configurationSupport;
	}
	
	// dedicated to configurationSupport.joinTable = true
	public Persister<C, I, Table> build(PersistenceContext persistenceContext) {
		return build(persistenceContext, new Table(configurationSupport.getPersistedClass().getSimpleName()));
	}
	
	public <T extends Table> Persister<C, I, T> build(PersistenceContext persistenceContext, T childClassTargetTable) {
		IReversibleAccessor<C, I> localIdentifierAccessor = configurationSupport.identifierAccessor;
		
		if (localIdentifierAccessor == null) {
			if (configurationSupport.inheritanceMapping == null) {
				// no ClassMappingStratey in hierarchy, so we can't get an identifier from it => impossible
				SerializableBiFunction<ColumnOptions, IdentifierPolicy, IFluentMappingBuilder> identifierMethodReference = ColumnOptions::identifier;
				Method identifierSetter = configurationSupport.methodSpy.findMethod(identifierMethodReference);
				throw new UnsupportedOperationException("Identifier is not defined, please add one throught " + Reflections.toString(identifierSetter));
			} else {
				// at least one parent ClassMappingStrategy exists, it necessarily defines an identifier : we stop at the very first one
				IdAccessor<? super C, ?> idAccessor = configurationSupport.inheritanceMapping.getIdMappingStrategy().getIdAccessor();
				if (!(idAccessor instanceof SinglePropertyIdAccessor)) {
					throw new NotYetSupportedOperationException();
				}
				localIdentifierAccessor = ((SinglePropertyIdAccessor<C, I>) idAccessor).getIdAccessor();
			}
		}
		
		EntityDecoratedEmbeddableConfigurationSupport<C, I> entityMappingConfiguration = configurationSupport.entityDecoratedEmbeddableConfigurationSupport;
		EmbeddableMappingBuilder<C> builder = new EmbeddableMappingBuilder<>(entityMappingConfiguration);
		
		Map<IReversibleAccessor, Column> childClassColumnMapping = builder.build(persistenceContext.getDialect(), childClassTargetTable);
		// creating primary key columns on child table as they are on parent table
		Set<Column<Table, Object>> primaryKey = configurationSupport.inheritanceMapping.getTargetTable().getPrimaryKey().getColumns();
		for (Column column : primaryKey) {
			childClassTargetTable.addColumn(column.getName(), column.getJavaType()).primaryKey();
		}
		
		// for now only single column primary key are really supported
		Set<Column<Table, Object>> columns = childClassTargetTable.getPrimaryKey().getColumns();
		childClassColumnMapping.put(localIdentifierAccessor, Iterables.first(columns));
		
		IdentifierInsertionManager<C, I> identifierInsertionManager = new AlreadyAssignedIdentifierManager<>(configurationSupport.inheritanceMapping.getIdMappingStrategy().getIdentifierInsertionManager().getIdentifierType());
		ClassMappingStrategy<C, I, T> childClassMappingStrategy = new ClassMappingStrategy<>(entityMappingConfiguration.getClassToPersist(),
						childClassTargetTable, (Map) childClassColumnMapping, localIdentifierAccessor, identifierInsertionManager);
		
		EntityMappingBuilder<C, I> superBuilder = new EntityMappingBuilder<>(configurationSupport);
		Persister<? super C, I, ?> superPersister = superBuilder.build(persistenceContext, configurationSupport.inheritanceMapping.getTargetTable());
//		EntityMappingBuilder<C, I> superBuilder = new EntityMappingBuilder<>(configurationSupport);
//		Persister<? super C, I, ?> superPersister = superBuilder.build(persistenceContext, configurationSupport.inheritanceMapping.getTargetTable());
		
		
		JoinedStrategiesSelectExecutor<C, I> joinedStrategiesSelectExecutor = new JoinedStrategiesSelectExecutor<>(childClassMappingStrategy, persistenceContext.getDialect(), persistenceContext.getConnectionProvider());
		Column subclassPK = Iterables.first((Set<Column<Table, Object>>) childClassTargetTable.getPrimaryKey().getColumns());
		Column superclassPK = Iterables.first((Set<Column<Table, Object>>) configurationSupport.inheritanceMapping.getTargetTable().getPrimaryKey().getColumns());
		joinedStrategiesSelectExecutor.addComplementaryTables(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, configurationSupport.inheritanceMapping, (target, input) -> {
			// applying values from inherited bean (input) to subclass one (target)
			for (IReversibleAccessor columnFieldEntry : configurationSupport.inheritanceMapping.getMainMappingStrategy().getPropertyToColumn().keySet()) {
				columnFieldEntry.toMutator().set(target, columnFieldEntry.get(input));
			}
		}, subclassPK, superclassPK, false);
		
		Persister<C, I, T> result = new Persister<C, I, T>(persistenceContext, childClassMappingStrategy) {
			@Override
			protected List<C> doSelect(Collection ids) {
				return joinedStrategiesSelectExecutor.select(ids);
			}
			
			@Override
			public Set<Table> giveImpliedTables() {
				return joinedStrategiesSelectExecutor.getJoinedStrategiesSelect().giveTables();
			}
		};
		// made so inheritanceMapping participate into persistenceContext such as DDL deployment (quite mandatory)
		persistenceContext.addPersister(result);
		result.getPersisterListener().addInsertListener(new BeforeInsertCascader<C, Object>((Persister<Object, ?, ?>) superPersister) {
			
			@Override
			protected void postTargetInsert(Iterable<? extends Object> entities) {
				// nothing
			}
			
			@Override
			protected Object getTarget(C o) {
				return o;
			}
		});
		
		result.getPersisterListener().addUpdateListener(new AfterUpdateCascader<C, Object>((Persister<Object, ?, ?>) superPersister) {
			@Override
			protected void postTargetUpdate(Iterable entities) {
				// nothing
			}
			
			@Override
			protected Duo<Object, Object> getTarget(Object modifiedTrigger, Object unmodifiedTrigger) {
				return new Duo<>(modifiedTrigger, unmodifiedTrigger);
			}
		});
		
		result.getPersisterListener().addUpdateByIdListener(new UpdateByIdListener<C>() {
			@Override
			public void afterUpdateById(Iterable<C> entities) {
				superPersister.updateById((Iterable) entities);
			}
		});
		
		result.getPersisterListener().addDeleteListener(new BeforeDeleteCascader<C, Object>((Persister<Object, ?, ?>) superPersister) {
			
			@Override
			protected void postTargetDelete(Iterable<Object> entities) {
				// nothing
			}
			
			@Override
			protected Object getTarget(C c) {
				return c;
			}
		});
		
		result.getPersisterListener().addDeleteByIdListener(new DeleteByIdListener<C>() {
			@Override
			public void afterDeleteById(Iterable<C> entities) {
				superPersister.deleteById((Iterable) entities);
			}
		});
		return result;
	}
}
