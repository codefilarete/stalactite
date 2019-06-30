package org.gama.stalactite.persistence.engine;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import org.gama.lang.Duo;
import org.gama.lang.bean.Objects;
import org.gama.lang.collection.Iterables;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.MethodReferenceCapturer;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupport.EntityLinkageByColumn;
import org.gama.stalactite.persistence.engine.cascade.AfterUpdateSupport;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteByIdSupport;
import org.gama.stalactite.persistence.engine.cascade.BeforeDeleteSupport;
import org.gama.stalactite.persistence.engine.cascade.BeforeInsertSupport;
import org.gama.stalactite.persistence.engine.cascade.JoinedStrategiesSelect;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.listening.PersisterListener;
import org.gama.stalactite.persistence.engine.listening.UpdateByIdListener;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.IEntityMappingStrategy;
import org.gama.stalactite.persistence.mapping.IdAccessor;
import org.gama.stalactite.persistence.mapping.SinglePropertyIdAccessor;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.Nullable.nullable;

/**
 * Classes aimed at building a persister for joined tables inheritance
 * 
 * @author Guillaume Mary
 */
public class JoinedTablesEntityMappingBuilder<C, I> {
	
	private final EntityMappingConfiguration<C, I> configurationSupport;
	
	private final MethodReferenceCapturer methodSpy;
	
	public JoinedTablesEntityMappingBuilder(EntityMappingConfiguration<C, I> entityMappingConfiguration, MethodReferenceCapturer methodSpy) {
		this.configurationSupport = entityMappingConfiguration;
		this.methodSpy = methodSpy;
	}
	
	public <T extends Table> JoinedTablesPersister<C, I, T> build(PersistenceContext persistenceContext, @Nullable T childClassTargetTable) {
		if (childClassTargetTable == null) {
			childClassTargetTable = (T) nullable(giveTableUsedInMapping()).orGet(() -> new Table(configurationSupport.getTableNamingStrategy().giveName(configurationSupport.getPersistedClass())));
		}
		
		if (configurationSupport.getIdentifierAccessor() != null && configurationSupport.getInheritanceConfiguration() != null) {
			throw new MappingConfigurationException("Defining an identifier while inheritance is used is not supported");
		}
		
		// We gonna create 2 JoinedTablesPersister (one for parent strategy, one for child strategy) then we will join them alltogether
		// on primary key, so select will result in a join on those 2 strategies.
		// OneToOne and OneToMany will be managed by their respective JoinedTablesPersister (actually done by Persister)
		
		// NB : this persister is in fact a JoinedTablesPersister but its type doesn't matter here
		Persister<? super C, I, Table> superPersister = buildSuperPersister(persistenceContext);
		
		ClassMappingStrategy<? super C, I, ?> parentMappingStrategy = superPersister.getMappingStrategy();
		IReversibleAccessor<C, I> identifierAccessor = giveIdentifierAccessor(parentMappingStrategy);
		
		ClassMappingStrategy<C, I, T> childClassMappingStrategy = buildChildClassMappingStrategy(
				// identifier type can also be deduced from identifierAccessor with Accessor.giveReturnType(..) but getting it from insertion manager
				// is more straightforward because it doesn't need any algorithm to discover accessor type (see giveReturnType(..))
				parentMappingStrategy.getIdMappingStrategy().getIdentifierInsertionManager().getIdentifierType(),
				identifierAccessor,
				childClassTargetTable,
				superPersister.getMainTable(),
				persistenceContext.getDialect());
		
		// NB : result is added to persistenceContext by build(..) method (to participate to DDL deployment)
		JoinedTablesPersister<C, I, T> result = new EntityMappingBuilder<>(configurationSupport, methodSpy).configureRelations(persistenceContext, childClassMappingStrategy);
		// adding join on parent table
		Column subclassPK = Iterables.first((Set<Column<Table, Object>>) childClassTargetTable.getPrimaryKey().getColumns());
		Column superclassPK = Iterables.first((Set<Column<Table, Object>>) superPersister.getMainTable().getPrimaryKey().getColumns());
		result.getJoinedStrategiesSelectExecutor().addComplementaryTable(JoinedStrategiesSelect.FIRST_STRATEGY_NAME, parentMappingStrategy, (target, input) -> {
			// applying values from inherited bean (input) to subclass one (target)
			for (IReversibleAccessor columnFieldEntry : parentMappingStrategy.getPropertyToColumn().keySet()) {
				columnFieldEntry.toMutator().set(target, columnFieldEntry.get(input));
			}
		}, subclassPK, superclassPK, false);
		
		addCascadesBetweenChildAndParentTable(result.getPersisterListener(), superPersister);
		
		return result;
	}
	
	private Table giveTableUsedInMapping() {
		Set<Table> usedTablesInMapping = Iterables.collect(configurationSupport.getPropertiesMapping().getPropertiesMapping(),
				linkage -> linkage instanceof FluentEntityMappingConfigurationSupport.EntityLinkageByColumn,
				linkage -> ((EntityLinkageByColumn) linkage).getColumn().getTable(),
				HashSet::new);
		switch (usedTablesInMapping.size()) {
			case 0:
				return null;
			case 1:
				return Iterables.first(usedTablesInMapping);
			default:
				throw new MappingConfigurationException("Different tables found in columns given as parameter of methods mapping : " + usedTablesInMapping);
		}
	}
	
	private <T extends Table> ClassMappingStrategy<C, I, T> buildChildClassMappingStrategy(Class<I> identifierType,
																						   IReversibleAccessor<C, I> identifierAccessor,
																						   T childTargetTable,
																						   Table parentTargetTable,
																						   Dialect dialect) {
		EmbeddableMappingBuilder<C> builder = new EmbeddableMappingBuilder<>(configurationSupport.getPropertiesMapping());
		Map<IReversibleAccessor, Column> childClassColumnMapping = builder.build(dialect, childTargetTable);
		// getting primary key from parent table and applying it to child table
		Set<Column<Table, Object>> primaryKey = parentTargetTable.getPrimaryKey().getColumns();
		for (Column column : primaryKey) {
			childTargetTable.addColumn(column.getName(), column.getJavaType()).primaryKey();
		}
		
		// for now only single column primary key are really supported
		Set<Column<Table, Object>> columns = childTargetTable.getPrimaryKey().getColumns();
		childClassColumnMapping.put(identifierAccessor, Iterables.first(columns));
		
		// Child class insertion manager is always an "Already assigned" one because parent manages it for her
		IdentifierInsertionManager<C, I> identifierInsertionManager = new AlreadyAssignedIdentifierManager<>(identifierType);
		return new ClassMappingStrategy<>(configurationSupport.getPersistedClass(),
				childTargetTable, (Map) childClassColumnMapping, identifierAccessor, identifierInsertionManager);
	}
	
	private void addCascadesBetweenChildAndParentTable(PersisterListener<C, I> persisterListener, Persister<? super C, I, Table> superPersister) {
		// Before insert of child we must insert parent
		// this weird cast is due to <? super C> ...
		Consumer<Iterable<C>> superEntitiesInsertor = superPersister::insert;
		persisterListener.addInsertListener(new BeforeInsertSupport<>(superEntitiesInsertor, Function.identity()));
		
		// On child update, parent must be updated too, no constraint on order for this, after is arbitrarly choosen
		// this weird cast is due to <? super C> ...
		BiConsumer<Iterable<Duo<C, C>>, Boolean> superEntitiesUpdator = superPersister::update;
		persisterListener.addUpdateListener(new AfterUpdateSupport<>(superEntitiesUpdator, Function.identity()));
		// idem for updateById
		persisterListener.addUpdateByIdListener(new UpdateByIdListener<C>() {
			@Override
			public void afterUpdateById(Iterable<C> entities) {
				superPersister.updateById((Iterable) entities);
			}
		});
		
		// On child deletion, parent must be deleted first
		// this weird cast is due to <? super C> ...
		Consumer<Iterable<C>> superEntitiesDeletor = (Consumer) (Consumer<Iterable>) superPersister::delete;
		persisterListener.addDeleteListener(new BeforeDeleteSupport<>(superEntitiesDeletor, Function.identity()));
		// idem for deleteById
		Consumer<Iterable<C>> superEntitiesDeletorById = (Consumer) (Consumer<Iterable>) superPersister::deleteById;
		persisterListener.addDeleteByIdListener(new BeforeDeleteByIdSupport<>(superEntitiesDeletorById, Function.identity()));
	}
	
	private IReversibleAccessor<C, I> giveIdentifierAccessor(IEntityMappingStrategy<? super C, I, ?> parentMappingStrategy) {
		IdAccessor<? super C, ?> idAccessor = parentMappingStrategy.getIdMappingStrategy().getIdAccessor();
		if (!(idAccessor instanceof SinglePropertyIdAccessor)) {
			throw new NotYetSupportedOperationException();
		}
		return ((SinglePropertyIdAccessor<C, I>) idAccessor).getIdAccessor();
	}
	
	private Persister<? super C, I, Table> buildSuperPersister(PersistenceContext persistenceContext) {
		EntityMappingBuilder<? super C, I> inheritanceMappingBuilder = new EntityMappingBuilder<>(configurationSupport.getInheritanceConfiguration(), methodSpy);
		Table inheritanceTable = Objects.preventNull(configurationSupport.getInheritanceTable(), new Table(configurationSupport.getInheritanceConfiguration().getPersistedClass().getSimpleName()));
		return inheritanceMappingBuilder.build(persistenceContext, inheritanceTable);
	}
}
