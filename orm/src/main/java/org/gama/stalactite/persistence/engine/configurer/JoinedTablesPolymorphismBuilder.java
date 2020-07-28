package org.gama.stalactite.persistence.engine.configurer;

import java.util.HashMap;
import java.util.Map;

import org.gama.lang.collection.Iterables;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.ValueAccessPointSet;
import org.gama.stalactite.persistence.engine.AssociationTableNamingStrategy;
import org.gama.stalactite.persistence.engine.ColumnNamingStrategy;
import org.gama.stalactite.persistence.engine.ElementCollectionTableNamingStrategy;
import org.gama.stalactite.persistence.engine.ForeignKeyNamingStrategy;
import org.gama.stalactite.persistence.engine.PersisterRegistry;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.JoinedTablesPolymorphism;
import org.gama.stalactite.persistence.engine.SubEntityMappingConfiguration;
import org.gama.stalactite.persistence.engine.TableNamingStrategy;
import org.gama.stalactite.persistence.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.Identification;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.MappingPerTable.Mapping;
import org.gama.stalactite.persistence.engine.runtime.EntityMappingStrategyTreeSelectBuilder;
import org.gama.stalactite.persistence.engine.runtime.IEntityConfiguredJoinedTablesPersister;
import org.gama.stalactite.persistence.engine.runtime.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.runtime.JoinedTablesPolymorphicPersister;
import org.gama.stalactite.persistence.engine.runtime.PersisterListenerWrapper;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.IConnectionConfiguration;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

import static org.gama.lang.Nullable.nullable;

/**
 * @author Guillaume Mary
 */
abstract class JoinedTablesPolymorphismBuilder<C, I, T extends Table> extends AbstractPolymorphicPersisterBuilder<C, I, T> {
	
	private final JoinedTablesPolymorphism<C, I> polymorphismPolicy;
	private final TableNamingStrategy tableNamingStrategy;
	
	JoinedTablesPolymorphismBuilder(JoinedTablesPolymorphism<C, I> polymorphismPolicy,
									Identification identification,
									JoinedTablesPersister<C, I, T> mainPersister,
									ColumnBinderRegistry columnBinderRegistry,
									ColumnNameProvider columnNameProvider,
									TableNamingStrategy tableNamingStrategy,
									ColumnNamingStrategy columnNamingStrategy,
									ForeignKeyNamingStrategy foreignKeyNamingStrategy,
									ElementCollectionTableNamingStrategy elementCollectionTableNamingStrategy,
									ColumnNamingStrategy joinColumnNamingStrategy,
									ColumnNamingStrategy indexColumnNamingStrategy,
									AssociationTableNamingStrategy associationTableNamingStrategy) {
		super(polymorphismPolicy, identification, mainPersister, columnBinderRegistry, columnNameProvider, columnNamingStrategy, foreignKeyNamingStrategy,
				elementCollectionTableNamingStrategy, joinColumnNamingStrategy, indexColumnNamingStrategy, associationTableNamingStrategy);
		this.polymorphismPolicy = polymorphismPolicy;
		this.tableNamingStrategy = tableNamingStrategy;
	}
	
	@Override
	public IEntityConfiguredJoinedTablesPersister<C, I> build(Dialect dialect, IConnectionConfiguration connectionConfiguration, PersisterRegistry persisterRegistry) {
		Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> persisterPerSubclass = new HashMap<>();
		
		BeanMappingBuilder beanMappingBuilder = new BeanMappingBuilder();
		for (SubEntityMappingConfiguration<? extends C, I> subConfiguration : polymorphismPolicy.getSubClasses()) {
			// first we'll use table of columns defined in embedded override
			// then the one defined by inheritance
			// if both are null we'll create a new one
			Table tableDefinedByColumnOverride = BeanMappingBuilder.giveTargetTable(subConfiguration.getPropertiesMapping());
			Table tableDefinedByInheritanceConfiguration = polymorphismPolicy.giveTable(subConfiguration);
			
			assertAllAreEqual(tableDefinedByColumnOverride, tableDefinedByInheritanceConfiguration);
			
			Table subTable = nullable(tableDefinedByColumnOverride)
					.elseSet(tableDefinedByInheritanceConfiguration)
					.getOr(() -> new Table(tableNamingStrategy.giveName(subConfiguration.getEntityType())));
			
			Map<IReversibleAccessor, Column> subEntityPropertiesMapping = beanMappingBuilder.build(subConfiguration.getPropertiesMapping(), subTable,
					this.columnBinderRegistry, this.columnNameProvider);
			addPrimarykey(identification, subTable);
			addForeignKey(identification, subTable);
			Mapping subEntityMapping = new Mapping(subConfiguration, subTable, subEntityPropertiesMapping, false);
			addIdentificationToMapping(identification, subEntityMapping);
			ClassMappingStrategy<? extends C, I, Table> classMappingStrategy = PersisterBuilderImpl.createClassMappingStrategy(
					false,
					subTable,
					subEntityPropertiesMapping,
					new ValueAccessPointSet(),	// TODO: implement properties set by constructor feature in joined-tables polymorphism
					identification,
					subConfiguration.getPropertiesMapping().getBeanType(),
					null);
			
			// NB: persisters are not registered into PersistenceContext because it may break implicit polymorphism principle (persisters are then
			// available by PersistenceContext.getPersister(..)) and it is not sure that they are perfect ones (all their features should be tested)
			JoinedTablesPersister subclassPersister = new JoinedTablesPersister(classMappingStrategy, dialect, connectionConfiguration);
			persisterPerSubclass.put(subConfiguration.getEntityType(), subclassPersister);
			
			// Adding join with parent table to select
			Column subEntityPrimaryKey = (Column) Iterables.first(subTable.getPrimaryKey().getColumns());
			Column entityPrimaryKey = (Column) Iterables.first(this.mainPersister.getMainTable().getPrimaryKey().getColumns());
			subclassPersister.getEntityMappingStrategyTreeSelectExecutor().addComplementaryJoin(EntityMappingStrategyTreeSelectBuilder.ROOT_STRATEGY_NAME,
					this.mainPersister.getMappingStrategy(), subEntityPrimaryKey, entityPrimaryKey);
		}
		
		JoinedTablesPolymorphicPersister<C, I> surrogate = new JoinedTablesPolymorphicPersister(
				mainPersister, persisterPerSubclass, connectionConfiguration.getConnectionProvider(),
				dialect);
		PersisterListenerWrapper<C, I> result = new PersisterListenerWrapper<>(surrogate);
		
		for (SubEntityMappingConfiguration<? extends C, I> subConfiguration : polymorphismPolicy.getSubClasses()) {
			JoinedTablesPersister<C, I, T> subEntityPersister = persisterPerSubclass.get(subConfiguration.getEntityType());
			
			// We register relation of sub class persister to take into account its specific one-to-ones, one-to-manys and element collection mapping
			registerRelationCascades(subConfiguration, dialect, connectionConfiguration, persisterRegistry, subEntityPersister);
		}
		
		return result;
	}
	
	abstract void addPrimarykey(Identification identification, Table table);
	
	abstract void addForeignKey(Identification identification, Table table);
	
	abstract void addIdentificationToMapping(Identification identification, Mapping mapping);
}
