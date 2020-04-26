package org.gama.stalactite.persistence.engine.configurer;

import java.util.HashMap;
import java.util.Map;

import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.ValueAccessPointSet;
import org.gama.stalactite.persistence.engine.IEntityConfiguredPersister;
import org.gama.stalactite.persistence.engine.PersistenceContext;
import org.gama.stalactite.persistence.engine.PolymorphismPolicy.SingleTablePolymorphism;
import org.gama.stalactite.persistence.engine.SubEntityMappingConfiguration;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.configurer.BeanMappingBuilder.ColumnNameProvider;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.Identification;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.MappingPerTable.Mapping;
import org.gama.stalactite.persistence.engine.configurer.PersisterBuilderImpl.PolymorphismBuilder;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.dml.binder.ColumnBinderRegistry;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;

/**
 * @author Guillaume Mary
 */
class SingleTablePolymorphismBuilder<C, I, T extends Table, D> implements PolymorphismBuilder<C, I, T> {
	
	private final SingleTablePolymorphism<C, I, D> polymorphismPolicy;
	private final JoinedTablesPersister<C, I, T> mainPersister;
	private final Mapping mainMapping;
	private final ColumnBinderRegistry columnBinderRegistry;
	private final ColumnNameProvider columnNameProvider;
	private final Identification identification;
	private Column<T, D> discriminatorColumn;
	
	SingleTablePolymorphismBuilder(SingleTablePolymorphism<C, I, D> polymorphismPolicy,
								   Identification identification,
								   JoinedTablesPersister<C, I, T> mainPersister,
								   Mapping mainMapping,
								   ColumnBinderRegistry columnBinderRegistry,
								   ColumnNameProvider columnNameProvider
	) {
		this.polymorphismPolicy = polymorphismPolicy;
		this.identification = identification;
		this.mainPersister = mainPersister;
		this.mainMapping = mainMapping;
		this.columnBinderRegistry = columnBinderRegistry;
		this.columnNameProvider = columnNameProvider;
	}
	
	private Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> buildSubEntitiesPersisters(PersistenceContext persistenceContext) {
		Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> persisterPerSubclass = new HashMap<>();
		
		BeanMappingBuilder beanMappingBuilder = new BeanMappingBuilder();
		for (SubEntityMappingConfiguration<? extends C, I> subConfiguration : polymorphismPolicy.getSubClasses()) {
			Map<IReversibleAccessor, Column> subEntityPropertiesMapping = beanMappingBuilder.build(subConfiguration.getPropertiesMapping(), mainPersister.getMainTable(),
					this.columnBinderRegistry, this.columnNameProvider);
			// in single-table polymorphism, main properties must be given to sub-entities ones, because CRUD operations are dipatched to them
			// by a proxy and main persister is not so much used
			subEntityPropertiesMapping.putAll(mainMapping.getMapping());
			ClassMappingStrategy<? extends C, I, T> classMappingStrategy = PersisterBuilderImpl.createClassMappingStrategy(
					false,
					(T) mainPersister.getMainTable(),
					subEntityPropertiesMapping,
					new ValueAccessPointSet(),	// TODO: implement properties set by constructor feature in single-table polymorphism
					identification,
					// TODO: no generated keys handler for now, should be taken on main strategy or brought by identification
					null,
					subConfiguration.getPropertiesMapping().getBeanType(),
					null);
			
			// no primary key to add nor foreign key since table is the same as main one (single table strategy)
			JoinedTablesPersister subclassPersister = new JoinedTablesPersister(persistenceContext, classMappingStrategy);
			
			persisterPerSubclass.put(subConfiguration.getEntityType(), subclassPersister);
		}
		
		return persisterPerSubclass;
	}
	
	private void addDiscriminatorToSelect() {
		this.discriminatorColumn = mainPersister.getMainTable().addColumn(
				polymorphismPolicy.getDiscriminatorColumn(),
				polymorphismPolicy.getDiscrimintorType());
		this.discriminatorColumn.setNullable(false);
	}
	
	@Override
	public IEntityConfiguredPersister<C, I> build(PersistenceContext persistenceContext) {
		addDiscriminatorToSelect();
		Map<Class<? extends C>, JoinedTablesPersister<C, I, T>> joinedTablesPersisters = buildSubEntitiesPersisters(persistenceContext);
		// NB: persisters are not registered into PersistenceContext because it may break implicit polymorphism principle (persisters are then
		// available by PersistenceContext.getPersister(..)) and it is one sure that they are perfect ones (all their features should be tested)
		return new PersisterListenerWrapper<>(new SingleTablePolymorphicPersister(
				mainPersister, joinedTablesPersisters, persistenceContext.getConnectionProvider(), persistenceContext.getDialect(), discriminatorColumn, polymorphismPolicy));
	}
	
}
