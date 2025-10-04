package org.codefilarete.stalactite.engine.configurer.polymorphism;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.reflection.ValueAccessPointMap;
import org.codefilarete.reflection.ValueAccessPointSet;
import org.codefilarete.stalactite.dsl.PolymorphismPolicy;
import org.codefilarete.stalactite.dsl.PolymorphismPolicy.SingleTablePolymorphism;
import org.codefilarete.stalactite.dsl.subentity.SubEntityMappingConfiguration;
import org.codefilarete.stalactite.engine.configurer.AbstractIdentification;
import org.codefilarete.stalactite.engine.configurer.BeanMappingBuilder;
import org.codefilarete.stalactite.engine.configurer.BeanMappingBuilder.BeanMapping;
import org.codefilarete.stalactite.engine.configurer.NamingConfiguration;
import org.codefilarete.stalactite.engine.configurer.PersisterBuilderImpl;
import org.codefilarete.stalactite.engine.runtime.AbstractPolymorphismPersister;
import org.codefilarete.stalactite.engine.runtime.ConfiguredRelationalPersister;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.engine.runtime.singletable.SingleTablePolymorphismPersister;
import org.codefilarete.stalactite.mapping.ClassMapping;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.ColumnBinderRegistry;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.KeepOrderSet;
import org.codefilarete.tool.exception.NotImplementedException;
import org.codefilarete.tool.function.Converter;

/**
 * @author Guillaume Mary
 */
class SingleTablePolymorphismBuilder<C, I, T extends Table<T>, DTYPE> extends AbstractPolymorphicPersisterBuilder<C, I, T> {
	
	private final Map<ReversibleAccessor<C, Object>, Column<T, Object>> mainMapping;
	private final Map<ReversibleAccessor<C, Object>, Column<T, Object>> mainReadonlyMapping;
	private final ValueAccessPointMap<C, Converter<Object, Object>> mainReadConverters;
	private final ValueAccessPointMap<C, Converter<Object, Object>> mainWriteConverters;
	
	SingleTablePolymorphismBuilder(SingleTablePolymorphism<C, DTYPE> polymorphismPolicy,
								   AbstractIdentification<C, I> identification,
								   ConfiguredRelationalPersister<C, I> mainPersister,
								   Map<? extends ReversibleAccessor<C, Object>, ? extends Column<T, Object>> mainMapping,
								   Map<? extends ReversibleAccessor<C, Object>, ? extends Column<T, Object>> mainReadonlyMapping,
								   ValueAccessPointMap<C, ? extends Converter<Object, Object>> mainReadConverters,
								   ValueAccessPointMap<C, ? extends Converter<Object, Object>> mainWriteConverters,
								   ColumnBinderRegistry columnBinderRegistry,
								   NamingConfiguration namingConfiguration
	) {
		super(polymorphismPolicy, identification, mainPersister, columnBinderRegistry, namingConfiguration);
		this.mainMapping = (Map<ReversibleAccessor<C, Object>, Column<T, Object>>) mainMapping;
		this.mainReadonlyMapping = (Map<ReversibleAccessor<C, Object>, Column<T, Object>>) mainReadonlyMapping;
		this.mainReadConverters = (ValueAccessPointMap<C, Converter<Object, Object>>) mainReadConverters;
		this.mainWriteConverters = (ValueAccessPointMap<C, Converter<Object, Object>>) mainWriteConverters;
	}
	
	@Override
	public AbstractPolymorphismPersister<C, I> build(Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		Map<Class<C>, ConfiguredRelationalPersister<C, I>> persisterPerSubclass = collectSubClassPersister(dialect, connectionConfiguration);
		
		// Note that registering the cascades to sub-persisters must be done BEFORE the creation of the main persister to make it have all
		// entities joins and let it build a consistent entity graph load; without it, we miss sub-relations when loading main entities 
		registerSubEntitiesRelations(persisterPerSubclass, dialect, connectionConfiguration);
		
		Column<T, DTYPE> discriminatorColumn = ensureDiscriminatorColumn();
		// NB: persisters are not registered into PersistenceContext because it may break implicit polymorphism principle (persisters are then
		// available by PersistenceContext.getPersister(..)) and it is one sure that they are perfect ones (all their features should be tested)
		SingleTablePolymorphismPersister<C, I, ?, DTYPE> result = new SingleTablePolymorphismPersister<>(
			mainPersister, persisterPerSubclass, connectionConfiguration.getConnectionProvider(), dialect,
			discriminatorColumn, (SingleTablePolymorphism<C, DTYPE>) polymorphismPolicy);
		
		return result;
	}
	
	private Map<Class<C>, Set<C>> computeEntitiesPerType(Iterable<? extends C> entities) {
		Map<Class<C>, Set<C>> entitiesPerType = new HashMap<>();
		entities.forEach(entity ->
				entitiesPerType.computeIfAbsent((Class<C>) entity.getClass(), p -> new KeepOrderSet<>()).add(entity)
		);
		return entitiesPerType;
	}
	
	private <D extends C> Map<Class<D>, ConfiguredRelationalPersister<D, I>> collectSubClassPersister(Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		Map<Class<D>, ConfiguredRelationalPersister<D, I>> persisterPerSubclass = new HashMap<>();
		
		T mainTable = (T) mainPersister.getMapping().getTargetTable();
		for (SubEntityMappingConfiguration<D> subConfiguration : ((Set<SubEntityMappingConfiguration<D>>) (Set) polymorphismPolicy.getSubClasses())) {
			persisterPerSubclass.put(subConfiguration.getEntityType(),
									 buildSubclassPersister(dialect, connectionConfiguration, mainTable, subConfiguration));
		}
		return persisterPerSubclass;
	}
	
	private <D extends C> SimpleRelationalEntityPersister<D, I, T> buildSubclassPersister(Dialect dialect,
																				ConnectionConfiguration connectionConfiguration,
																				T mainTable,
																				SubEntityMappingConfiguration<D> subConfiguration) {
		// as a difference with other polymorphic cases, we don't use the following line to get the target table, but
		// only to ensure that configuration is right because it raises an exception if the user gave a column that is
		// not part of target table
		BeanMappingBuilder.giveTargetTable(subConfiguration.getPropertiesMapping(), mainTable);
		
		BeanMappingBuilder<D, T> beanMappingBuilder = new BeanMappingBuilder<>(subConfiguration.getPropertiesMapping(),
				mainTable,
				this.columnBinderRegistry,
				this.namingConfiguration.getColumnNamingStrategy());
		BeanMapping<D, T> beanMapping = beanMappingBuilder.build();
		Map<ReversibleAccessor<D, Object>, Column<T, Object>> subEntityPropertiesMapping = beanMapping.getMapping();
		Map<ReversibleAccessor<D, Object>, Column<T, Object>> subEntityReadonlyPropertiesMapping = beanMapping.getReadonlyMapping();
		ValueAccessPointMap<D, Converter<Object, Object>> subEntityPropertiesReadConverters = beanMapping.getReadConverters();
		ValueAccessPointMap<D, Converter<Object, Object>> subEntityPropertiesWriteConverters = beanMapping.getWriteConverters();
		// in single-table polymorphism, main properties must be given to sub-entities ones, because CRUD operations are dispatched to them
		// by a proxy and main persister is not so much used
		subEntityPropertiesMapping.putAll((Map) mainMapping);
		subEntityReadonlyPropertiesMapping.putAll((Map) mainReadonlyMapping);
		subEntityPropertiesReadConverters.putAll((Map) mainReadConverters);
		subEntityPropertiesWriteConverters.putAll((Map) mainWriteConverters);
		ClassMapping<D, I, T> classMappingStrategy = PersisterBuilderImpl.createClassMappingStrategy(
				true,    // given Identification (which is parent one) contains identifier policy
				mainTable,
				subEntityPropertiesMapping,
				subEntityReadonlyPropertiesMapping,
				subEntityPropertiesReadConverters,
				subEntityPropertiesWriteConverters,
				new ValueAccessPointSet<>(),    // TODO: implement properties set by constructor feature in single-table polymorphism
				(AbstractIdentification<D, I>) identification,
				subConfiguration.getPropertiesMapping().getBeanType(),
				null);
		// we need to copy also shadow columns, made in particular for one-to-one owned by source side because foreign key is maintained through it
		classMappingStrategy.addShadowColumns((ClassMapping) mainPersister.getMapping());
		
		// no primary key to add nor foreign key since table is the same as main one (single table strategy)
		return new SimpleRelationalEntityPersister<>(classMappingStrategy, dialect, connectionConfiguration);
	}
	
	@Override
	protected void assertSubPolymorphismIsSupported(PolymorphismPolicy<? extends C> subPolymorphismPolicy) {
		// Everything else than joined-tables is not implemented
		// - single-table with single-table needs analysis
		// - single-table with table-per-class is not implemented
		// Written as a negative condition to explicitly say what we support
		if (!(subPolymorphismPolicy instanceof PolymorphismPolicy.JoinTablePolymorphism)) {
			throw new NotImplementedException("Combining single-table polymorphism policy with " + Reflections.toString(subPolymorphismPolicy.getClass()));
		}
	}
	
	private Column<T, DTYPE> ensureDiscriminatorColumn() {
		Column<T, DTYPE> result = mainPersister.<T>getMapping().getTargetTable().addColumn(
				((SingleTablePolymorphism<C, DTYPE>) polymorphismPolicy).getDiscriminatorColumn(),
				((SingleTablePolymorphism<C, DTYPE>) polymorphismPolicy).getDiscrimintorType());
		result.setNullable(false);
		return result;
	}
	
}
