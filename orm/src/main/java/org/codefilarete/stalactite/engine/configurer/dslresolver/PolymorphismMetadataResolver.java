package org.codefilarete.stalactite.engine.configurer.dslresolver;

import java.util.LinkedHashMap;
import java.util.Map;

import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.PolymorphismPolicy;
import org.codefilarete.stalactite.dsl.PolymorphismPolicy.JoinTablePolymorphism;
import org.codefilarete.stalactite.dsl.PolymorphismPolicy.SingleTablePolymorphism;
import org.codefilarete.stalactite.dsl.PolymorphismPolicy.TablePerClassPolymorphism;
import org.codefilarete.stalactite.dsl.subentity.SubEntityMappingConfiguration;
import org.codefilarete.stalactite.engine.configurer.dslresolver.InheritanceConfigurationResolver.ResolvedConfiguration;
import org.codefilarete.stalactite.engine.configurer.model.AbstractEntity.AbstractPropertyMapping;
import org.codefilarete.stalactite.engine.configurer.model.AbstractEntity.PropertyMapping;
import org.codefilarete.stalactite.engine.configurer.model.AbstractEntity.ReadOnlyPropertyMapping;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.EntityPolymorphism;
import org.codefilarete.stalactite.engine.configurer.model.Mapping;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.tool.Nullable.nullable;

public class PolymorphismMetadataResolver {
	
	private final Dialect dialect;
	
	public PolymorphismMetadataResolver(Dialect dialect) {
		this.dialect = dialect;
	}
	
	<C, I> EntityPolymorphism<C, I> resolve(ResolvedConfiguration<C, I> templateConfiguration, PolymorphismPolicy<C> polymorphismPolicy, Entity<C, I, ?> templateEntity) {
		// according to polymorphismPolicy type, create the right instances of EntityPolymorphism, fill them with the adhoc values
		EntityPolymorphism<C, I> result;
		if (polymorphismPolicy instanceof SingleTablePolymorphism) {
			result = buildSingleTablePolymorphism(templateConfiguration, (SingleTablePolymorphism<C, ?>) polymorphismPolicy, templateEntity);
		} else if (polymorphismPolicy instanceof JoinTablePolymorphism) {
			result = buildJoinTablePolymorphism(templateConfiguration, (JoinTablePolymorphism<C>) polymorphismPolicy);
		} else if (polymorphismPolicy instanceof TablePerClassPolymorphism) {
			result = buildTablePerClassPolymorphism(templateConfiguration, (TablePerClassPolymorphism<C>) polymorphismPolicy, templateEntity);
		} else {
			throw new UnsupportedOperationException("Unsupported polymorphism policy: " + polymorphismPolicy.getClass());
		}
		return result;
	}
	
	private <C, D extends C, I, DTYPE, T extends Table<T>>
	org.codefilarete.stalactite.engine.configurer.model.SingleTablePolymorphism<D, I, DTYPE, T>
	buildSingleTablePolymorphism(ResolvedConfiguration<C, I> templateConfiguration,
	                             SingleTablePolymorphism<C, DTYPE> policy,
	                             Entity<C, I, ?> templateEntity) {
		// The discriminator column lives on the entity's own table
		String discriminatorColumnName = policy.getDiscriminatorColumn();
		Column<T, DTYPE> discriminatorColumn = templateConfiguration.getTable().addColumn(discriminatorColumnName, policy.getDiscriminatorType());
		
		org.codefilarete.stalactite.engine.configurer.model.SingleTablePolymorphism<D, I, DTYPE, T> result =
				new org.codefilarete.stalactite.engine.configurer.model.SingleTablePolymorphism<>(discriminatorColumn);
		
		PropertyMappingResolver<D, T> propertyMappingResolver = new PropertyMappingResolver<>(dialect.getColumnBinderRegistry());
		
		policy.getSubClasses().forEach(subConfig -> {
			SubEntityMappingConfiguration<D> subEntityConfig = (SubEntityMappingConfiguration<D>) subConfig;
			Mapping<D, T> mapping = createMapping(templateConfiguration, subEntityConfig, (T) templateConfiguration.getTable(), propertyMappingResolver);
			
			// Adding template entity property mappings to the sub-entity to make it capable of filling
			// those properties from the template/trunk table columns (single table case)
			templateEntity.getMapping().getPropertyMappingHolder().getWritablePropertyToColumn().forEach(property -> {
					mapping.getPropertyMappingHolder().addMapping((AbstractPropertyMapping<D, ?, ?>) property);
			});
			templateEntity.getMapping().getPropertyMappingHolder().getReadonlyPropertyToColumn().forEach(property -> {
					mapping.getPropertyMappingHolder().addMapping((AbstractPropertyMapping<D, ?, ?>) property);
			});
			
			DTYPE discriminatorValue = policy.getDiscriminatorValue(subConfig.getEntityType());
			Entity<D, I, T> subEntity = new Entity<>(templateConfiguration.getIdentifierMapping(), mapping);
			result.addSubEntity(discriminatorValue, subEntity);
		});
		
		return result;
	}
	
	private <C, D extends C, I, T extends Table<T>, SUBTABLE extends Table<SUBTABLE>>
	org.codefilarete.stalactite.engine.configurer.model.JoinTablePolymorphism<C, I, T>
	buildJoinTablePolymorphism(ResolvedConfiguration<C, I> templateConfiguration,
	                           JoinTablePolymorphism<C> policy) {
		org.codefilarete.stalactite.engine.configurer.model.JoinTablePolymorphism<C, I, T> result =
				new org.codefilarete.stalactite.engine.configurer.model.JoinTablePolymorphism<>();
		
		PropertyMappingResolver<D, SUBTABLE> propertyMappingResolver = new PropertyMappingResolver<>(dialect.getColumnBinderRegistry());
		
		policy.getSubClasses().forEach(subConfig -> {
			SubEntityMappingConfiguration<D> subEntityConfig = (SubEntityMappingConfiguration<D>) subConfig;
			SUBTABLE subTable = (SUBTABLE) nullable(policy.giveTable(subEntityConfig))
					.getOr(() -> new Table<>(templateConfiguration.getNamingConfiguration().getTableNamingStrategy().giveName(subEntityConfig.getEntityType())));
			// TODO: take into account the overridden columns, or remove this feature in the DSL (we should check that JPA supports it or not)
//			SUBTABLE subTable = (SUBTABLE) nullable(tableDefinedByColumnOverride)
//					.elseSet(subTable)
//					.getOr(() -> new Table<>(configuration.getNamingConfiguration().getTableNamingStrategy().giveName(subEntityConfig.getEntityType())));
			
			// propagating parent class primaryKey to the subTable
			KeepOrderSet<Column<SUBTABLE, ?>> columns = templateConfiguration.getTable().<I>getPrimaryKey().getColumns();
			columns.forEach(column -> {
				subTable.addColumn(column.getName(), column.getJavaType(), column.getSize(), column.isNullable())
						.primaryKey();
			});
			
			// The join is from the sub-entity table FK → parent entity table PK
			DirectRelationJoin<T, SUBTABLE, I> join = new DirectRelationJoin<>(
					templateConfiguration.getTable().getPrimaryKey(),
					subTable.<I>getPrimaryKey());
			Mapping<D, SUBTABLE> mapping = createMapping(templateConfiguration, subEntityConfig, subTable, propertyMappingResolver);
			Entity<D, I, SUBTABLE> subEntity = new Entity<>(templateConfiguration.getIdentifierMapping(), mapping);
			result.addSubEntity(subEntityConfig.getEntityType(), subEntity, join);
		});
		
		return result;
	}
	
	private <C, D extends C, I, T extends Table<T>, SUBTABLE extends Table<SUBTABLE>, O>
	org.codefilarete.stalactite.engine.configurer.model.TablePerClassPolymorphism<C, I>
	buildTablePerClassPolymorphism(ResolvedConfiguration<C, I> templateConfiguration,
	                               TablePerClassPolymorphism<C> policy,
	                               Entity<C, I, ?> templateEntity) {
		Map<Class<? extends C>, Entity<? extends C, I, ?>> subEntities = new LinkedHashMap<>();
		PropertyMappingResolver<D, SUBTABLE> propertyMappingResolver = new PropertyMappingResolver<>(dialect.getColumnBinderRegistry());
		
		policy.getSubClasses().forEach(subConfig -> {
			SubEntityMappingConfiguration<D> subEntityConfig = (SubEntityMappingConfiguration<D>) subConfig;
			SUBTABLE subTable = (SUBTABLE) nullable(policy.giveTable(subEntityConfig))
					.getOr(() -> new Table<>(templateConfiguration.getNamingConfiguration().getTableNamingStrategy().giveName(subEntityConfig.getEntityType())));
			// propagating parent class primaryKey to the subTable
			KeepOrderSet<Column<SUBTABLE, ?>> columns = templateConfiguration.getTable().<I>getPrimaryKey().getColumns();
			columns.forEach(column -> {
				subTable.addColumn(column.getName(), column.getJavaType(), column.getSize(), column.isNullable())
						.primaryKey();
			});
			Mapping<D, SUBTABLE> mapping = createMapping(templateConfiguration, subEntityConfig, subTable, propertyMappingResolver);
			
			// copying parent class properties to the subTable (table-per-class case)
			templateEntity.getMapping().getPropertyMappingHolder().getWritablePropertyToColumn().forEach(property -> {
				PropertyMapping<C, O, T> castProperty = (PropertyMapping<C, O, T>) property;
				Column<T, O> templateColumn = castProperty.getColumn();
				Column<SUBTABLE, O> subtableColumn = subTable.addColumn(templateColumn.getName(), templateColumn.getJavaType(), templateColumn.getSize(), templateColumn.isNullable());
				mapping.getPropertyMappingHolder().addMapping(new PropertyMapping<>(
						(ReadWritePropertyAccessPoint<D, O>) castProperty.getAccessPoint(),
						subtableColumn,
						castProperty.isSetByConstructor(),
						castProperty.getReadConverter(),
						castProperty.getWriteConverter(),
						castProperty.isUnique()));
			});
			templateEntity.getMapping().getPropertyMappingHolder().getReadonlyPropertyToColumn().forEach(property -> {
				ReadOnlyPropertyMapping<C, O, T> castProperty = (ReadOnlyPropertyMapping<C, O, T>) property;
				Column<T, O> templateColumn = castProperty.getColumn();
				Column<SUBTABLE, O> subtableColumn = subTable.addColumn(templateColumn.getName(), templateColumn.getJavaType(), templateColumn.getSize(), templateColumn.isNullable());
				mapping.getPropertyMappingHolder().addMapping(new ReadOnlyPropertyMapping<>(
						(ReadWritePropertyAccessPoint<D, O>) castProperty.getAccessPoint(),
						subtableColumn,
						castProperty.isSetByConstructor(),
						castProperty.getReadConverter(),
						castProperty.isUnique()));
			});
			
			Entity<D, I, SUBTABLE> subEntity = new Entity<>(templateConfiguration.getIdentifierMapping(), mapping);
			subEntities.put(subConfig.getEntityType(), subEntity);
			
		});
		
		return new org.codefilarete.stalactite.engine.configurer.model.TablePerClassPolymorphism<>(subEntities);
	}
	
	private <C, D extends C, I, T extends Table<T>, SUBTABLE extends Table<SUBTABLE>> Mapping<D, SUBTABLE> createMapping(ResolvedConfiguration<C, I> configuration,
	                                                                                                                     SubEntityMappingConfiguration<D> subEntityConfig,
	                                                                                                                     SUBTABLE subTable,
	                                                                                                                     PropertyMappingResolver<D, SUBTABLE> propertyMappingResolver) {
		Mapping<D, SUBTABLE> mapping = new Mapping<>(subEntityConfig.getEntityType(), subTable);
		mapping.getPropertyMappingHolder().addMapping(propertyMappingResolver.resolve(subEntityConfig.getPropertiesMapping(), subTable, configuration.getNamingConfiguration().getColumnNamingStrategy()));
		return mapping;
	}
}
