package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.LinkedHashMap;
import java.util.Map;

import org.codefilarete.stalactite.dsl.PolymorphismPolicy;
import org.codefilarete.stalactite.dsl.PolymorphismPolicy.JoinTablePolymorphism;
import org.codefilarete.stalactite.dsl.PolymorphismPolicy.SingleTablePolymorphism;
import org.codefilarete.stalactite.dsl.PolymorphismPolicy.TablePerClassPolymorphism;
import org.codefilarete.stalactite.dsl.subentity.SubEntityMappingConfiguration;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.EntityPolymorphism;
import org.codefilarete.stalactite.engine.configurer.model.Mapping;
import org.codefilarete.stalactite.engine.configurer.resolver.InheritanceMappingResolver.ResolvedConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;

import static org.codefilarete.tool.Nullable.nullable;

public class PolymorphismMetadataResolver {
	
	private final Dialect dialect;
	
	public PolymorphismMetadataResolver(Dialect dialect) {
		this.dialect = dialect;
	}
	
	<C, I> EntityPolymorphism<C, I> resolve(ResolvedConfiguration<C, I> configuration, PolymorphismPolicy<C> polymorphismPolicy) {
		// according to polymorphismPolicy type, create the right instances of EntityPolymorphism, fill them with the adhoc values
		EntityPolymorphism<C, I> result;
		if (polymorphismPolicy instanceof SingleTablePolymorphism) {
			result = buildSingleTablePolymorphism(configuration, (SingleTablePolymorphism<C, ?>) polymorphismPolicy);
		} else if (polymorphismPolicy instanceof JoinTablePolymorphism) {
			result = buildJoinTablePolymorphism(configuration, (JoinTablePolymorphism<C>) polymorphismPolicy);
		} else if (polymorphismPolicy instanceof TablePerClassPolymorphism) {
			result = buildTablePerClassPolymorphism(configuration, (TablePerClassPolymorphism<C>) polymorphismPolicy);
		} else {
			throw new UnsupportedOperationException("Unsupported polymorphism policy: " + polymorphismPolicy.getClass());
		}
		return result;
	}
	
	private <C, D extends C, I, DTYPE, T extends Table<T>> org.codefilarete.stalactite.engine.configurer.model.SingleTablePolymorphism<D, I, DTYPE, T>
	buildSingleTablePolymorphism(ResolvedConfiguration<C, I> configuration,
	                             SingleTablePolymorphism<C, DTYPE> policy) {
		// The discriminator column lives on the entity's own table
		String discriminatorColumnName = policy.getDiscriminatorColumn();
		Column<T, DTYPE> discriminatorColumn = configuration.getTable().addColumn(discriminatorColumnName, policy.getDiscrimintorType());
		
		org.codefilarete.stalactite.engine.configurer.model.SingleTablePolymorphism<D, I, DTYPE, T> result =
				new org.codefilarete.stalactite.engine.configurer.model.SingleTablePolymorphism<>(discriminatorColumn);
		
		PropertyMappingResolver<D, T> propertyMappingResolver = new PropertyMappingResolver<>(dialect.getColumnBinderRegistry());
		
		policy.getSubClasses().forEach(subConfig -> {
			SubEntityMappingConfiguration<D> subEntityConfig = (SubEntityMappingConfiguration<D>) subConfig;
			Mapping<D, T> mapping = createMapping(configuration, subEntityConfig, (T) configuration.getTable(), propertyMappingResolver);
			
			DTYPE discriminatorValue = policy.getDiscriminatorValue(subConfig.getEntityType());
			result.addSubEntity(discriminatorValue, mapping);
		});
		
		return result;
	}
	
	private <C, D extends C, I, T extends Table<T>, SUBTABLE extends Table<SUBTABLE>> org.codefilarete.stalactite.engine.configurer.model.JoinTablePolymorphism<C, I, T>
	buildJoinTablePolymorphism(ResolvedConfiguration<C, I> configuration,
	                           JoinTablePolymorphism<C> policy) {
		org.codefilarete.stalactite.engine.configurer.model.JoinTablePolymorphism<C, I, T> result =
				new org.codefilarete.stalactite.engine.configurer.model.JoinTablePolymorphism<>();
		
		PropertyMappingResolver<D, T> propertyMappingResolver = new PropertyMappingResolver<>(dialect.getColumnBinderRegistry());
		
		policy.getSubClasses().forEach(subConfig -> {
			SubEntityMappingConfiguration<D> subEntityConfig = (SubEntityMappingConfiguration<D>) subConfig;
			SUBTABLE subTable = (SUBTABLE) nullable(policy.giveTable(subEntityConfig))
					.getOr(() -> new Table<>(configuration.getNamingConfiguration().getTableNamingStrategy().giveName(subEntityConfig.getEntityType())));
			// TODO: take into account the overridden columns, or remove this feature in the DSL (we should check that JPA supports it or not)
//			SUBTABLE subTable = (SUBTABLE) nullable(tableDefinedByColumnOverride)
//					.elseSet(subTable)
//					.getOr(() -> new Table<>(configuration.getNamingConfiguration().getTableNamingStrategy().giveName(subEntityConfig.getEntityType())));
			
			// The join is from the sub-entity table FK → parent entity table PK
			DirectRelationJoin<T, SUBTABLE, I> join = new DirectRelationJoin<>(
					configuration.getTable().getPrimaryKey(),
					subTable.<I>getPrimaryKey());
			Mapping<D, SUBTABLE> mapping = createMapping(configuration, subEntityConfig, subTable, propertyMappingResolver);
			result.addSubEntity(subEntityConfig.getEntityType(), mapping, join);
		});
		
		return result;
	}
	
	private <C, D extends C, I, T extends Table<T>, SUBTABLE extends Table<SUBTABLE>> org.codefilarete.stalactite.engine.configurer.model.TablePerClassPolymorphism<C, I>
	buildTablePerClassPolymorphism(ResolvedConfiguration<C, I> configuration,
	                               TablePerClassPolymorphism<C> policy) {
		Map<Class<? extends C>, Mapping<? extends C, ?>> subEntities = new LinkedHashMap<>();
		PropertyMappingResolver<D, T> propertyMappingResolver = new PropertyMappingResolver<>(dialect.getColumnBinderRegistry());
		
		policy.getSubClasses().forEach(subConfig -> {
			SubEntityMappingConfiguration<D> subEntityConfig = (SubEntityMappingConfiguration<D>) subConfig;
			SUBTABLE subTable = (SUBTABLE) nullable(policy.giveTable(subEntityConfig))
					.getOr(() -> new Table<>(configuration.getNamingConfiguration().getTableNamingStrategy().giveName(subEntityConfig.getEntityType())));
			Mapping<D, SUBTABLE> mapping = createMapping(configuration, subEntityConfig, subTable, propertyMappingResolver);
			subEntities.put(subConfig.getEntityType(), mapping);
		});
		
		return new org.codefilarete.stalactite.engine.configurer.model.TablePerClassPolymorphism<>(subEntities);
	}
	
	private <C, D extends C, I, T extends Table<T>, SUBTABLE extends Table<SUBTABLE>> Mapping<D, SUBTABLE> createMapping(ResolvedConfiguration<C, I> configuration,
	                                                                                                                     SubEntityMappingConfiguration<D> subEntityConfig,
	                                                                                                                     SUBTABLE subTable,
	                                                                                                                     PropertyMappingResolver<D, T> propertyMappingResolver) {
		Mapping<D, SUBTABLE> mapping = new Mapping<>(subEntityConfig.getEntityType(), subTable);
		mapping.getPropertyMappingHolder().addMapping(propertyMappingResolver.resolve(subEntityConfig.getPropertiesMapping(), (T) configuration.getTable(), configuration.getNamingConfiguration().getColumnNamingStrategy()));
		return mapping;
	}
}
