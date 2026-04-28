package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.Set;

import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.RelationalMappingConfiguration;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.engine.configurer.NamingConfiguration;
import org.codefilarete.stalactite.engine.configurer.ValueAccessPointVariantSupport;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.resolver.InheritanceConfigurationResolver.ResolvedConfiguration;
import org.codefilarete.stalactite.engine.configurer.resolver.MetadataSolvingCache.EntitySource;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.tool.collection.Iterables.first;

public class OneToOneMetadataResolver {
	
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	
	public OneToOneMetadataResolver(Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
	}
	
	<C, I, X> void resolve(EntitySource<C, I> source) {
		// configuring one-to-ones owned by this entity
		source.getResolvedConfigurations().forEach(resolvedConfiguration -> {
			resolvedConfiguration.getMappingConfiguration().getOneToOnes().forEach(oneToOne -> {
				Entity<C, I, ?> entity = source.getEntity();
				this.resolve(entity, oneToOne);
			});
		});
		// configuring one-to-ones owned by its ancestors
		source.<X>getAncestorSources()
				.forEach((Entity<X, I, ?> ancestor, Set<ResolvedConfiguration<X, I>> resolvedConfigurations) -> 
				resolvedConfigurations.stream().map(ResolvedConfiguration::getMappingConfiguration).map(RelationalMappingConfiguration::getOneToOnes)
				.forEach(relations -> relations.forEach(oneToOne -> this.resolve(ancestor, oneToOne))));
	}
	
	<SRC, TRGT, SRCID, TRGTID, SRCTABLE extends Table<SRCTABLE>, TRGTTABLE extends Table<TRGTTABLE>>
	void resolve(Entity<SRC, SRCID, SRCTABLE> source, OneToOneRelation<SRC, TRGT, TRGTID> oneToOne) {
		
		EntitySource<TRGT, TRGTID> targetEntitySource = buildTargetEntity(oneToOne);
		NamingConfiguration namingConfiguration = first(targetEntitySource.getResolvedConfigurations()).getNamingConfiguration();
		
		ReadWritePropertyAccessPoint<TRGT, SRC> reverseAccessPoint = Nullable.nullable(oneToOne.getReverseAccessor()).map(ValueAccessPointVariantSupport::getAccessor).get();
		DirectRelationJoin<SRCTABLE, TRGTTABLE, ?> tablesJoin = null;
		BeanRelationFixer<SRC, TRGT> relationFixer;
		if (oneToOne.isRelationOwnedByTarget()) {
			// source owns the relation
			PrimaryKey<SRCTABLE, SRCID> sourceEntityPk = source.getTable().getPrimaryKey();
			
			// we don't create foreign key for table-per-class because source columns should reference different tables (the on per entity) which databases do not allow
			boolean canCreateForeignKey = !source.isTablePerClass();
			if (canCreateForeignKey) {
				if (!targetEntitySource.getEntity().isTablePerClass()) {
					ForeignKey<TRGTTABLE, SRCTABLE, SRCID> foreignKey = projectPrimaryKey(sourceEntityPk, targetEntitySource.<TRGTTABLE>getEntity().getTable(), namingConfiguration.getForeignKeyNamingStrategy());
					tablesJoin = new DirectRelationJoin<>(foreignKey.asReferencedKey(), foreignKey);
				}
			} // else: creating foreign key is not possible, nothing special to do
			
			relationFixer = determineRelationFixer(oneToOne);
		} else {
			// source owns the relation
			PrimaryKey<TRGTTABLE, TRGTID> targetPrimaryKey = targetEntitySource.<TRGTTABLE>getEntity().getTable().getPrimaryKey();
			ForeignKey<SRCTABLE, TRGTTABLE, TRGTID> foreignKey = projectPrimaryKey(targetPrimaryKey, source.getTable(), namingConfiguration.getForeignKeyNamingStrategy());
			tablesJoin = new DirectRelationJoin<>(foreignKey);
			
			
			relationFixer = BeanRelationFixer.of(oneToOne.getTargetProvider());
		}
		
		ResolvedOneToOneRelation<SRC, TRGT, SRCTABLE, TRGTTABLE, ?> entitiesLink = new ResolvedOneToOneRelation<>(
				targetEntitySource.getEntity(),
				oneToOne.getTargetProvider(),
				reverseAccessPoint,
				oneToOne.getRelationMode(),
				oneToOne.isFetchSeparately(),
				tablesJoin,
				relationFixer,
				oneToOne.isRelationOwnedByTarget()
		);
		source.addRelation(entitiesLink);
	}
	
	private <SRC, TRGT, TRGTID> EntitySource<TRGT, TRGTID> buildTargetEntity(OneToOneRelation<SRC, TRGT, TRGTID> oneToOne) {
		InheritanceConfigurationResolver<TRGT, TRGTID> inheritanceConfigurationResolver = new InheritanceConfigurationResolver<>();
		KeepOrderSet<ResolvedConfiguration<?, TRGTID>> ancestorsConfigurations = inheritanceConfigurationResolver.resolveConfigurations(oneToOne.getTargetMappingConfiguration());
		
		InheritanceMetadataResolver<TRGT, TRGTID, ?> keyMappingApplier = new InheritanceMetadataResolver<>(dialect, connectionConfiguration);
		return keyMappingApplier.resolve(ancestorsConfigurations);
	}
	
	private <SRCTABLE extends Table<SRCTABLE>, TRGTTABLE extends Table<TRGTTABLE>, JOINTYPE>
	ForeignKey<TRGTTABLE, SRCTABLE, JOINTYPE> projectPrimaryKey(PrimaryKey<SRCTABLE, JOINTYPE> primaryKey, TRGTTABLE target, ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
		Key.KeyBuilder<TRGTTABLE, JOINTYPE> referencingKeyBuilder = Key.from(target);
		primaryKey.getColumns().forEach(pkColumn -> {
			// nullability = false may not be necessary because of primary key, left for principle
			Column<TRGTTABLE, ?> newColumn = target.addColumn(pkColumn.getName(), pkColumn.getJavaType(), pkColumn.getSize(), false);
			newColumn.primaryKey();
			referencingKeyBuilder.addColumn(newColumn);
		});
		Key<TRGTTABLE, JOINTYPE> referencingKey = referencingKeyBuilder.build();
		return target.addForeignKey(foreignKeyNamingStrategy.giveName(referencingKey, primaryKey), referencingKey, primaryKey);
	}
	
	<SRC, TRGT> BeanRelationFixer<SRC, TRGT> determineRelationFixer(OneToOneRelation<SRC, TRGT, ?> oneToOneRelation) {
		Mutator<SRC, TRGT> sourceIntoTargetFixer = oneToOneRelation.getTargetProvider();
		BeanRelationFixer<SRC, TRGT> result;
		
		if (oneToOneRelation.getReverseGetter() != null) {
			AccessorByMethodReference<TRGT, SRC> localReverseGetter = Accessors.accessorByMethodReference(oneToOneRelation.getReverseGetter());
			AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(localReverseGetter);
			
			// we take advantage of foreign key computing and presence of AccessorDefinition to build relation fixer which is needed lately in determineRelationFixer(..) 
			Mutator<TRGT, SRC> targetIntoSourceFixer = Accessors.mutatorByMethod(accessorDefinition.getDeclaringClass(), accessorDefinition.getName());
			result = (src, target) -> {
				// fixing source on target
				if (target != null) {    // prevent NullPointerException, actually means no linked entity (null relation), so nothing to do
					targetIntoSourceFixer.set(target, src);
				}
				// fixing target on source
				sourceIntoTargetFixer.set(src, target);
			};
		} else if (oneToOneRelation.getReverseSetter() != null) {
			// we take advantage of foreign key computing and presence of AccessorDefinition to build relation fixer which is needed lately in determineRelationFixer(..) 
			result = (target, input) -> {
				// fixing target on source side
				oneToOneRelation.getReverseSetter().set(input, target);
				// fixing source on target side
				sourceIntoTargetFixer.set(target, input);
			};
		} else {
			// non bidirectional relation : relation is owned by target without defining any way to fix it in memory
			// we can only fix target on source side
			result = sourceIntoTargetFixer::set;
		}
		
		return result;
	}
}
