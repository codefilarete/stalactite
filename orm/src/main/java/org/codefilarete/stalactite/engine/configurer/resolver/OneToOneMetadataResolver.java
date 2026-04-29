package org.codefilarete.stalactite.engine.configurer.resolver;

import java.util.Set;

import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.ReadWritePropertyAccessPoint;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.JoinColumnNamingStrategy;
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
import org.codefilarete.stalactite.sql.ddl.structure.Key.KeyBuilder;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.Nullable;
import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.tool.Nullable.nullable;
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
			extracted(source.getEntity(), resolvedConfiguration.getMappingConfiguration());
		});
		// configuring one-to-ones owned by its ancestors
		source.<X>getAncestorSources()
				.forEach((Entity<X, I, ?> ancestor, Set<ResolvedConfiguration<X, I>> resolvedConfigurations) -> {
					resolvedConfigurations.forEach(resolvedConfiguration -> {
						extracted(ancestor, resolvedConfiguration.getMappingConfiguration());
					});
				});
	}
	
	private <C, I> void extracted(Entity<C, I, ?> entity, EntityMappingConfiguration<C, I> mappingConfiguration) {
		mappingConfiguration.getOneToOnes().forEach(oneToOne -> {
			this.resolve(entity, oneToOne);
		});
		// treating relations embedded in insets
		mappingConfiguration.getPropertiesMapping().getInsets().forEach(inset -> {
			inset.getConfigurationProvider().getConfiguration().getOneToOnes().forEach(oneToOne -> {
				this.resolve(entity, oneToOne.embedInto(inset.getAccessor()));
			});
		});
	}
	
	<SRC, TRGT, SRCID, TRGTID, SRCTABLE extends Table<SRCTABLE>, TRGTTABLE extends Table<TRGTTABLE>>
	void resolve(Entity<SRC, SRCID, SRCTABLE> source, OneToOneRelation<SRC, TRGT, TRGTID> oneToOne) {
		
		EntitySource<TRGT, TRGTID> targetEntitySource = buildTargetEntity(oneToOne);
		NamingConfiguration namingConfiguration = first(targetEntitySource.getResolvedConfigurations()).getNamingConfiguration();
		
		ReadWritePropertyAccessPoint<TRGT, SRC> reverseAccessPoint = Nullable.nullable(oneToOne.getReverseAccessor()).map(ValueAccessPointVariantSupport::getAccessor).get();
		DirectRelationJoin<SRCTABLE, TRGTTABLE, ?> tablesJoin = null;
		BeanRelationFixer<SRC, TRGT> relationFixer;
		Entity<TRGT, TRGTID, TRGTTABLE> targetEntity = targetEntitySource.getEntity();
		if (oneToOne.isRelationOwnedByTarget()) {
			// target owns the relation
			// we don't create foreign key for table-per-class because source columns should reference different tables (the on per entity) which databases do not allow
			boolean canCreateForeignKey = !source.isTablePerClass();
			if (canCreateForeignKey) {
				if (!targetEntity.isTablePerClass()) {
					OneToOneOwnedByTargetHelper<SRC, TRGT, SRCID, TRGTID, SRCTABLE, TRGTTABLE, SRCID> helper = new OneToOneOwnedByTargetHelper<>();
					ForeignKey<TRGTTABLE, SRCTABLE, SRCID> foreignKey = helper.determineForeignKeyColumns(oneToOne, source.getTable().getPrimaryKey(), targetEntity.getTable(), namingConfiguration.getJoinColumnNamingStrategy(), namingConfiguration.getForeignKeyNamingStrategy());
					tablesJoin = new DirectRelationJoin<>(foreignKey.toReferencedKey(), foreignKey);
				}
			} // else: creating foreign key is not possible, nothing special to do
			
			relationFixer = determineRelationFixer(oneToOne);
		} else {
			// source owns the relation
			OneToOneOwnedBySourceHelper<SRC, TRGT, SRCID, TRGTID, SRCTABLE, TRGTTABLE, TRGTID> helper = new OneToOneOwnedBySourceHelper<>();
			ForeignKey<SRCTABLE, TRGTTABLE, TRGTID> foreignKey = helper.determineForeignKeyColumns(oneToOne, source.getTable(), targetEntity.getTable().getPrimaryKey(), namingConfiguration.getJoinColumnNamingStrategy(), namingConfiguration.getForeignKeyNamingStrategy());
			tablesJoin = new DirectRelationJoin<>(foreignKey);
			
			relationFixer = BeanRelationFixer.of(oneToOne.getTargetProvider());
		}
		
		ResolvedOneToOneRelation<SRC, TRGT, SRCTABLE, TRGTTABLE, ?> entitiesLink = new ResolvedOneToOneRelation<>(
				targetEntity,
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
	
	private class OneToOneOwnedBySourceHelper<SRC, TRGT, SRCID, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID> {
		
		protected ForeignKey<LEFTTABLE, RIGHTTABLE, TRGTID> determineForeignKeyColumns(OneToOneRelation<SRC, TRGT, ?> oneToOneRelation,
		                                                                               LEFTTABLE leftTable,
		                                                                               PrimaryKey<RIGHTTABLE, TRGTID> rightPrimaryKey,
		                                                                               JoinColumnNamingStrategy joinColumnNamingStrategy,
		                                                                               ForeignKeyNamingStrategy foreignKeyNamingStrategy
		) {
			// adding foreign key constraint
			KeyBuilder<LEFTTABLE, TRGTID> leftKeyBuilder = Key.from(leftTable);
			AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(oneToOneRelation.getTargetProvider());
			rightPrimaryKey.getColumns().forEach(column -> {
				String effectiveLeftColumnName = nullable(oneToOneRelation.getColumnName()).elseSet(() -> joinColumnNamingStrategy.giveName(accessorDefinition, column)).get();
				Column<LEFTTABLE, ?> foreignKeyColumn = leftTable.addColumn(effectiveLeftColumnName, column.getJavaType());
				leftKeyBuilder.addColumn(foreignKeyColumn);
			});
			Key<LEFTTABLE, TRGTID> leftKey = leftKeyBuilder.build();
			
			// According to the nullable option, we specify the ddl schema option
			leftKey.getColumns().forEach(c -> ((Column) c).nullable(oneToOneRelation.isNullable()));
			
			String foreignKeyName = foreignKeyNamingStrategy.giveName(leftKey, rightPrimaryKey);
			return leftTable.addForeignKey(foreignKeyName, leftKey, rightPrimaryKey);
		}
		
	}
	
	private class OneToOneOwnedByTargetHelper<SRC, TRGT, SRCID, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>, JOINID> {
		
		protected ForeignKey<RIGHTTABLE, LEFTTABLE, SRCID> determineForeignKeyColumns(OneToOneRelation<SRC, TRGT, ?> oneToOneRelation,
		                                                                              PrimaryKey<LEFTTABLE, SRCID> leftPrimaryKey,
		                                                                              RIGHTTABLE rightTable,
		                                                                              JoinColumnNamingStrategy joinColumnNamingStrategy,
		                                                                              ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
			
			Column<RIGHTTABLE, SRCID> reverseColumn = oneToOneRelation.getReverseColumn();
			// small to check for incongruous reverse column definition: it can't be possible when key is composite
			if (reverseColumn != null && leftPrimaryKey.isComposed()) {
				throw new UnsupportedOperationException("Can't map composite primary key " + leftPrimaryKey + " on single reverse foreign key : " + reverseColumn);
			}
			
			// priority 1: take user definition of reverse column
			KeyBuilder<RIGHTTABLE, SRCID> rightKeyBuilder = Key.from(rightTable);
			if (reverseColumn == null) {
				String reverseColumnName = oneToOneRelation.getReverseColumnName();
				if (reverseColumnName != null) {
					Column<LEFTTABLE, SRCID> leftPKColumn = (Column<LEFTTABLE, SRCID>) first(leftPrimaryKey.getColumns());
					reverseColumn = rightTable.addColumn(reverseColumnName, leftPKColumn.getJavaType());
					rightKeyBuilder.addColumn(reverseColumn);
				}
			} else {
				rightKeyBuilder.addColumn(reverseColumn);
			}
			
			// priority 2: user didn't define reverse column, but we can guess it from the reverse accessor
			if (reverseColumn == null) {
				AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(oneToOneRelation.getTargetProvider());
				leftPrimaryKey.getColumns().forEach(pkColumn -> {
					String effectiveLeftColumnName = nullable(oneToOneRelation.getColumnName()).elseSet(() -> joinColumnNamingStrategy.giveName(accessorDefinition, pkColumn)).get();
					Column<RIGHTTABLE, ?> column = rightTable.addColumn(effectiveLeftColumnName, pkColumn.getJavaType());
					rightKeyBuilder.addColumn(column);
				});
			}
			
			// According to the nullable option, we specify the ddl schema option
			Key<RIGHTTABLE, SRCID> rightKey = rightKeyBuilder.build();
			if (oneToOneRelation.isNullable()) {
				rightKey.getColumns().forEach(c -> ((Column) c).nullable(true));
			}
			
			String foreignKeyName = foreignKeyNamingStrategy.giveName(rightKey, leftPrimaryKey);
			return rightTable.addForeignKey(foreignKeyName, rightKey, leftPrimaryKey);
		}
	}
}
