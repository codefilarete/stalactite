package org.codefilarete.stalactite.engine.configurer.dslresolver;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.codefilarete.reflection.Accessor;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.reflection.Mutator;
import org.codefilarete.reflection.PropertyMutator;
import org.codefilarete.reflection.ReversibleMutator;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.naming.AssociationTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.AssociationTableNamingStrategy.ReferencedColumnNames;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.property.CascadeOptions;
import org.codefilarete.stalactite.engine.configurer.NamingConfiguration;
import org.codefilarete.stalactite.engine.configurer.dslresolver.InheritanceConfigurationResolver.ResolvedConfiguration;
import org.codefilarete.stalactite.engine.configurer.dslresolver.MetadataSolvingCache.EntitySource;
import org.codefilarete.stalactite.engine.configurer.model.DirectRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.Entity;
import org.codefilarete.stalactite.engine.configurer.model.IntermediaryRelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.RelationJoin;
import org.codefilarete.stalactite.engine.configurer.model.ResolvedOneToManyRelation;
import org.codefilarete.stalactite.engine.configurer.onetomany.OneToManyRelation;
import org.codefilarete.stalactite.engine.runtime.AssociationTable;
import org.codefilarete.stalactite.engine.runtime.IndexedAssociationTable;
import org.codefilarete.stalactite.sql.ConnectionConfiguration;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Key;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.result.BeanRelationFixer;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.bean.Objects;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.KeepOrderSet;

import static org.codefilarete.tool.Nullable.nullable;
import static org.codefilarete.tool.collection.Iterables.first;

public class OneToManyMetadataResolver {
	
	private final Dialect dialect;
	private final ConnectionConfiguration connectionConfiguration;
	
	public OneToManyMetadataResolver(Dialect dialect, ConnectionConfiguration connectionConfiguration) {
		this.dialect = dialect;
		this.connectionConfiguration = connectionConfiguration;
	}
	
	<C, I, X> void resolve(EntitySource<C, I> source) {
		// configuring one-to-manys owned by this entity
		source.getResolvedConfigurations().forEach(resolvedConfiguration -> {
			resolve(source.getEntity(), resolvedConfiguration.getMappingConfiguration());
		});
		// configuring one-to-manys owned by its ancestors
		source.<X>getAncestorSources()
				.forEach((Entity<X, I, ?> ancestor, Set<ResolvedConfiguration<X, I>> resolvedConfigurations) -> {
					resolvedConfigurations.forEach(resolvedConfiguration -> {
						resolve(ancestor, resolvedConfiguration.getMappingConfiguration());
					});
				});
	}
	
	private <C, I> void resolve(Entity<C, I, ?> entity, EntityMappingConfiguration<C, I> mappingConfiguration) {
		KeepOrderSet<EntitySource<?, ?>> targetEntities = new KeepOrderSet<>();
		mappingConfiguration.getOneToManys().forEach(oneToMany -> {
			EntitySource<Object, Object> resolve = this.resolve(entity, oneToMany);
			targetEntities.add(resolve);
		});
		// treating relations embedded in insets
		mappingConfiguration.getPropertiesMapping().getInsets().forEach(inset -> {
			inset.getConfigurationProvider().getConfiguration().getOneToManys().forEach(oneToMany -> {
				EntitySource<Object, Object> resolve = this.resolve(entity, oneToMany.embedInto(inset.getAccessor(), inset.getEmbeddedClass()));
				targetEntities.add(resolve);
			});
		});
		
		// we go deeper on each target entity to resolve its relations: breadth-first algorithm
		targetEntities.forEach(this::resolve);
	}
	
	<SRC, TRGT, S extends Collection<TRGT>, SRCID, TRGTID, SRCTABLE extends Table<SRCTABLE>, TRGTTABLE extends Table<TRGTTABLE>>
	EntitySource<TRGT, TRGTID> resolve(Entity<SRC, SRCID, SRCTABLE> source, OneToManyRelation<SRC, TRGT, TRGTID, S> oneToMany) {

		EntitySource<TRGT, TRGTID> targetEntitySource = buildTargetEntity(oneToMany);
		NamingConfiguration namingConfiguration = first(targetEntitySource.getResolvedConfigurations()).getNamingConfiguration();
		
		PropertyMutator<TRGT, SRC> reverseAccessPoint = oneToMany.giveReverseSetter();
		RelationJoin tablesJoin = null;
		BeanRelationFixer<SRC, TRGT> relationFixer;
		Entity<TRGT, TRGTID, TRGTTABLE> targetEntity = targetEntitySource.getEntity();
		AccessorDefinition collectionAccessorDefinition = AccessorDefinition.giveDefinition(oneToMany.getCollectionAccessor());
		
		Supplier<S> collectionFactory = oneToMany.getCollectionFactory();
		if (collectionFactory == null) {
			collectionFactory = Reflections.giveCollectionFactory((Class<S>) collectionAccessorDefinition.getMemberType());
		}
		
		AccessorDefinition accessorDefinitionForTableNaming = new AccessorDefinition(
				collectionAccessorDefinition.getDeclaringClass(),
				collectionAccessorDefinition.getName(),
				// we prefer the target persister type to method reference member type because the latter only gets the collection type which is not
				// valuable information for table / column naming
				oneToMany.getTargetMappingConfiguration().getEntityType());
		
		Column<TRGTTABLE, Integer> indexingColumn = null;
		if (oneToMany.isOwnedByReverseSide()) {
			// target owns the relation
			// we don't create foreign key for table-per-class because source columns should reference different tables (the on per entity) which databases do not allow
			boolean canCreateForeignKey = !source.isTablePerClass();
			if (canCreateForeignKey) {
				if (!targetEntity.isTablePerClass()) {
					OneToManyOwnedByTargetHelper<SRC, TRGT, SRCID, TRGTID, SRCTABLE, TRGTTABLE> helper = new OneToManyOwnedByTargetHelper<>();
					tablesJoin = helper.determineJoin(oneToMany, source.getTable().getPrimaryKey(), targetEntity.getTable(), namingConfiguration.getJoinColumnNamingStrategy(), namingConfiguration.getForeignKeyNamingStrategy());
				}
			} // else: creating foreign key is not possible, nothing special to do
			
			Mutator<TRGT, SRC> NOOP_REVERSE_SETTER = (o, i) -> {};
			relationFixer = BeanRelationFixer.of(oneToMany.getCollectionAccessor(), collectionFactory, Objects.preventNull(oneToMany.getReverseLink(), NOOP_REVERSE_SETTER));
			
			if (oneToMany.isOrdered()) {
				indexingColumn = oneToMany.getIndexingColumn();
				if (indexingColumn == null) {
					
					String indexingColumnName = nullable(oneToMany.getIndexingColumnName()).getOr(() -> namingConfiguration.getIndexColumnNamingStrategy().giveName(accessorDefinitionForTableNaming));
					Class indexColumnType = oneToMany.getRelationMode() == CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL
							? int.class
							: Integer.class;	// column must be nullable since row won't be deleted through orphan removal but only "detached" from parent row
					indexingColumn = targetEntity.getTable().addColumn(indexingColumnName, indexColumnType);
				}
			}
			
		} else {
			// an association table is necessary to link source and target entities
			// we don't create foreign key for table-per-class because source columns should reference different tables (the on per entity) which databases do not allow
			boolean canCreateForeignKey = !source.isTablePerClass();
			if (canCreateForeignKey) {
				if (!targetEntity.isTablePerClass()) {
					OneToManyWithAssociationTableHelper<SRC, TRGT, SRCID, TRGTID, SRCTABLE, TRGTTABLE, ?> helper = new OneToManyWithAssociationTableHelper<>(accessorDefinitionForTableNaming);
					tablesJoin = helper.determineJoin(
							oneToMany,
							source.getTable().getPrimaryKey(),
							targetEntity.getTable(),
							namingConfiguration.getAssociationTableNamingStrategy(),
							namingConfiguration.getForeignKeyNamingStrategy(),
							namingConfiguration.getIndexColumnNamingStrategy());
				}
			} // else: creating foreign key is not possible, nothing special to do
			
			Mutator<TRGT, SRC> NOOP_REVERSE_SETTER = (o, i) -> {};
			relationFixer = BeanRelationFixer.of(oneToMany.getCollectionAccessor(), collectionFactory, Objects.preventNull(oneToMany.getReverseLink(), NOOP_REVERSE_SETTER));
			
			if (oneToMany.isOrdered()) {
				indexingColumn = oneToMany.getIndexingColumn();
				if (indexingColumn == null) {
					
					String indexingColumnName = nullable(oneToMany.getIndexingColumnName()).getOr(() -> namingConfiguration.getIndexColumnNamingStrategy().giveName(accessorDefinitionForTableNaming));
					Class indexColumnType = oneToMany.getRelationMode() == CascadeOptions.RelationMode.ALL_ORPHAN_REMOVAL
							? int.class
							: Integer.class;	// column must be nullable since row won't be deleted through orphan removal but only "detached" from parent row
					indexingColumn = targetEntity.getTable().addColumn(indexingColumnName, indexColumnType);
				}
			}
		}
		
		ResolvedOneToManyRelation<SRC, TRGT, S, SRCID, TRGTID, SRCTABLE, TRGTTABLE> entitiesLink = new ResolvedOneToManyRelation<>(
				targetEntity,
				oneToMany.getCollectionAccessor(),
				oneToMany.getRelationMode(),
				oneToMany.isOwnedByReverseSide(),
				reverseAccessPoint,
				oneToMany.isFetchSeparately(),
				tablesJoin,
				relationFixer,
				collectionFactory,
				indexingColumn
		);
		source.addRelation(entitiesLink);

		return targetEntitySource;
	}
	
	private <SRC, TRGT, S extends Collection<TRGT>, TRGTID> EntitySource<TRGT, TRGTID>
	buildTargetEntity(OneToManyRelation<SRC, TRGT, TRGTID, S> oneToMany) {
		InheritanceConfigurationResolver<TRGT, TRGTID> inheritanceConfigurationResolver = new InheritanceConfigurationResolver<>();
		KeepOrderSet<ResolvedConfiguration<?, TRGTID>> ancestorsConfigurations = inheritanceConfigurationResolver.resolveConfigurations(oneToMany.getTargetMappingConfiguration());
		
		InheritanceMetadataResolver<TRGT, TRGTID, ?> keyMappingApplier = new InheritanceMetadataResolver<>(dialect, connectionConfiguration);
		return keyMappingApplier.resolve(ancestorsConfigurations);
	}
	
	private static class OneToManyOwnedByTargetHelper<SRC, TRGT, SRCID, TRGTID, LEFTTABLE extends Table<LEFTTABLE>, RIGHTTABLE extends Table<RIGHTTABLE>> {
		
		protected RelationJoin determineJoin(OneToManyRelation<SRC, TRGT, TRGTID, ?> relation,
		                                     PrimaryKey<LEFTTABLE, SRCID> leftPrimaryKey,
		                                     RIGHTTABLE targetTable,
		                                     JoinColumnNamingStrategy joinColumnNamingStrategy,
		                                     ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
			ForeignKey<RIGHTTABLE, LEFTTABLE, SRCID> foreignKey = determineForeignKeyColumns(relation, leftPrimaryKey, targetTable, joinColumnNamingStrategy, foreignKeyNamingStrategy);
			
			return new DirectRelationJoin<>(foreignKey.getReferencedKey(), foreignKey);
		}
		
		private ForeignKey<RIGHTTABLE, LEFTTABLE, SRCID> determineForeignKeyColumns(OneToManyRelation<SRC, TRGT, TRGTID, ?> relation,
		                                                                              PrimaryKey<LEFTTABLE, SRCID> leftPrimaryKey,
		                                                                              RIGHTTABLE targetTable,
		                                                                              JoinColumnNamingStrategy joinColumnNamingStrategy,
		                                                                              ForeignKeyNamingStrategy foreignKeyNamingStrategy) {
			Key.KeyBuilder<RIGHTTABLE, SRCID> foreignKeyBuilder = Key.from(targetTable);
			if (!relation.getForeignKeyNameMapping().isEmpty()) {
				Map<Accessor<SRCID, ?>, Column<RIGHTTABLE, ?>> foreignKeyColumnMapping = new HashMap<>();
				relation.getForeignKeyNameMapping().forEach((valueAccessPoint, colName) -> {
					AccessorDefinition localAccessorDefinition = AccessorDefinition.giveDefinition(valueAccessPoint);
					Accessor<SRCID, Object> accessor = valueAccessPoint instanceof Accessor
							? (Accessor) valueAccessPoint
							: (valueAccessPoint instanceof ReversibleMutator ? ((ReversibleMutator) valueAccessPoint).toAccessor() : null);
					if (accessor == null) {
						throw new UnsupportedOperationException("Can't get accessor from " + valueAccessPoint);
					}
					Column<RIGHTTABLE, ?> column = targetTable.addColumn(colName, localAccessorDefinition.getMemberType());
					foreignKeyBuilder.addColumn(column);
					foreignKeyColumnMapping.put(accessor, column);
				});
			} else if (!relation.getForeignKeyColumnMapping().isEmpty()) {
				Map<Accessor<SRCID, ?>, Column<RIGHTTABLE, ?>> foreignKeyColumnMapping = new HashMap<>();
				relation.getForeignKeyColumnMapping().forEach((valueAccessPoint, column) -> {
					Accessor<SRCID, Object> accessor = valueAccessPoint instanceof Accessor
							? (Accessor) valueAccessPoint
							: (valueAccessPoint instanceof ReversibleMutator ? ((ReversibleMutator) valueAccessPoint).toAccessor() : null);
					if (accessor == null) {
						throw new UnsupportedOperationException("Can't get accessor from " + valueAccessPoint);
					}
					foreignKeyBuilder.addColumn((Column<RIGHTTABLE, ?>) column);
					foreignKeyColumnMapping.put(accessor, (Column<RIGHTTABLE, ?>) column);
				});
			} else if (relation.getReverseColumnName() != null || relation.getReverseColumn() != null) {
				Column<RIGHTTABLE, ?> reverseColumn;
				if (relation.getReverseColumnName() != null) {
					PrimaryKey<LEFTTABLE, SRCID> srcPrimaryKey = leftPrimaryKey;
					// with a reverse column name, the primary key should be a single column key
					if (srcPrimaryKey.isComposed()) {
						throw new MappingConfigurationException("Giving reverse column whereas the primary key is composed :"
								+ " primary key = [" + srcPrimaryKey.getColumns().stream().map(Column::getName).collect(Collectors.joining(", ")) + "] vs "
								+ " reverse column = " + relation.getReverseColumnName());
					}
					Column<LEFTTABLE, ?> pk = first(srcPrimaryKey.getColumns());
					reverseColumn = targetTable.addColumn(
							relation.getReverseColumnName(),
							pk.getJavaType(),
							pk.getSize());
				} else {
					reverseColumn = (Column<RIGHTTABLE, ?>) relation.getReverseColumn();
				}
				foreignKeyBuilder.addColumn(reverseColumn);
			} else if (relation.giveReverseSetter() != null) {
				// Please note that here Setter is only used to get foreign key column names, not to set values on reverse side
				Map<Column<LEFTTABLE, ?>, Column<RIGHTTABLE, ?>> foreignKeyColumnMapping = new HashMap<>();
				PrimaryKey<LEFTTABLE, SRCID> primaryKey = leftPrimaryKey;
				AccessorDefinition reverseGetterDefinition = AccessorDefinition.giveDefinition(relation.giveReverseSetter());
				primaryKey.getColumns().forEach(pkColumn -> {
					String colName = joinColumnNamingStrategy.giveName(reverseGetterDefinition, pkColumn);
					Column<RIGHTTABLE, ?> fkColumn = targetTable.addColumn(colName, pkColumn.getJavaType(), pkColumn.getSize(), null);
					foreignKeyBuilder.addColumn(fkColumn);
					foreignKeyColumnMapping.put(pkColumn, fkColumn);
				});
			} // else : no reverse side mapped, this case can't happen since OneToManyWithMappedAssociationConfigurer is only
			  // invoked when reverse side is mapped (see OneToManyRelation.isOwnedByReverseSide())
			Key<RIGHTTABLE, SRCID> foreignKey;
			foreignKey = foreignKeyBuilder.build();
			if (relation.isReverseAsMandatory() != null && relation.isReverseAsMandatory()) {
				foreignKey.getColumns().stream().map(Column.class::cast).forEach(Column::notNull);
			}
			return targetTable.addForeignKey(foreignKeyNamingStrategy.giveName(foreignKey, leftPrimaryKey), foreignKey, leftPrimaryKey);
		}
		
	}
	
	private static class OneToManyWithAssociationTableHelper<
			SRC, TRGT, SRCID, TRGTID,
			LEFTTABLE extends Table<LEFTTABLE>,
			RIGHTTABLE extends Table<RIGHTTABLE>,
			ASSOCIATIONTABLE extends AssociationTable<ASSOCIATIONTABLE, LEFTTABLE, RIGHTTABLE, SRCID, TRGTID>> {
		
		private final AccessorDefinition accessorDefinitionForTableNaming;
		
		public OneToManyWithAssociationTableHelper(AccessorDefinition accessorDefinitionForTableNaming) {
			this.accessorDefinitionForTableNaming = accessorDefinitionForTableNaming;
		}
		
		protected RelationJoin determineJoin(OneToManyRelation<SRC, TRGT, TRGTID, ?> relation,
		                                     PrimaryKey<LEFTTABLE, SRCID> leftPrimaryKey,
		                                     RIGHTTABLE targetTable,
		                                     AssociationTableNamingStrategy associationTableNamingStrategy,
		                                     ForeignKeyNamingStrategy foreignKeyNamingStrategy,
		                                     ColumnNamingStrategy indexColumnNamingStrategy
		) {
			// we don't create foreign key for table-per-class because source columns should reference different tables (the one
			// per entity) which databases do not allow
			boolean createOneSideForeignKey = !relation.isSourceTablePerClassPolymorphic();
			boolean createManySideForeignKey = !relation.isTargetTablePerClassPolymorphic();
			PrimaryKey<RIGHTTABLE, TRGTID> rightPrimaryKey = targetTable.getPrimaryKey();
			ReferencedColumnNames<LEFTTABLE, RIGHTTABLE> columnNames = associationTableNamingStrategy.giveColumnNames(
					accessorDefinitionForTableNaming,
					leftPrimaryKey,
					rightPrimaryKey);
			if (relation.getSourceJoinColumnName() != null) {
				columnNames.setLeftColumnName(Iterables.first(leftPrimaryKey.getColumns()), relation.getSourceJoinColumnName());
			}
			if (relation.getSourceJoinColumnName() != null) {
				columnNames.setRightColumnName(Iterables.first(rightPrimaryKey.getColumns()), relation.getTargetJoinColumnName());
			}
			String associationTableName = nullable(relation.getAssociationTableName()).getOr(() -> associationTableNamingStrategy.giveName(accessorDefinitionForTableNaming,
					leftPrimaryKey, rightPrimaryKey));
			
			ASSOCIATIONTABLE intermediaryTable;
			if (relation.isOrdered()) {
				String indexingColumnName = nullable(relation.getIndexingColumnName()).getOr(() -> indexColumnNamingStrategy.giveName(accessorDefinitionForTableNaming));
				
				intermediaryTable = (ASSOCIATIONTABLE) new IndexedAssociationTable<>(
						leftPrimaryKey.getTable().getSchema(),
						associationTableName,
						leftPrimaryKey,
						rightPrimaryKey,
						columnNames,
						foreignKeyNamingStrategy,
						createOneSideForeignKey,
						createManySideForeignKey,
						indexingColumnName
				);
			} else {
				
				intermediaryTable = (ASSOCIATIONTABLE) new AssociationTable<>(
						leftPrimaryKey.getTable().getSchema(),
						associationTableName,
						leftPrimaryKey,
						rightPrimaryKey,
						columnNames,
						foreignKeyNamingStrategy,
						createOneSideForeignKey,
						createManySideForeignKey
				);
			}
			
			return new IntermediaryRelationJoin<>(intermediaryTable);
		}
	}
	
}
