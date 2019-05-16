package org.gama.stalactite.persistence.engine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.reflection.AccessorByMethodReference;
import org.gama.reflection.Accessors;
import org.gama.reflection.IReversibleAccessor;
import org.gama.reflection.MutatorByMethodReference;
import org.gama.reflection.PropertyAccessor;
import org.gama.sql.ConnectionProvider;
import org.gama.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.persistence.engine.FluentEmbeddableMappingConfigurationSupport.AbstractInset;
import org.gama.stalactite.persistence.engine.FluentEntityMappingConfigurationSupport.EntityLinkageByColumnName;
import org.gama.stalactite.persistence.engine.builder.CascadeMany;
import org.gama.stalactite.persistence.engine.cascade.JoinedTablesPersister;
import org.gama.stalactite.persistence.engine.model.City;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.id.Identified;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.ForeignKey;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * @author Guillaume Mary
 */
class CascadeOneConfigurerTest {
	
//	@Test
//	void tableStructure() {
//		// defining Country mapping
//		Table<?> countryTable = new Table<>("country");
//		Column countryTableIdColumn = countryTable.addColumn("id", long.class).primaryKey();
//		Column countryTableNameColumn = countryTable.addColumn("name", String.class);
//		Column countryTableCapitalColumn = countryTable.addColumn("capitalId", long.class);
//		Map<IReversibleAccessor, Column> countryMapping = Maps
//				.asMap((IReversibleAccessor) new PropertyAccessor<>(new AccessorByMethodReference<>(Country::getId), Accessors.mutatorByField(Country.class, "id")), countryTableIdColumn)
//				.add(new PropertyAccessor<>(new AccessorByMethodReference<>(Country::getName), new MutatorByMethodReference<>(Country::setName)), countryTableNameColumn)
//				.add(new PropertyAccessor<>(new AccessorByMethodReference<>(Country::getCapital), new MutatorByMethodReference<>(Country::setCapital)), countryTableCapitalColumn);
//		IReversibleAccessor<Country, Identifier<Long>> countryIdentifierAccessorByMethodReference = new PropertyAccessor<>(
//				Accessors.accessorByMethodReference(Country::getId),
//				Accessors.mutatorByField(Country.class, "id")
//		);
//		ClassMappingStrategy<Country, Identifier<Long>, Table> countryClassMappingStrategy = new ClassMappingStrategy<Country, Identifier<Long>, Table>(Country.class, countryTable,
//				(Map) countryMapping, countryIdentifierAccessorByMethodReference, (IdentifierInsertionManager) new AlreadyAssignedIdentifierManager<Country, Identifier>(Identifier.class));
//		
//		// defining City mapping
//		Table<?> cityTable = new Table<>("city");
//		Column cityTableIdColumn = cityTable.addColumn("id", long.class).primaryKey();
//		IReversibleAccessor<City, Identifier<Long>> cityIdentifierAccessorByMethodReference = new PropertyAccessor<>(
//				Accessors.accessorByMethodReference(City::getId),
//				Accessors.mutatorByField(City.class, "id")
//		);
//		// defining Country -> City relation through capital property
//		PropertyAccessor<Country, City> capitalAccessPoint = new PropertyAccessor<>(new AccessorByMethodReference<>(Country::getCapital),
//				new MutatorByMethodReference<>(Country::setCapital));
//		CascadeOne<Country, City, Identifier<Long>> countryCapitalRelation = new CascadeOne<>(capitalAccessPoint, new EntityMappingConfiguration<City, Identifier<Long>>() {
//			@Override
//			public Class<City> getPersistedClass() {
//				return City.class;
//			}
//			
//			@Override
//			public TableNamingStrategy getTableNamingStrategy() {
//				return TableNamingStrategy.DEFAULT;
//			}
//			
//			@Override
//			public IReversibleAccessor getIdentifierAccessor() {
//				return cityIdentifierAccessorByMethodReference;
//			}
//			
//			@Override
//			public IdentifierInsertionManager<City, Identifier<Long>> getIdentifierInsertionManager() {
//				return (IdentifierInsertionManager) new AlreadyAssignedIdentifierManager<City, Identifier>(Identifier.class);
//			}
//			
//			@Override
//			public EmbeddableMappingConfiguration<City> getPropertiesMapping() {
//				return new EmbeddableMappingConfiguration<City>() {
//					@Override
//					public EmbeddableMappingConfiguration<? super City> getMappedSuperClassConfiguration() {
//						return null;
//					}
//					
//					@Override
//					public List<Linkage> getPropertiesMapping() {
//						EntityLinkageByColumnName identifierLinkage = new EntityLinkageByColumnName<>(
//								new PropertyAccessor<>(new AccessorByMethodReference<>(City::getId), Accessors.mutatorByField(City.class, "id")),
//								Identifier.class,
//								"id"
//						);
//						identifierLinkage.primaryKey();
//						EntityLinkageByColumnName nameLinkage = new EntityLinkageByColumnName<>(
//								new PropertyAccessor<>(new AccessorByMethodReference<>(City::getName), Accessors.mutatorByField(City.class, "name")),
//								String.class,
//								"name"
//						);
//						return Arrays.asList(identifierLinkage, nameLinkage);
//					}
//					
//					@Override
//					public Collection<AbstractInset<City, ?>> getInsets() {
//						return new ArrayList<>();
//					}
//					
//					@Override
//					public ColumnNamingStrategy getColumnNamingStrategy() {
//						return ColumnNamingStrategy.DEFAULT;
//					}
//				};
//			}
//			
//			@Override
//			public List<CascadeOne<City, ?, ?>> getOneToOnes() {
//				return new ArrayList<>();
//			}
//			
//			@Override
//			public List<CascadeMany<City, ?, ?, ? extends Collection>> getOneToManys() {
//				return new ArrayList<>();
//			}
//			
//			@Override
//			public VersioningStrategy getOptimisticLockOption() {
//				return null;
//			}
//			
//			@Override
//			public EntityMappingConfiguration<? super City, Identifier<Long>> getInheritanceConfiguration() {
//				return null;
//			}
//			
//			@Override
//			public boolean isJoinTable() {
//				return false;
//			}
//			
//			@Override
//			public Table getInheritanceTable() {
//				return null;
//			}
//			
//			@Override
//			public ForeignKeyNamingStrategy getForeignKeyNamingStrategy() {
//				return null;
//			}
//			
//			@Override
//			public AssociationTableNamingStrategy getAssociationTableNamingStrategy() {
//				return null;
//			}
//		});
//		
//		// Checking tables structure foreign key presence
//		Dialect dialect = new Dialect();
//		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
//		dialect.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
//		dialect.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
//		dialect.getJavaTypeToSqlTypeMapping().put(Identified.class, "int");
//		
//		CascadeOneConfigurer<Country, City, Identifier<Long>> testInstance = new CascadeOneConfigurer<>(new PersistenceContext(mock(ConnectionProvider.class), dialect));
//		JoinedTablesPersister<Country, Identifier<Long>, Table> countryPersister = new JoinedTablesPersister<>(countryClassMappingStrategy, dialect,
//				mock(ConnectionProvider.class), 10);
//		testInstance.appendCascade(countryCapitalRelation, countryPersister, ForeignKeyNamingStrategy.DEFAULT);
//		
//		assertEquals(Arrays.asSet("id", "capitalId", "name"), countryTable.mapColumnsOnName().keySet());
//		assertEquals(Arrays.asSet("FK_country_capitalId_City_id"), Iterables.collect(countryTable.getForeignKeys(), ForeignKey::getName, HashSet::new));
//		assertEquals(countryTableCapitalColumn, Iterables.first(Iterables.first(countryTable.getForeignKeys()).getColumns()));
//		assertEquals(cityTableIdColumn, Iterables.first(Iterables.first(countryTable.getForeignKeys()).getTargetColumns()));
//		
//		assertEquals(Arrays.asSet("id", "name"), cityTable.mapColumnsOnName().keySet());
//		assertEquals(Arrays.asSet(), Iterables.collect(cityTable.getForeignKeys(), ForeignKey::getName, HashSet::new));
//	}
	
	@Test
	void tableStructure_relationMappedByReverseSide() {
		// defining Country mapping
		Table<?> countryTable = new Table<>("country");
		Column countryTableIdColumn = countryTable.addColumn("id", long.class).primaryKey();
		Column countryTableNameColumn = countryTable.addColumn("name", String.class);
		Map<IReversibleAccessor, Column> countryMapping = Maps
				.asMap((IReversibleAccessor) new PropertyAccessor<>(new AccessorByMethodReference<>(Country::getId), Accessors.mutatorByField(Country.class, "id")), countryTableIdColumn)
				.add(new PropertyAccessor<>(new AccessorByMethodReference<>(Country::getName), new MutatorByMethodReference<>(Country::setName)), countryTableNameColumn);
		IReversibleAccessor<Country, Identifier<Long>> countryIdentifierAccessorByMethodReference = new PropertyAccessor<>(
				Accessors.accessorByMethodReference(Country::getId),
				Accessors.mutatorByField(Country.class, "id")
		);
		ClassMappingStrategy<Country, Identifier<Long>, Table> countryClassMappingStrategy = new ClassMappingStrategy<Country, Identifier<Long>, Table>(Country.class, countryTable,
				(Map) countryMapping, countryIdentifierAccessorByMethodReference, (IdentifierInsertionManager) new AlreadyAssignedIdentifierManager<Country, Identifier>(Identifier.class));
		
		// defining City mapping
		Table<?> cityTable = new Table<>("city");
		Column cityTableCountryColumn = cityTable.addColumn("countryId", long.class);
		IReversibleAccessor<City, Identifier<Long>> cityIdentifierAccessorByMethodReference = new PropertyAccessor<>(
				Accessors.accessorByMethodReference(City::getId),
				Accessors.mutatorByField(City.class, "id")
		);
		// defining Country -> City relation through capital property
		PropertyAccessor<Country, City> capitalAccessPoint = new PropertyAccessor<>(new AccessorByMethodReference<>(Country::getCapital),
				new MutatorByMethodReference<>(Country::setCapital));
		CascadeOne<Country, City, Identifier<Long>> countryCapitalRelation = new CascadeOne<>(capitalAccessPoint, new EntityMappingConfiguration<City, Identifier<Long>>() {
			@Override
			public Class<City> getPersistedClass() {
				return City.class;
			}
			
			@Override
			public TableNamingStrategy getTableNamingStrategy() {
				return TableNamingStrategy.DEFAULT;
			}
			
			@Override
			public IReversibleAccessor getIdentifierAccessor() {
				return cityIdentifierAccessorByMethodReference;
			}
			
			@Override
			public IdentifierInsertionManager<City, Identifier<Long>> getIdentifierInsertionManager() {
				return (IdentifierInsertionManager) new AlreadyAssignedIdentifierManager<City, Identifier>(Identifier.class);
			}
			
			@Override
			public EmbeddableMappingConfiguration<City> getPropertiesMapping() {
				return new EmbeddableMappingConfiguration<City>() {
					@Override
					public EmbeddableMappingConfiguration<? super City> getMappedSuperClassConfiguration() {
						return null;
					}
					
					@Override
					public List<Linkage> getPropertiesMapping() {
						EntityLinkageByColumnName identifierLinkage = new EntityLinkageByColumnName<>(
								new PropertyAccessor<>(new AccessorByMethodReference<>(City::getId), Accessors.mutatorByField(City.class, "id")),
								Identifier.class,
								"id"
						);
						identifierLinkage.primaryKey();
						EntityLinkageByColumnName nameLinkage = new EntityLinkageByColumnName<>(
								new PropertyAccessor<>(new AccessorByMethodReference<>(City::getName), Accessors.mutatorByField(City.class, "name")),
								String.class,
								"name"
						);
						return Arrays.asList(identifierLinkage, nameLinkage);
					}
					
					@Override
					public Collection<AbstractInset<City, ?>> getInsets() {
						return Collections.emptyList();
					}
					
					@Override
					public ColumnNamingStrategy getColumnNamingStrategy() {
						return ColumnNamingStrategy.DEFAULT;
					}
				};
			}
			
			@Override
			public List<CascadeOne<City, ?, ?>> getOneToOnes() {
				return new ArrayList<>();
			}
			
			@Override
			public List<CascadeMany<City, ?, ?, ? extends Collection>> getOneToManys() {
				return new ArrayList<>();
			}
			
			@Override
			public VersioningStrategy getOptimisticLockOption() {
				return null;
			}
			
			@Override
			public EntityMappingConfiguration<? super City, Identifier<Long>> getInheritanceConfiguration() {
				return null;
			}
			
			@Override
			public boolean isJoinTable() {
				return false;
			}
			
			@Override
			public Table getInheritanceTable() {
				return null;
			}
			
			@Override
			public ForeignKeyNamingStrategy getForeignKeyNamingStrategy() {
				return null;
			}
			
			@Override
			public AssociationTableNamingStrategy getAssociationTableNamingStrategy() {
				return null;
			}
		});
		countryCapitalRelation.setReverseColumn(cityTableCountryColumn);
		
		// Checking tables structure foreign key presence 
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(Identifier.class, "int");
		dialect.getColumnBinderRegistry().register((Class) Identified.class, Identified.identifiedBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getJavaTypeToSqlTypeMapping().put(Identified.class, "int");
		
		
		CascadeOneConfigurer<Country, City, Identifier<Long>> testInstance = new CascadeOneConfigurer<>(new PersistenceContext(mock(ConnectionProvider.class), dialect));
		JoinedTablesPersister<Country, Identifier<Long>, Table> countryPersister = new JoinedTablesPersister<>(countryClassMappingStrategy, dialect,
				mock(ConnectionProvider.class), 10);
		testInstance.appendCascade(countryCapitalRelation, countryPersister, ForeignKeyNamingStrategy.DEFAULT);
		
		assertEquals(Arrays.asSet("id", "countryId", "name"), cityTable.mapColumnsOnName().keySet());
		assertEquals(Arrays.asSet("FK_city_countryId_country_id"), Iterables.collect(cityTable.getForeignKeys(), ForeignKey::getName, HashSet::new));
		assertEquals(cityTableCountryColumn, Iterables.first(Iterables.first(cityTable.getForeignKeys()).getColumns()));
		assertEquals(countryTableIdColumn, Iterables.first(Iterables.first(cityTable.getForeignKeys()).getTargetColumns()));
		
		assertEquals(Arrays.asSet("id", "name"), countryTable.mapColumnsOnName().keySet());
		assertEquals(Arrays.asSet(), Iterables.collect(countryTable.getForeignKeys(), ForeignKey::getName, HashSet::new));
	}
	
}