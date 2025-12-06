package org.codefilarete.stalactite.engine.configurer;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.Accessors;
import org.codefilarete.reflection.MutatorByMethodReference;
import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration.ColumnLinkageOptions;
import org.codefilarete.stalactite.dsl.entity.EntityMappingConfiguration.SingleKeyMapping;
import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.dsl.naming.ColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.ForeignKeyNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.IndexNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.JoinColumnNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.TableNamingStrategy;
import org.codefilarete.stalactite.engine.PersisterRegistry;
import org.codefilarete.stalactite.engine.configurer.property.ColumnLinkageOptionsSupport;
import org.codefilarete.stalactite.engine.configurer.embeddable.LinkageSupport;
import org.codefilarete.stalactite.engine.configurer.builder.PersisterBuilderContext;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneRelation;
import org.codefilarete.stalactite.engine.configurer.onetoone.OneToOneRelationConfigurer;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.runtime.SimpleRelationalEntityPersister;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.id.PersistableIdentifier;
import org.codefilarete.stalactite.id.PersistedIdentifier;
import org.codefilarete.stalactite.id.StatefulIdentifierAlreadyAssignedIdentifierPolicy;
import org.codefilarete.stalactite.mapping.DefaultEntityMapping;
import org.codefilarete.stalactite.mapping.id.manager.AlreadyAssignedIdentifierManager;
import org.codefilarete.stalactite.mapping.id.manager.IdentifierInsertionManager;
import org.codefilarete.stalactite.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.codefilarete.stalactite.sql.ConnectionProvider;
import org.codefilarete.stalactite.sql.Dialect;
import org.codefilarete.stalactite.sql.ddl.Size;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.ForeignKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.stalactite.sql.statement.binder.DefaultParameterBinders;
import org.codefilarete.stalactite.sql.statement.binder.ParameterBinder;
import org.codefilarete.stalactite.test.DefaultDialect;
import org.codefilarete.tool.Reflections;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Guillaume Mary
 */
class OneToOneRelationConfigurerTest {
	
	@BeforeEach
	void initEntityCandidates() {
		PersisterBuilderContext.CURRENT.set(new PersisterBuilderContext(mock(PersisterRegistry.class)));
	}
	
	@AfterEach
	void removeEntityCandidates() {
		PersisterBuilderContext.CURRENT.remove();
	}
	
	@Test
	<T extends Table<T>> void tableStructure_associationTable() throws SQLException {
		// Given
		// defining Country mapping
		T countryTable = (T) new Table("country");
		Column countryTableIdColumn = countryTable.addColumn("id", long.class).primaryKey();
		Column countryTableNameColumn = countryTable.addColumn("name", String.class);
		Column countryTableCapitalColumn = countryTable.addColumn("capitalId", Identifier.LONG_TYPE);
		Map<ReversibleAccessor, Column> countryMapping = Maps
				.asMap((ReversibleAccessor) new PropertyAccessor<>(new AccessorByMethodReference<>(Country::getId), Accessors.mutatorByField(Country.class, "id")), countryTableIdColumn)
				.add(new PropertyAccessor<>(new AccessorByMethodReference<>(Country::getName), new MutatorByMethodReference<>(Country::setName)), countryTableNameColumn)
				.add(new PropertyAccessor<>(new AccessorByMethodReference<>(Country::getCapital), new MutatorByMethodReference<>(Country::setCapital)), countryTableCapitalColumn);
		ReversibleAccessor<Country, Identifier<Long>> countryIdentifierAccessorByMethodReference = new PropertyAccessor<>(
				Accessors.accessorByMethodReference(Country::getId),
				Accessors.mutatorByField(Country.class, "id")
		);
		DefaultEntityMapping<Country, Identifier<Long>, T> countryEntityMappingStrategy = new DefaultEntityMapping<Country, Identifier<Long>, T>(Country.class, countryTable,
				(Map) countryMapping, countryIdentifierAccessorByMethodReference,
				(IdentifierInsertionManager) new AlreadyAssignedIdentifierManager<Country, Identifier>(Identifier.class, c -> {}, c -> false));
		
		LinkageSupport<City, Identifier<Long>> identifierLinkage = new LinkageSupport<>(City::getId);
		identifierLinkage.setField(Reflections.findField(City.class, "id"));
		identifierLinkage.setColumnOptions(new ColumnLinkageOptionsSupport("id"));
		LinkageSupport<City, String> nameLinkage = new LinkageSupport<>(City::getName);
		nameLinkage.setField(Reflections.findField(City.class, "name"));
		nameLinkage.setColumnOptions(new ColumnLinkageOptionsSupport("name"));
		
		// defining City mapping
		Table<?> cityTable = new Table<>("city");
		Column cityTableIdColumn = cityTable.addColumn("id", Identifier.class).primaryKey();
		ReversibleAccessor<City, Identifier<Long>> cityIdentifierAccessorByMethodReference = new PropertyAccessor<>(
				Accessors.accessorByMethodReference(City::getId),
				Accessors.mutatorByField(City.class, "id")
		);
		EmbeddableMappingConfiguration<City> cityPropertiesMapping = mock(EmbeddableMappingConfiguration.class);
		// declaring mapping
		when(cityPropertiesMapping.getBeanType()).thenReturn(City.class);
		when(cityPropertiesMapping.getPropertiesMapping()).thenReturn(Arrays.asList(identifierLinkage, nameLinkage));
		// preventing NullPointerException
		when(cityPropertiesMapping.getInsets()).thenReturn(Collections.emptyList());
		when(cityPropertiesMapping.getColumnNamingStrategy()).thenReturn(ColumnNamingStrategy.DEFAULT);
		
		EntityMappingConfiguration<City, Identifier<Long>> cityMappingConfiguration = mock(EntityMappingConfiguration.class);
		// declaring mapping
		when(cityMappingConfiguration.getEntityType()).thenReturn(City.class);
		when(cityMappingConfiguration.getPropertiesMapping()).thenReturn(cityPropertiesMapping);
		// preventing NullPointerException
		when(cityMappingConfiguration.getTableNamingStrategy()).thenReturn(TableNamingStrategy.DEFAULT);
		when(cityMappingConfiguration.getKeyMapping()).thenReturn(new SingleKeyMapping<City, Identifier<Long>>() {
			@Override
			public IdentifierPolicy<Identifier<Long>> getIdentifierPolicy() {
				return StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;
			}
			
			@Override
			public ReversibleAccessor<City, Identifier<Long>> getAccessor() {
				return cityIdentifierAccessorByMethodReference;
			}
			
			@Override
			public ColumnLinkageOptions getColumnOptions() {
				return new ColumnLinkageOptions() {
					@Nullable
					@Override
					public String getColumnName() {
						return null;
					}

					@Nullable
					@Override
					public Size getColumnSize() {
						return null;
					}
				};
			}
			
			@Nullable
			@Override
			public Field getField() {
				return null;
			}

			@Override
			public boolean isSetByConstructor() {
				return false;
			}
		});
		when(cityMappingConfiguration.getOneToOnes()).thenReturn(Collections.emptyList());
		when(cityMappingConfiguration.getOneToManys()).thenReturn(Collections.emptyList());
		when(cityMappingConfiguration.getManyToManys()).thenReturn(Collections.emptyList());
		when(cityMappingConfiguration.inheritanceIterable()).thenAnswer(CALLS_REAL_METHODS);
		
		// defining Country -> City relation through capital property
		PropertyAccessor<Country, City> capitalAccessPoint = new PropertyAccessor<>(new AccessorByMethodReference<>(Country::getCapital),
				new MutatorByMethodReference<>(Country::setCapital));
		OneToOneRelation<Country, City, Identifier<Long>> countryCapitalRelation = new OneToOneRelation<>(
				capitalAccessPoint,
				() -> true,
				() -> new EntityMappingConfigurationWithTable<>(cityMappingConfiguration, cityTable));
		// no reverse column declared, hence relation is maintained through an association table
		
		// Checking tables structure foreign key presence
		Dialect dialect = new DefaultDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		
		// When
		ConnectionConfigurationSupport connectionConfiguration = new ConnectionConfigurationSupport(mock(ConnectionProvider.class), 10);
		SimpleRelationalEntityPersister<Country, Identifier<Long>, T> countryPersister = new SimpleRelationalEntityPersister<>(countryEntityMappingStrategy, dialect,
				connectionConfiguration);
		OneToOneRelationConfigurer<Country, Identifier<Long>, City, Identifier<Long>> testInstance = new OneToOneRelationConfigurer<>(
				dialect,
				connectionConfiguration,
				countryPersister,
				TableNamingStrategy.DEFAULT, JoinColumnNamingStrategy.JOIN_DEFAULT, ForeignKeyNamingStrategy.DEFAULT, IndexNamingStrategy.DEFAULT,
				PersisterBuilderContext.CURRENT.get());
		
		testInstance.configure(countryCapitalRelation);
		
		// Then
		assertThat(countryTable.mapColumnsOnName().keySet()).isEqualTo(Arrays.asSet("id", "capitalId", "name"));
		assertThat(countryTable.getForeignKeys()).extracting(ForeignKey::getName).containsExactlyInAnyOrder("FK_country_capitalId_city_id");
		assertThat(Iterables.first(Iterables.first(countryTable.getForeignKeys()).getColumns())).isEqualTo(countryTableCapitalColumn);
		assertThat(Iterables.first(Iterables.first(countryTable.getForeignKeys()).getTargetColumns())).isEqualTo(cityTableIdColumn);
		
		assertThat(cityTable.mapColumnsOnName().keySet()).isEqualTo(Arrays.asSet("id", "name"));
		assertThat(cityTable.getForeignKeys()).extracting(ForeignKey::getName).isEmpty();
		
		// Additional checking on foreign key binder : it must have a binder due to relation owned by source through Country::getCapital
		ParameterBinder<Identifier> cityParameterBinder = dialect.getColumnBinderRegistry().getBinder(countryTableCapitalColumn);
		assertThat(cityParameterBinder).isNotNull();
		
		PreparedStatement preparedStatementMock = mock(PreparedStatement.class);
		cityParameterBinder.set(preparedStatementMock, 1, new PersistableIdentifier<>(4L));
		verify(preparedStatementMock).setLong(eq(1), eq(4L));
		
		ResultSet resultSetMock = mock(ResultSet.class);
		// because City ParameterBinder is a NullAwareParameterBinder that wraps the interesting one, we must mimic a non null value in ResultSet
		// to make underlying binder being called
		when(resultSetMock.getObject(anyString())).thenReturn(new Object());
		when(resultSetMock.getLong(anyString())).thenReturn(42L);
		assertThat(cityParameterBinder.get(resultSetMock, "whateverColumn")).isEqualTo(new PersistedIdentifier<>(42L));
	}
	
	@Test
	<T extends Table<T>> void tableStructure_relationMappedByReverseSide() {
		// defining Country mapping
		T countryTable = (T) new Table("country");
		Column countryTableIdColumn = countryTable.addColumn("id", long.class).primaryKey();
		Column countryTableNameColumn = countryTable.addColumn("name", String.class);
		Map<ReversibleAccessor, Column> countryMapping = Maps
				.asMap((ReversibleAccessor) new PropertyAccessor<>(new AccessorByMethodReference<>(Country::getId), Accessors.mutatorByField(Country.class, "id")), countryTableIdColumn)
				.add(new PropertyAccessor<>(new AccessorByMethodReference<>(Country::getName), new MutatorByMethodReference<>(Country::setName)), countryTableNameColumn);
		ReversibleAccessor<Country, Identifier<Long>> countryIdentifierAccessorByMethodReference = new PropertyAccessor<>(
				Accessors.accessorByMethodReference(Country::getId),
				Accessors.mutatorByField(Country.class, "id")
		);
		DefaultEntityMapping<Country, Identifier<Long>, T> countryEntityMappingStrategy = new DefaultEntityMapping<Country, Identifier<Long>, T>(Country.class, countryTable,
				(Map) countryMapping, countryIdentifierAccessorByMethodReference,
				(IdentifierInsertionManager) new AlreadyAssignedIdentifierManager<Country, Identifier>(Identifier.class, c -> {}, c -> false));
		
		// defining City mapping
		Table<?> cityTable = new Table<>("city");
		Column cityTableCountryColumn = cityTable.addColumn("countryId", long.class);
		ReversibleAccessor<City, Identifier<Long>> cityIdentifierAccessorByMethodReference = new PropertyAccessor<>(
				Accessors.accessorByMethodReference(City::getId),
				Accessors.mutatorByField(City.class, "id")
		);
		
		LinkageSupport<City, Identifier<Long>> identifierLinkage = new LinkageSupport<>(City::getId);
		identifierLinkage.setField(Reflections.findField(City.class, "id"));
		identifierLinkage.setColumnOptions(new ColumnLinkageOptionsSupport("id"));
		LinkageSupport<City, String> nameLinkage = new LinkageSupport<>(City::getName);
		nameLinkage.setField(Reflections.findField(City.class, "name"));
		nameLinkage.setColumnOptions(new ColumnLinkageOptionsSupport("name"));
		
		EmbeddableMappingConfiguration<City> cityPropertiesMapping = mock(EmbeddableMappingConfiguration.class);
		// declaring mapping
		when(cityPropertiesMapping.getPropertiesMapping()).thenReturn(Arrays.asList(identifierLinkage, nameLinkage));
		// preventing NullPointerException
		when(cityPropertiesMapping.getInsets()).thenReturn(Collections.emptyList());
		when(cityPropertiesMapping.getColumnNamingStrategy()).thenReturn(ColumnNamingStrategy.DEFAULT);
		
		EntityMappingConfiguration<City, Identifier<Long>> cityMappingConfiguration = mock(EntityMappingConfiguration.class);
		// declaring mapping
		when(cityMappingConfiguration.getEntityType()).thenReturn(City.class);
		when(cityMappingConfiguration.getPropertiesMapping()).thenReturn(cityPropertiesMapping);
		// preventing NullPointerException
		when(cityMappingConfiguration.getTableNamingStrategy()).thenReturn(TableNamingStrategy.DEFAULT);
		when(cityMappingConfiguration.getKeyMapping()).thenReturn(new SingleKeyMapping<City, Identifier<Long>>() {
			@Override
			public IdentifierPolicy<Identifier<Long>> getIdentifierPolicy() {
				return StatefulIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED;
			}
			
			@Override
			public ReversibleAccessor<City, Identifier<Long>> getAccessor() {
				return cityIdentifierAccessorByMethodReference;
			}
			
			@Override
			public ColumnLinkageOptions getColumnOptions() {
				return new ColumnLinkageOptions() {
					@Nullable
					@Override
					public String getColumnName() {
						return null;
					}

					@Nullable
					@Override
					public Size getColumnSize() {
						return null;
					}
				};
			}
			
			@Nullable
			@Override
			public Field getField() {
				return null;
			}
			
			@Override
			public boolean isSetByConstructor() {
				return false;
			}
		});
		when(cityMappingConfiguration.getOneToOnes()).thenReturn(Collections.emptyList());
		when(cityMappingConfiguration.getOneToManys()).thenReturn(Collections.emptyList());
		when(cityMappingConfiguration.getManyToManys()).thenReturn(Collections.emptyList());
		when(cityMappingConfiguration.inheritanceIterable()).thenAnswer(CALLS_REAL_METHODS);
		
		// defining Country -> City relation through capital property
		PropertyAccessor<Country, City> capitalAccessPoint = new PropertyAccessor<>(new AccessorByMethodReference<>(Country::getCapital),
				new MutatorByMethodReference<>(Country::setCapital));
		OneToOneRelation<Country, City, Identifier<Long>> countryCapitalRelation = new OneToOneRelation<>(
				capitalAccessPoint,
				() -> true,
				() -> new EntityMappingConfigurationWithTable<>(cityMappingConfiguration, cityTable));
		// giving reverse column to declare a relation owned by target table (no association table)
		countryCapitalRelation.setReverseColumn(cityTableCountryColumn);
		
		// Checking tables structure foreign key presence 
		Dialect dialect = new DefaultDialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		
		ConnectionConfigurationSupport connectionConfiguration = new ConnectionConfigurationSupport(mock(ConnectionProvider.class), 10);
		SimpleRelationalEntityPersister<Country, Identifier<Long>, T> countryPersister = new SimpleRelationalEntityPersister<>(countryEntityMappingStrategy, dialect,
				connectionConfiguration);
		
		OneToOneRelationConfigurer<Country, Identifier<Long>, City, Identifier<Long>> testInstance = new OneToOneRelationConfigurer<>(
				dialect,
				connectionConfiguration,
				countryPersister,
				TableNamingStrategy.DEFAULT, JoinColumnNamingStrategy.JOIN_DEFAULT, ForeignKeyNamingStrategy.DEFAULT, IndexNamingStrategy.DEFAULT,
				PersisterBuilderContext.CURRENT.get());
		
		testInstance.configure(countryCapitalRelation);
		
		assertThat(cityTable.mapColumnsOnName().keySet()).isEqualTo(Arrays.asSet("id", "countryId", "name"));
		assertThat(cityTable.getForeignKeys()).extracting(ForeignKey::getName).containsExactlyInAnyOrder("FK_city_countryId_country_id");
		assertThat(Iterables.first(Iterables.first(cityTable.getForeignKeys()).getColumns())).isEqualTo(cityTableCountryColumn);
		assertThat(Iterables.first(Iterables.first(cityTable.getForeignKeys()).getTargetColumns())).isEqualTo(countryTableIdColumn);
		
		assertThat(countryTable.mapColumnsOnName().keySet()).isEqualTo(Arrays.asSet("id", "name"));
		assertThat(countryTable.getForeignKeys()).extracting(ForeignKey::getName).isEmpty();
	}
	
}
