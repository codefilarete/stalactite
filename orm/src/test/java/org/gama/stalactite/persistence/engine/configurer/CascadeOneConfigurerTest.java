package org.gama.stalactite.persistence.engine.configurer;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.reflection.AccessorByMethodReference;
import org.gama.reflection.Accessors;
import org.gama.reflection.ReversibleAccessor;
import org.gama.reflection.MutatorByMethodReference;
import org.gama.reflection.PropertyAccessor;
import org.gama.stalactite.persistence.engine.ColumnNamingStrategy;
import org.gama.stalactite.persistence.engine.EmbeddableMappingConfiguration;
import org.gama.stalactite.persistence.engine.EntityMappingConfiguration;
import org.gama.stalactite.persistence.engine.ForeignKeyNamingStrategy;
import org.gama.stalactite.persistence.engine.PersisterRegistry;
import org.gama.stalactite.persistence.engine.TableNamingStrategy;
import org.gama.stalactite.persistence.engine.model.City;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.engine.runtime.SimpleRelationalEntityPersister;
import org.gama.stalactite.persistence.id.Identifier;
import org.gama.stalactite.persistence.id.PersistableIdentifier;
import org.gama.stalactite.persistence.id.PersistedIdentifier;
import org.gama.stalactite.persistence.id.StatefullIdentifierAlreadyAssignedIdentifierPolicy;
import org.gama.stalactite.persistence.id.manager.AlreadyAssignedIdentifierManager;
import org.gama.stalactite.persistence.id.manager.IdentifierInsertionManager;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.sql.Dialect;
import org.gama.stalactite.persistence.sql.ConnectionConfiguration;
import org.gama.stalactite.persistence.sql.ConnectionConfiguration.ConnectionConfigurationSupport;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.ForeignKey;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.sql.ConnectionProvider;
import org.gama.stalactite.sql.binder.DefaultParameterBinders;
import org.gama.stalactite.sql.binder.ParameterBinder;
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
class CascadeOneConfigurerTest {
	
	@Test
	void tableStructure() throws SQLException {
		// Given
		// defining Country mapping
		Table<?> countryTable = new Table<>("country");
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
		ClassMappingStrategy<Country, Identifier<Long>, Table> countryClassMappingStrategy = new ClassMappingStrategy<Country, Identifier<Long>, Table>(Country.class, countryTable,
				(Map) countryMapping, countryIdentifierAccessorByMethodReference,
				(IdentifierInsertionManager) new AlreadyAssignedIdentifierManager<Country, Identifier>(Identifier.class, c -> {}, c -> false));
		
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
		when(cityMappingConfiguration.getIdentifierAccessor()).thenReturn(cityIdentifierAccessorByMethodReference);
		when(cityMappingConfiguration.getIdentifierPolicy()).thenReturn(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED);
		when(cityMappingConfiguration.getOneToOnes()).thenReturn(Collections.emptyList());
		when(cityMappingConfiguration.getOneToManys()).thenReturn(Collections.emptyList());
		when(cityMappingConfiguration.inheritanceIterable()).thenAnswer(CALLS_REAL_METHODS);
		

		// defining Country -> City relation through capital property
		PropertyAccessor<Country, City> capitalAccessPoint = new PropertyAccessor<>(new AccessorByMethodReference<>(Country::getCapital),
				new MutatorByMethodReference<>(Country::setCapital));
		CascadeOne<Country, City, Identifier<Long>> countryCapitalRelation = new CascadeOne<>(capitalAccessPoint, cityMappingConfiguration, cityTable);
		
		// Checking tables structure foreign key presence
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		
		// When
		SimpleRelationalEntityPersister<Country, Identifier<Long>, Table> countryPersister = new SimpleRelationalEntityPersister<>(countryClassMappingStrategy, dialect,
				new ConnectionConfigurationSupport(mock(ConnectionProvider.class), 10));
		CascadeOneConfigurer<Country, City, Identifier<Long>, Identifier<Long>> testInstance = new CascadeOneConfigurer<>(countryCapitalRelation,
				countryPersister,
				dialect,
				mock(ConnectionConfiguration.class),
				mock(PersisterRegistry.class),
				ForeignKeyNamingStrategy.DEFAULT, ColumnNamingStrategy.JOIN_DEFAULT);
		testInstance.appendCascades("city", new PersisterBuilderImpl<>(cityMappingConfiguration));
		
		// Then
		assertThat(countryTable.mapColumnsOnName().keySet()).isEqualTo(Arrays.asSet("id", "capitalId", "name"));
		assertThat(countryTable.getForeignKeys()).extracting(ForeignKey::getName).containsExactlyInAnyOrder("FK_country_capitalId_city_id");
		assertThat(Iterables.first(Iterables.first(countryTable.getForeignKeys()).getColumns())).isEqualTo(countryTableCapitalColumn);
		assertThat(Iterables.first(Iterables.first(countryTable.getForeignKeys()).getTargetColumns())).isEqualTo(cityTableIdColumn);
		
		assertThat(cityTable.mapColumnsOnName().keySet()).isEqualTo(Arrays.asSet("id", "name"));
		assertThat(cityTable.getForeignKeys()).extracting(ForeignKey::getName).isEmpty();
		
		// Additional checking on foreign key binder : it must have a binder due to relation owned by source throught Country::getCapital
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
	void tableStructure_relationMappedByReverseSide() {
		// defining Country mapping
		Table<?> countryTable = new Table<>("country");
		Column countryTableIdColumn = countryTable.addColumn("id", long.class).primaryKey();
		Column countryTableNameColumn = countryTable.addColumn("name", String.class);
		Map<ReversibleAccessor, Column> countryMapping = Maps
				.asMap((ReversibleAccessor) new PropertyAccessor<>(new AccessorByMethodReference<>(Country::getId), Accessors.mutatorByField(Country.class, "id")), countryTableIdColumn)
				.add(new PropertyAccessor<>(new AccessorByMethodReference<>(Country::getName), new MutatorByMethodReference<>(Country::setName)), countryTableNameColumn);
		ReversibleAccessor<Country, Identifier<Long>> countryIdentifierAccessorByMethodReference = new PropertyAccessor<>(
				Accessors.accessorByMethodReference(Country::getId),
				Accessors.mutatorByField(Country.class, "id")
		);
		ClassMappingStrategy<Country, Identifier<Long>, Table> countryClassMappingStrategy = new ClassMappingStrategy<Country, Identifier<Long>, Table>(Country.class, countryTable,
				(Map) countryMapping, countryIdentifierAccessorByMethodReference,
				(IdentifierInsertionManager) new AlreadyAssignedIdentifierManager<Country, Identifier>(Identifier.class, c -> {}, c -> false));
		
		// defining City mapping
		Table<?> cityTable = new Table<>("city");
		Column cityTableCountryColumn = cityTable.addColumn("countryId", long.class);
		ReversibleAccessor<City, Identifier<Long>> cityIdentifierAccessorByMethodReference = new PropertyAccessor<>(
				Accessors.accessorByMethodReference(City::getId),
				Accessors.mutatorByField(City.class, "id")
		);
		// defining Country -> City relation through capital property
		PropertyAccessor<Country, City> capitalAccessPoint = new PropertyAccessor<>(new AccessorByMethodReference<>(Country::getCapital),
				new MutatorByMethodReference<>(Country::setCapital));
		
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
		when(cityMappingConfiguration.getIdentifierAccessor()).thenReturn(cityIdentifierAccessorByMethodReference);
		when(cityMappingConfiguration.getIdentifierPolicy()).thenReturn(StatefullIdentifierAlreadyAssignedIdentifierPolicy.ALREADY_ASSIGNED);
		when(cityMappingConfiguration.getOneToOnes()).thenReturn(Collections.emptyList());
		when(cityMappingConfiguration.getOneToManys()).thenReturn(Collections.emptyList());
		when(cityMappingConfiguration.inheritanceIterable()).thenAnswer(CALLS_REAL_METHODS);
		
		CascadeOne<Country, City, Identifier<Long>> countryCapitalRelation = new CascadeOne<>(capitalAccessPoint, cityMappingConfiguration, cityTable);
		countryCapitalRelation.setReverseColumn(cityTableCountryColumn);
		
		// Checking tables structure foreign key presence 
		Dialect dialect = new Dialect();
		dialect.getColumnBinderRegistry().register((Class) Identifier.class, Identifier.identifierBinder(DefaultParameterBinders.LONG_PRIMITIVE_BINDER));
		dialect.getSqlTypeRegistry().put(Identifier.class, "int");
		
		SimpleRelationalEntityPersister<Country, Identifier<Long>, Table> countryPersister = new SimpleRelationalEntityPersister<>(countryClassMappingStrategy, dialect,
				new ConnectionConfigurationSupport(mock(ConnectionProvider.class), 10));
		CascadeOneConfigurer<Country, City, Identifier<Long>, Identifier<Long>> testInstance = new CascadeOneConfigurer<>(countryCapitalRelation,
				countryPersister,
				dialect,
				mock(ConnectionConfiguration.class),
				mock(PersisterRegistry.class),
				ForeignKeyNamingStrategy.DEFAULT, ColumnNamingStrategy.JOIN_DEFAULT);
		testInstance.appendCascades("city", new PersisterBuilderImpl<>(cityMappingConfiguration));
		
		assertThat(cityTable.mapColumnsOnName().keySet()).isEqualTo(Arrays.asSet("id", "countryId", "name"));
		assertThat(cityTable.getForeignKeys()).extracting(ForeignKey::getName).containsExactlyInAnyOrder("FK_city_countryId_country_id");
		assertThat(Iterables.first(Iterables.first(cityTable.getForeignKeys()).getColumns())).isEqualTo(cityTableCountryColumn);
		assertThat(Iterables.first(Iterables.first(cityTable.getForeignKeys()).getTargetColumns())).isEqualTo(countryTableIdColumn);
		
		assertThat(countryTable.mapColumnsOnName().keySet()).isEqualTo(Arrays.asSet("id", "name"));
		assertThat(countryTable.getForeignKeys()).extracting(ForeignKey::getName).isEmpty();
	}
	
}