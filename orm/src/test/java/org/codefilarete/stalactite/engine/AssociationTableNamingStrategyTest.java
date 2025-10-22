package org.codefilarete.stalactite.engine;

import java.util.HashSet;
import java.util.Set;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.dsl.naming.AssociationTableNamingStrategy.DefaultAssociationTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.AssociationTableNamingStrategy.HibernateAssociationTableNamingStrategy;
import org.codefilarete.stalactite.dsl.naming.AssociationTableNamingStrategy.ReferencedColumnNames;
import org.codefilarete.stalactite.dsl.MappingConfigurationException;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.exception.Exceptions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author Guillaume Mary
 */
class AssociationTableNamingStrategyTest {
	
	@Test
	void defaultImplementation_giveName() {
		Table countryTable = new Table(null, "CountryTable");
		Column countryTableIdColumn = countryTable.addColumn("id", String.class).primaryKey();
		Table cityTable = new Table(null, "CityTable");
		Column cityTableRefIdColumn = cityTable.addColumn("cityId", String.class).primaryKey();
		
		
		DefaultAssociationTableNamingStrategy testInstance = new DefaultAssociationTableNamingStrategy();
		
		assertThat(testInstance.giveName(new AccessorDefinition(Country.class, "cities", Set.class), countryTable.getPrimaryKey(), cityTable.getPrimaryKey())).isEqualTo(
				"Country_cities");
		assertThat(testInstance.giveName(new AccessorDefinition(Country.class, "giveCities", Set.class), countryTable.getPrimaryKey(), cityTable.getPrimaryKey())).isEqualTo(
				"Country_giveCities");
		ReferencedColumnNames<?, ?> expectedColumns = new ReferencedColumnNames<>();
		expectedColumns.setLeftColumnName(countryTableIdColumn, "countryTable_id");
		expectedColumns.setRightColumnName(cityTableRefIdColumn, "giveCities_cityId");
		assertThat(testInstance.giveColumnNames(new AccessorDefinition(Country.class, "giveCities", Set.class), countryTable.getPrimaryKey(), cityTable.getPrimaryKey()))
				.usingRecursiveComparison()
				.isEqualTo(expectedColumns);
	}
	
	@Test
	void hibernateImplementation_giveName() {
		Table countryTable = new Table(null, "CountryTable");
		Column countryTableIdColumn = countryTable.addColumn("id", String.class).primaryKey();
		Table cityTable = new Table(null, "CityTable");
		Column cityTableRefIdColumn = cityTable.addColumn("cityId", String.class).primaryKey();
		
		
		HibernateAssociationTableNamingStrategy testInstance = new HibernateAssociationTableNamingStrategy();
		
		assertThat(testInstance.giveName(new AccessorDefinition(Country.class, "cities", Set.class), countryTable.getPrimaryKey(), cityTable.getPrimaryKey())).isEqualTo(
				"CountryTable_CityTable");
		assertThat(testInstance.giveName(new AccessorDefinition(Country.class, "giveCities", Set.class), countryTable.getPrimaryKey(), cityTable.getPrimaryKey())).isEqualTo(
				"CountryTable_CityTable");
		ReferencedColumnNames<?, ?> expectedColumns = new ReferencedColumnNames<>();
		expectedColumns.setLeftColumnName(countryTableIdColumn, "countryTable_id");
		expectedColumns.setRightColumnName(cityTableRefIdColumn, "cityId");
		assertThat(testInstance.giveColumnNames(new AccessorDefinition(Country.class, "giveCities", Set.class), countryTable.getPrimaryKey(), cityTable.getPrimaryKey()))
				.usingRecursiveComparison()
				.isEqualTo(expectedColumns);
	}
	
	@Nested
	class keyColumnNames_primaryKeysTargetSameEntity_keyColumnNamesAreDifferent {
		
		@Test
		void fallsbackOnAttributeName() {
			DefaultAssociationTableNamingStrategy testInstance = new DefaultAssociationTableNamingStrategy();
			AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(new AccessorByMethodReference<>(CyclingModel::getCyclingModels));
			Table cyclingModelTable = new Table("CyclingModelTable");
			Column idColumn = cyclingModelTable.addColumn("id", long.class);
			idColumn.primaryKey();
			ReferencedColumnNames columnNames = testInstance.giveColumnNames(accessorDefinition, cyclingModelTable.getPrimaryKey(), cyclingModelTable.getPrimaryKey());
			// please note that ending "s" was removed, not a strong rule, could be removed if too "intrusive"
			assertThat(columnNames.getLeftColumnName(idColumn)).isEqualTo("cyclingModelTable_id");
			assertThat(columnNames.getRightColumnName(idColumn)).isEqualTo("cyclingModels_id");
		}
		
		@Test
		<T extends Table<T>> void attributeNameIsSameAsTable_throwsException() {
			DefaultAssociationTableNamingStrategy testInstance = new DefaultAssociationTableNamingStrategy();
			AccessorDefinition accessorDefinition = AccessorDefinition.giveDefinition(new AccessorByMethodReference<>(CyclingModel::getCyclingModel));
			T cyclingModelTable = (T) new Table("CyclingModel");
			cyclingModelTable.addColumn("id", long.class).primaryKey();
			assertThatThrownBy(() -> testInstance.giveColumnNames(accessorDefinition, cyclingModelTable.getPrimaryKey(), cyclingModelTable.getPrimaryKey()))
					.extracting(t -> Exceptions.findExceptionInCauses(t, MappingConfigurationException.class), InstanceOfAssertFactories.THROWABLE)
					.hasMessage("Identical column names in association table of collection" 
							+ " o.c.s.e.keyColumnNames_primaryKeysTargetSameEntity_keyColumnNamesAreDifferent$CyclingModel.cyclingModel : cyclingModel_id");
		}
		
		private class CyclingModel {
			
			private final Set<CyclingModel> cyclingModels = new HashSet<>();
			
			private final CyclingModel cyclingModel = null;
			
			public Set<CyclingModel> getCyclingModels() {
				return cyclingModels;
			}
			
			public CyclingModel getCyclingModel() {
				return cyclingModel;
			}
		}
		
	}
	
	private static class Country {
		
		private final Set<City> cities = new HashSet<>();
		
		Country() {
			
		}
		
		public Set<City> getCities() {
			return cities;
		}
		
		public Set<City> giveCities() {
			return cities;
		}
		
	}
}