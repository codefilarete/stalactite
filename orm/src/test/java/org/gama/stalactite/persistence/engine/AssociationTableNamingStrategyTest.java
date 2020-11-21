package org.gama.stalactite.persistence.engine;

import java.util.HashSet;
import java.util.Set;

import org.gama.lang.Duo;
import org.gama.lang.test.Assertions;
import org.gama.reflection.AccessorByMethodReference;
import org.gama.reflection.AccessorDefinition;
import org.gama.stalactite.persistence.engine.AssociationTableNamingStrategy.DefaultAssociationTableNamingStrategy;
import org.gama.stalactite.persistence.engine.model.City;
import org.gama.stalactite.persistence.structure.Column;
import org.gama.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.gama.lang.test.Assertions.hasExceptionInCauses;
import static org.gama.lang.test.Assertions.hasMessage;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Guillaume Mary
 */
class AssociationTableNamingStrategyTest {
	
	@Test
	void giveName() {
		Table countryTable = new Table(null, "CountryTable");
		Column countryPK = countryTable.addColumn("id", String.class).primaryKey();
		Table cityTable = new Table(null, "CityTable");
		Column countryFK = cityTable.addColumn("countryId", String.class);
		
		
		DefaultAssociationTableNamingStrategy testInstance = new DefaultAssociationTableNamingStrategy();
		
		assertEquals("Country_cities", testInstance.giveName(new AccessorDefinition(Country.class, "cities", Set.class), countryPK, countryFK));
		assertEquals("Country_giveCities", testInstance.giveName(new AccessorDefinition(Country.class, "giveCities", Set.class), countryPK, countryFK));
		
	}
	
	@Nested
	class keyColumnNames_primaryKeysTargetSameEntity_keyColumnNamesAreDifferent {
		
		@Test
		void fallsbackOnAttributeName() {
			DefaultAssociationTableNamingStrategy testInstance = new DefaultAssociationTableNamingStrategy();
			AccessorByMethodReference<CyclingModel, Set<CyclingModel>> methodReference = new AccessorByMethodReference<>(CyclingModel::getCyclingModels);
			AccessorDefinition accessorDefinition = new AccessorDefinition(
					methodReference.getDeclaringClass(),
					AccessorDefinition.giveDefinition(methodReference).getName(),
					methodReference.getPropertyType());
			Table cyclingModelTable = new Table("CyclingModelTable");
			Column cyclingModelIdColumn = cyclingModelTable.addColumn("id", long.class);
			Duo<String, String> columnNames = testInstance.giveColumnNames(accessorDefinition, cyclingModelIdColumn, cyclingModelIdColumn);
			// please note that ending "s" was removed, not a strong rule, could be removed if too "intrusive"
			assertEquals(new Duo<>("cyclingModelTable_id", "cyclingModel_id"), columnNames);
		}
		
		@Test
		void attributeNameIsSameAsTable_throwsException() {
			DefaultAssociationTableNamingStrategy testInstance = new DefaultAssociationTableNamingStrategy();
			AccessorByMethodReference<CyclingModel, Set<CyclingModel>> methodReference = new AccessorByMethodReference<>(CyclingModel::getCyclingModels);
			AccessorDefinition accessorDefinition = new AccessorDefinition(
					methodReference.getDeclaringClass(),
					AccessorDefinition.giveDefinition(methodReference).getName(),
					methodReference.getPropertyType());
			Table cyclingModelTable = new Table("CyclingModel");
			Column cyclingModelIdColumn = cyclingModelTable.addColumn("id", long.class);
			Assertions.assertThrows(() -> testInstance.giveColumnNames(accessorDefinition, cyclingModelIdColumn, cyclingModelIdColumn),
					hasExceptionInCauses(MappingConfigurationException.class)
					.andProjection(hasMessage("Identical column names in association table of collection" 
							+ " o.g.s.p.e.keyColumnNames_primaryKeysTargetSameEntity_keyColumnNamesAreDifferent$CyclingModel.cyclingModels")));
		}
		
		private class CyclingModel {
			
			private final Set<CyclingModel> cyclingModels = new HashSet<>();
			
			public Set<CyclingModel> getCyclingModels() {
				return cyclingModels;
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