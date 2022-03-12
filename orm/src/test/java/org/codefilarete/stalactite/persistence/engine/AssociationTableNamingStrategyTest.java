package org.codefilarete.stalactite.persistence.engine;

import java.util.HashSet;
import java.util.Set;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.codefilarete.tool.Duo;
import org.codefilarete.tool.exception.Exceptions;
import org.codefilarete.reflection.AccessorByMethodReference;
import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.persistence.engine.AssociationTableNamingStrategy.DefaultAssociationTableNamingStrategy;
import org.codefilarete.stalactite.persistence.engine.model.City;
import org.codefilarete.stalactite.persistence.structure.Column;
import org.codefilarete.stalactite.persistence.structure.Table;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
		
		assertThat(testInstance.giveName(new AccessorDefinition(Country.class, "cities", Set.class), countryPK, countryFK)).isEqualTo(
				"Country_cities");
		assertThat(testInstance.giveName(new AccessorDefinition(Country.class, "giveCities", Set.class), countryPK, countryFK)).isEqualTo(
				"Country_giveCities");
		
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
			assertThat(columnNames).isEqualTo(new Duo<>("cyclingModelTable_id", "cyclingModel_id"));
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
			assertThatThrownBy(() -> testInstance.giveColumnNames(accessorDefinition, cyclingModelIdColumn, cyclingModelIdColumn))
					.extracting(t -> Exceptions.findExceptionInCauses(t, MappingConfigurationException.class), InstanceOfAssertFactories.THROWABLE)
					.hasMessage("Identical column names in association table of collection" 
							+ " o.c.s.p.e.keyColumnNames_primaryKeysTargetSameEntity_keyColumnNamesAreDifferent$CyclingModel.cyclingModels");
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