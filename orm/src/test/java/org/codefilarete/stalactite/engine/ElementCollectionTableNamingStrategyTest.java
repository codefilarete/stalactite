package org.codefilarete.stalactite.engine;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.dsl.naming.ElementCollectionTableNamingStrategy.DefaultElementCollectionTableNamingStrategy;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Country;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ElementCollectionTableNamingStrategyTest {
	
	@Test
	void giveName() {
		DefaultElementCollectionTableNamingStrategy testInstance = new DefaultElementCollectionTableNamingStrategy();
		assertThat(testInstance.giveName(new AccessorDefinition(Country.class, "getCities", City.class))).isEqualTo("Country_cities");
		assertThat(testInstance.giveName(new AccessorDefinition(Country.class, "giveCities", City.class))).isEqualTo("Country_giveCities");
	}
}