package org.codefilarete.stalactite.engine;

import java.util.Collections;
import java.util.Map;

import org.codefilarete.reflection.AccessorDefinition;
import org.codefilarete.stalactite.dsl.naming.MapEntryTableNamingStrategy.DefaultMapEntryTableNamingStrategy;
import org.codefilarete.stalactite.engine.model.City;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.PrimaryKey;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Iterables;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MapEntryTableNamingStrategyTest {
	
	@Test
	void giveName() {
		DefaultMapEntryTableNamingStrategy testInstance = new DefaultMapEntryTableNamingStrategy();
		assertThat(testInstance.giveTableName(new AccessorDefinition(Country.class, "getCities", City.class), String.class, Integer.class)).isEqualTo("Country_cities");
		assertThat(testInstance.giveTableName(new AccessorDefinition(Country.class, "giveCities", City.class), String.class, Integer.class)).isEqualTo("Country_giveCities");
	}
	
	@Test
	void giveMapKeyColumnNames() {
		DefaultMapEntryTableNamingStrategy testInstance = new DefaultMapEntryTableNamingStrategy();
		Table cityTable = new Table("cityTable");
		cityTable.addColumn("id", long.class).primaryKey();
		
		PrimaryKey primaryKey = cityTable.getPrimaryKey();
		Column pkColumn = (Column) Iterables.first(primaryKey.getColumns());
		Map getCities = testInstance.giveMapKeyColumnNames(new AccessorDefinition(Country.class, "getCities", City.class), City.class, primaryKey, Collections.<String>emptySet());
		assertThat(getCities).containsEntry(pkColumn, "city_id");
	}
	
}