package org.codefilarete.stalactite.engine.configurer.resolver;

import org.codefilarete.stalactite.dsl.entity.FluentEntityMappingBuilder;
import org.codefilarete.stalactite.dsl.idpolicy.IdentifierPolicy;
import org.codefilarete.stalactite.dsl.naming.TableNamingStrategy;
import org.codefilarete.stalactite.engine.model.AbstractCountry;
import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.engine.model.King;
import org.codefilarete.stalactite.engine.model.Realm;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.dsl.FluentMappings.embeddableBuilder;
import static org.codefilarete.stalactite.dsl.FluentMappings.entityBuilder;

class TableLookupStepTest {
	
	@Test
	void defaultBehavior() {
		FluentEntityMappingBuilder<Realm, Integer> entityMappingBuilder = entityBuilder(Realm.class, int.class)
				.mapKey(Country::getVersion, IdentifierPolicy.databaseAutoIncrement())
				.map(Country::setDescription);
		
		TableLookupStep<Realm, Integer> testInstance = new TableLookupStep<>();
		Table table = testInstance.lookupForTable(entityMappingBuilder.getConfiguration(), TableNamingStrategy.DEFAULT);
		assertThat(table.getName()).isEqualTo("Realm");
	}
	
	@Test
	void tableNameIsDefined() {
		FluentEntityMappingBuilder<Realm, Integer> entityMappingBuilder = entityBuilder(Realm.class, int.class)
				.mapKey(Country::getVersion, IdentifierPolicy.databaseAutoIncrement())
				.map(Country::setDescription)
				.onTable("Country");
		
		TableLookupStep<Realm, Integer> testInstance = new TableLookupStep<>();
		Table table = testInstance.lookupForTable(entityMappingBuilder.getConfiguration(), TableNamingStrategy.DEFAULT);
		assertThat(table.getName()).isEqualTo("Country");
	}
	
	@Test
	void tableIsDefined() {
		Table targetTable = new Table("Country");
		FluentEntityMappingBuilder<Realm, Integer> entityMappingBuilder = entityBuilder(Realm.class, int.class)
				.mapKey(Country::getVersion, IdentifierPolicy.databaseAutoIncrement())
				.map(Country::setDescription)
				.onTable(targetTable);
		
		TableLookupStep<Realm, Integer> testInstance = new TableLookupStep<>();
		Table table = testInstance.lookupForTable(entityMappingBuilder.getConfiguration(), TableNamingStrategy.DEFAULT);
		assertThat(table).isSameAs(targetTable);
	}
	
	@Test
	void mappedSuperClass_tableNameIsDefined() {
		FluentEntityMappingBuilder<Realm, String> entityMappingBuilder = entityBuilder(Realm.class, String.class)
				.embed(Realm::getKing, embeddableBuilder(King.class)
						.map(King::getName).columnName("kingName")
				)
				.map(Country::setDescription)
				.mapSuperClass(entityBuilder(Country.class, String.class)
						.embed(Country::getTimestamp, embeddableBuilder(Timestamp.class)
								.map(Timestamp::getCreationDate).columnName("creation_date")
								.map(Timestamp::setModificationDate).setByConstructor()
						)
						.map(Country::setName)
						.onTable("Country")
						.mapSuperClass(entityBuilder(AbstractCountry.class, String.class)
								.mapKey(AbstractCountry::getDummyProperty, IdentifierPolicy.alreadyAssigned()).columnName("myProperty")
								.onTable("AbstractCountry")
						).joiningTables()
				);
		
		TableLookupStep<Realm, String> testInstance = new TableLookupStep<>();
		Table table = testInstance.lookupForTable(entityMappingBuilder.getConfiguration(), TableNamingStrategy.DEFAULT);
		assertThat(table.getName()).isEqualTo("Realm");
	}
}
