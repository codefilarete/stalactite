package org.codefilarete.stalactite.persistence.engine;

import org.codefilarete.stalactite.persistence.engine.configurer.FluentEmbeddableMappingConfigurationSupport;
import org.codefilarete.stalactite.persistence.engine.model.Country;
import org.codefilarete.stalactite.persistence.engine.model.Person;
import org.codefilarete.stalactite.persistence.engine.model.PersonWithGender;
import org.codefilarete.stalactite.persistence.engine.model.Timestamp;
import org.junit.jupiter.api.Test;

/**
 * @author Guillaume Mary
 */
class FluentEmbeddableMappingConfigurationSupportTest {
	/**
	 * Test to check that the API returns right Object which means:
	 * - interfaces are well written to return right types, so one can chain others methods
	 * - at runtime instance of the right type is also returned
	 * (avoid "java.lang.ClassCastException: com.sun.proxy.$Proxy10 cannot be cast to org.codefilarete.stalactite.persistence.engine
	 * .FluentEmbeddableMappingBuilder")
	 * <p>
	 * As many as possible combinations of method chaining should be done here, because all combination seems impossible, this test must be
	 * considered
	 * as a best effort, and any regression found in user code should be added here
	 */
	
	@Test
	void apiUsage() {
		try {
			MappingEase.embeddableBuilder(Country.class)
					.map(Country::getName)
					.embed(Country::getPresident, MappingEase.embeddableBuilder(Person.class)
							.map(Person::getName)
							.embed(Person::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
									.map(Timestamp::getCreationDate)
									.map(Timestamp::getModificationDate)))
					.overrideName(Person::getId, "personId")
					.overrideName(Person::getName, "personName")
					
					.embed(Country::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.map(Country::getId)
					.map(Country::setDescription, "zxx")
					.mapSuperClass(new FluentEmbeddableMappingConfigurationSupport<>(Object.class));
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			MappingEase.embeddableBuilder(Country.class)
					.map(Country::getName)
					.embed(Country::getPresident, MappingEase.embeddableBuilder(Person.class)
							.map(Person::getName)
							.embed(Person::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
									.map(Timestamp::getCreationDate).mandatory()
									.map(Timestamp::getModificationDate)))
					.embed(Country::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.map(Country::getId, "zz")
					.mapSuperClass(MappingEase.embeddableBuilder(Object.class))
					.map(Country::getDescription, "xx");
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			MappingEase.embeddableBuilder(Country.class)
					.map(Country::getName)
					.map(Country::getId, "zz")
					.mapSuperClass(new FluentEmbeddableMappingConfigurationSupport<>(Object.class))
					// embed with setter
					.embed(Country::setPresident, MappingEase.embeddableBuilder(Person.class)
							.map(Person::getName)// inner embed with setter
							.embed(Person::setTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
									.map(Timestamp::getCreationDate)
									.map(Timestamp::getModificationDate)))
					// embed with setter
					.embed(Country::setTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.map(Country::getDescription, "xx")
					.map(Country::getDummyProperty, "dd");
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			EmbeddableMappingConfigurationProvider<Person> personMappingBuilder = MappingEase.embeddableBuilder(Person.class)
					.map(Person::getName);
			
			MappingEase.embeddableBuilder(Country.class)
					.map(Country::getName)
					.map(Country::getId, "zz")
					.mapSuperClass(new FluentEmbeddableMappingConfigurationSupport<>(Object.class))
					// embed with setter
					.embed(Country::getPresident, personMappingBuilder);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			EmbeddableMappingConfigurationProvider<Person> personMappingBuilder = MappingEase.embeddableBuilder(Person.class)
					.map(Person::getName);
			
			MappingEase.embeddableBuilder(Country.class)
					.map(Country::getName)
					.map(Country::getId, "zz")
					.embed(Country::getPresident, personMappingBuilder)
					.mapSuperClass(new FluentEmbeddableMappingConfigurationSupport<>(Object.class))
					// reusing embeddable ...
					.embed(Country::getPresident, personMappingBuilder)
					// with getter override
					.overrideName(Person::getName, "toto")
					// with setter override
					.overrideName(Person::setName, "tata");
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			MappingEase.embeddableBuilder(PersonWithGender.class)
					.map(Person::getName)
					.mapEnum(PersonWithGender::getGender).byOrdinal()
					.embed(Person::setTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.overrideName(Timestamp::getCreationDate, "myDate")
					.mapEnum(PersonWithGender::getGender, "MM").byOrdinal()
					.mapEnum(PersonWithGender::getGender, "MM").mandatory()
					.map(PersonWithGender::getId, "zz")
					.mapEnum(PersonWithGender::setGender).byName()
					.embed(Person::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.map(Timestamp::getCreationDate)
							.map(Timestamp::getModificationDate))
					.mapEnum(PersonWithGender::setGender, "MM").byName();
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
	}
}