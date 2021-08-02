package org.gama.stalactite.persistence.engine;

import org.gama.stalactite.persistence.engine.configurer.FluentEmbeddableMappingConfigurationSupport;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.engine.model.Person;
import org.gama.stalactite.persistence.engine.model.PersonWithGender;
import org.gama.stalactite.persistence.engine.model.Timestamp;
import org.junit.jupiter.api.Test;

/**
 * @author Guillaume Mary
 */
class FluentEmbeddableMappingConfigurationSupportTest {
	/**
	 * Test to check that the API returns right Object which means:
	 * - interfaces are well written to return right types, so one can chain others methods
	 * - at runtime instance of the right type is also returned
	 * (avoid "java.lang.ClassCastException: com.sun.proxy.$Proxy10 cannot be cast to org.gama.stalactite.persistence.engine
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
					.add(Country::getName)
					.embed(Country::getPresident, MappingEase.embeddableBuilder(Person.class)
							.add(Person::getName)
							.embed(Person::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
									.add(Timestamp::getCreationDate)
									.add(Timestamp::getModificationDate)))
					.overrideName(Person::getId, "personId")
					.overrideName(Person::getName, "personName")
					
					.embed(Country::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.add(Timestamp::getCreationDate)
							.add(Timestamp::getModificationDate))
					.add(Country::getId)
					.add(Country::setDescription, "zxx")
					.mapSuperClass(new FluentEmbeddableMappingConfigurationSupport<>(Object.class));
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			MappingEase.embeddableBuilder(Country.class)
					.add(Country::getName)
					.embed(Country::getPresident, MappingEase.embeddableBuilder(Person.class)
							.add(Person::getName)
							.embed(Person::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
									.add(Timestamp::getCreationDate).mandatory()
									.add(Timestamp::getModificationDate)))
					.embed(Country::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.add(Timestamp::getCreationDate)
							.add(Timestamp::getModificationDate))
					.add(Country::getId, "zz")
					.mapSuperClass(MappingEase.embeddableBuilder(Object.class))
					.add(Country::getDescription, "xx");
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			MappingEase.embeddableBuilder(Country.class)
					.add(Country::getName)
					.add(Country::getId, "zz")
					.mapSuperClass(new FluentEmbeddableMappingConfigurationSupport<>(Object.class))
					// embed with setter
					.embed(Country::setPresident, MappingEase.embeddableBuilder(Person.class)
							.add(Person::getName)// inner embed with setter
							.embed(Person::setTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
									.add(Timestamp::getCreationDate)
									.add(Timestamp::getModificationDate)))
					// embed with setter
					.embed(Country::setTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.add(Timestamp::getCreationDate)
							.add(Timestamp::getModificationDate))
					.add(Country::getDescription, "xx")
					.add(Country::getDummyProperty, "dd");
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			EmbeddableMappingConfigurationProvider<Person> personMappingBuilder = MappingEase.embeddableBuilder(Person.class)
					.add(Person::getName);
			
			MappingEase.embeddableBuilder(Country.class)
					.add(Country::getName)
					.add(Country::getId, "zz")
					.mapSuperClass(new FluentEmbeddableMappingConfigurationSupport<>(Object.class))
					// embed with setter
					.embed(Country::getPresident, personMappingBuilder);
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
		
		try {
			EmbeddableMappingConfigurationProvider<Person> personMappingBuilder = MappingEase.embeddableBuilder(Person.class)
					.add(Person::getName);
			
			MappingEase.embeddableBuilder(Country.class)
					.add(Country::getName)
					.add(Country::getId, "zz")
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
					.add(Person::getName)
					.addEnum(PersonWithGender::getGender).byOrdinal()
					.embed(Person::setTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.add(Timestamp::getCreationDate)
							.add(Timestamp::getModificationDate))
					.overrideName(Timestamp::getCreationDate, "myDate")
					.addEnum(PersonWithGender::getGender, "MM").byOrdinal()
					.addEnum(PersonWithGender::getGender, "MM").mandatory()
					.add(PersonWithGender::getId, "zz")
					.addEnum(PersonWithGender::setGender).byName()
					.embed(Person::getTimestamp, MappingEase.embeddableBuilder(Timestamp.class)
							.add(Timestamp::getCreationDate)
							.add(Timestamp::getModificationDate))
					.addEnum(PersonWithGender::setGender, "MM").byName();
		} catch (RuntimeException e) {
			// Since we only want to test compilation, we don't care about that the above code throws an exception or not
		}
	}
}