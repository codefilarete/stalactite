package org.codefilarete.stalactite.dsl.naming;

import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration.Linkage;
import org.codefilarete.stalactite.engine.model.AbstractVehicle;
import org.codefilarete.stalactite.engine.model.PersonWithGender;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

class IndexNamingStrategyTest {
	
	static Iterable<Arguments> defaultImplementation_data() {
		Linkage abstractVehicleTimestamp = Mockito.mock(Linkage.class);
		when(abstractVehicleTimestamp.getAccessor()).thenReturn(PropertyAccessor.fromMethodReference(
				AbstractVehicle::getTimestamp, AbstractVehicle::setTimestamp
		));
		Linkage personMainBicycle = Mockito.mock(Linkage.class);
		when(personMainBicycle.getAccessor()).thenReturn(PropertyAccessor.fromMethodReference(
				PersonWithGender::getMainBicycle, PersonWithGender::setMainBicycle
		));
		Linkage personMainBicycle_withOverriddenName = Mockito.mock(Linkage.class);
		when(personMainBicycle_withOverriddenName.getAccessor()).thenReturn(PropertyAccessor.fromMethodReference(
				PersonWithGender::getMainBicycle, PersonWithGender::setMainBicycle
		));
		when(personMainBicycle_withOverriddenName.getColumnName()).thenReturn("principalBike");
		Linkage PersonWithGenderGender_withOverriddenName = Mockito.mock(Linkage.class);
		when(PersonWithGenderGender_withOverriddenName.getAccessor()).thenReturn(PropertyAccessor.fromMethodReference(
				PersonWithGender::getGender, PersonWithGender::setGender
		));
		when(PersonWithGenderGender_withOverriddenName.getColumnName()).thenReturn("GENDER");
		Linkage personMainBicycle_withOverriddenUpperCasedName = Mockito.mock(Linkage.class);
		when(personMainBicycle_withOverriddenUpperCasedName.getAccessor()).thenReturn(PropertyAccessor.fromMethodReference(
				PersonWithGender::getMainBicycle, PersonWithGender::setMainBicycle
		));
		when(personMainBicycle_withOverriddenUpperCasedName.getColumnName()).thenReturn("PRINCIPALBike");
		return Arrays.asList(
				arguments(abstractVehicleTimestamp, "abstract_vehicle_timestamp_key"),
				arguments(personMainBicycle, "person_main_bicycle_key"),
				arguments(personMainBicycle_withOverriddenName, "person_principal_bike_key"),
				arguments(PersonWithGenderGender_withOverriddenName, "person_with_gender_gender_key"),
				arguments(personMainBicycle_withOverriddenName, "person_principal_bike_key"),
				arguments(personMainBicycle_withOverriddenUpperCasedName, "person_principalbike_key")
		);
	}
	
	@ParameterizedTest
	@MethodSource("defaultImplementation_data")
	void defaultImplementation(Linkage linkage, String expectedIndexName) {
		String indexName = IndexNamingStrategy.DEFAULT.giveName(linkage);
		assertThat(indexName).isEqualTo(expectedIndexName);
	}
	
}