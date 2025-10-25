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
		return Arrays.asList(
				arguments(abstractVehicleTimestamp, "abstract_vehicle_timestamp_key"),
				arguments(personMainBicycle, "person_main_bicycle_key")
				);
	}
	
	@ParameterizedTest
	@MethodSource("defaultImplementation_data")
	void defaultImplementation(Linkage linkage, String expectedIndexName) {
		String indexName = IndexNamingStrategy.DEFAULT.giveName(linkage);
		assertThat(indexName).isEqualTo(expectedIndexName);
	}
	
}