package org.codefilarete.stalactite.dsl.naming;

import org.codefilarete.reflection.PropertyAccessor;
import org.codefilarete.reflection.ReversibleAccessor;
import org.codefilarete.stalactite.dsl.embeddable.EmbeddableMappingConfiguration.Linkage;
import org.codefilarete.stalactite.engine.model.AbstractVehicle;
import org.codefilarete.stalactite.engine.model.Bicycle;
import org.codefilarete.stalactite.engine.model.Gender;
import org.codefilarete.stalactite.engine.model.PersonWithGender;
import org.codefilarete.stalactite.engine.model.Timestamp;
import org.codefilarete.stalactite.sql.ddl.structure.Column;
import org.codefilarete.stalactite.sql.ddl.structure.Table;
import org.codefilarete.tool.collection.Arrays;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class UniqueConstraintNamingStrategyTest {
	
	static Iterable<Arguments> defaultImplementation_data() {
		Table abstractVehicleTable = new Table(null, "AbstractVehicle");
		Column timestampColumn = abstractVehicleTable.addColumn("timestamp", String.class);
		PropertyAccessor<AbstractVehicle, Timestamp> timestampPropertyAccessor = PropertyAccessor.fromMethodReference(
				AbstractVehicle::getTimestamp, AbstractVehicle::setTimestamp
		);
		
		Table personTable = new Table(null, "Person");
		Column mainBicycleColumn = personTable.addColumn("mainBicycle", String.class);
		PropertyAccessor<PersonWithGender, Bicycle> mainBicyclePropertyAccessor = PropertyAccessor.fromMethodReference(
				PersonWithGender::getMainBicycle, PersonWithGender::setMainBicycle
		);
		Column principalBikeColumn = personTable.addColumn("principalBike", String.class);
		Table personWithGenderTable = new Table(null, "PersonWithGender");
		Column genderColumn = personWithGenderTable.addColumn("gender", String.class);
		Linkage personWithGenderGender_withOverriddenName = Mockito.mock(Linkage.class);
		PropertyAccessor<PersonWithGender, Gender> genderPropertyAccessor = PropertyAccessor.fromMethodReference(
				PersonWithGender::getGender, PersonWithGender::setGender
		);
		return Arrays.asList(
				arguments(timestampPropertyAccessor, timestampColumn, "abstract_vehicle_timestamp_key"),
				arguments(mainBicyclePropertyAccessor, mainBicycleColumn, "person_main_bicycle_key"),
				arguments(mainBicyclePropertyAccessor, principalBikeColumn, "person_principal_bike_key"),
				arguments(genderPropertyAccessor, genderColumn, "person_with_gender_gender_key"),
				arguments(mainBicyclePropertyAccessor, principalBikeColumn, "person_principal_bike_key")
		);
	}
	
	@ParameterizedTest
	@MethodSource("defaultImplementation_data")
	void defaultImplementation(ReversibleAccessor accessor, Column column, String expectedIndexName) {
		String constraintName = UniqueConstraintNamingStrategy.DEFAULT.giveName(accessor, column);
		assertThat(constraintName).isEqualTo(expectedIndexName);
	}
	
}
