package org.gama.stalactite.persistence.engine;

import org.gama.lang.collection.Arrays;
import org.gama.reflection.AbstractReflector;
import org.gama.reflection.AccessorByMethod;
import org.gama.reflection.AccessorByMethodReference;
import org.gama.reflection.AccessorChain;
import org.gama.reflection.MutatorByMethod;
import org.gama.reflection.MutatorByMethodReference;
import org.gama.stalactite.persistence.engine.AccessorChainComparator.AccessComparator;
import org.gama.stalactite.persistence.engine.AccessorChainComparator.MemberDefinition;
import org.gama.stalactite.persistence.engine.model.Country;
import org.gama.stalactite.persistence.engine.model.Person;
import org.gama.stalactite.persistence.engine.model.Timestamp;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.gama.reflection.Accessors.accessorByField;
import static org.gama.reflection.Accessors.mutatorByField;
import static org.gama.stalactite.persistence.engine.AccessorChainComparator.giveMemberDefinition;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Guillaume Mary
 */
class AccessorChainComparatorTest {
	
	static Object[][] testAccessorComparator() {
		return new Object[][] {
				// ACCESSOR vs ACCESSOR
				// field vs method
				{ accessorByField(Country.class, "name"), accessorByField(Country.class, "name"), true },
				{ accessorByField(Country.class, "name"), new AccessorByMethod<>(Country.class, "getName"), true },
				{ accessorByField(Country.class, "name"), new AccessorByMethodReference<>(Country::getName), true },
				
				// field vs method with same owning type 
				{ accessorByField(Country.class, "name"), new AccessorByMethod<>(Country.class, "getDescription"), false },
				{ accessorByField(Country.class, "name"), new AccessorByMethodReference<>(Country::getDescription), false },
				
				// members with same name but different owner
				{ accessorByField(Country.class, "name"), accessorByField(Person.class, "name"), false },
				{ accessorByField(Country.class, "name"), new AccessorByMethod<>(Person.class, "getName"), false },
				{ accessorByField(Country.class, "name"), new AccessorByMethodReference<>(Person::getName), false },
				
				// MUTATOR vs MUTATOR
				// field vs method
				{ mutatorByField(Country.class, "name"), mutatorByField(Country.class, "name"), true },
				{ mutatorByField(Country.class, "name"), new MutatorByMethod<>(Country.class, "setName", String.class), true },
				{ mutatorByField(Country.class, "name"), new MutatorByMethodReference<>(Country::setName), true },
				
				// field vs method with same owning type 
				{ mutatorByField(Country.class, "name"), new MutatorByMethod<>(Country.class, "setDescription", String.class), false },
				{ mutatorByField(Country.class, "name"), new MutatorByMethodReference<>(Country::setDescription), false },
				
				// members with same name but different owner
				{ mutatorByField(Country.class, "name"), mutatorByField(Person.class, "name"), false },
				{ mutatorByField(Country.class, "name"), new MutatorByMethod<>(Person.class, "setName", String.class), false },
				{ mutatorByField(Country.class, "name"), new MutatorByMethodReference<>(Person::setName), false },
				
				// MUTATOR vs ACCESSOR
				{ mutatorByField(Country.class, "name"), accessorByField(Country.class, "name"), true },
				{ mutatorByField(Country.class, "name"), new AccessorByMethod<>(Country.class, "getName"), true },
				{ mutatorByField(Country.class, "name"), new AccessorByMethodReference<>(Country::getName), true },
				
				{ mutatorByField(Country.class, "name"), new AccessorByMethod<>(Country.class, "getDescription"), false },
				{ mutatorByField(Country.class, "name"), new AccessorByMethodReference<>(Country::getDescription), false },
			
		};
	}
	
	@ParameterizedTest
	@MethodSource("testAccessorComparator")
	void testAccessorComparator(AbstractReflector accessor1, AbstractReflector accessor2, boolean expectedEquality) {
		AccessComparator testInstance = AccessorChainComparator.accessComparator();
		assertEquals(expectedEquality, testInstance.compare(accessor1, accessor2) == 0);
	}
	
	static Object[][] testGiveMemberDefinition() {
		return new Object[][] {
				// accessor
				{ accessorByField(Country.class, "name"), Country.class, "name", String.class },
				{ new AccessorByMethod<>(Country.class, "getName"), Country.class, "name", String.class },
				{ new AccessorByMethodReference<>(Country::getName), Country.class, "name", String.class },
				
				// mutator
				{ mutatorByField(Country.class, "name"), Country.class, "name", String.class },
				{ new MutatorByMethod<>(Country.class, "setName", String.class), Country.class, "name", String.class },
				{ new MutatorByMethodReference<>(Country::setName), Country.class, "name", String.class },
				
				{ new AccessorChain<>(Arrays.asList(new AccessorByMethodReference<>(Country::getPresident), new AccessorByMethodReference<>(Person::getTimestamp))),
						Country.class, "president.timestamp", Timestamp.class },
		};
	}
	
	@ParameterizedTest
	@MethodSource("testGiveMemberDefinition")
	void testGiveMemberDefinition(Object o, Class expectedDeclaringClass, String expectedName, Class expectedMemberType) {
		MemberDefinition memberDefinition = giveMemberDefinition(o);
		assertEquals(expectedDeclaringClass, memberDefinition.getDeclaringClass());
		assertEquals(expectedName, memberDefinition.getName());
		assertEquals(expectedMemberType, memberDefinition.getMemberType());
	}
	
//	@Test
//	void testXX() {
//		assertEquals(Arrays.asList("name, createDate"), xx(Arrays.asHashSet(
//				new AccessorChain<>(
//						accessorByMethodReference(Country::getCapital),
//						accessorByMethodReference(City::getName)),
//				new AccessorChain<>(
//						accessorByMethodReference(Country::getPresident),
//						accessorByMethodReference(Person::getName)),
//				new AccessorChain<>(
//						accessorByMethodReference(Country::getTimestamp),
//						accessorByMethodReference(Timestamp::getCreationDate)),
//				new AccessorChain<>(
//						accessorByMethodReference(Country::getPresident),
//						accessorByMethodReference(Person::getTimestamp),
//						accessorByMethodReference(Timestamp::getCreationDate))
//				))
//		);
//	}
}