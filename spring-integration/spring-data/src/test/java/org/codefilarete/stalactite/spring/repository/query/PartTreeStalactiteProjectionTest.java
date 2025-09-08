package org.codefilarete.stalactite.spring.repository.query;

import java.util.HashMap;
import java.util.Map;

import org.codefilarete.reflection.AccessorChain;
import org.codefilarete.stalactite.spring.repository.query.PartTreeStalactiteProjectionTest.UserAddressStructure.Address;
import org.codefilarete.stalactite.spring.repository.query.PartTreeStalactiteProjectionTest.UserAddressStructure.Street;
import org.codefilarete.stalactite.spring.repository.query.PartTreeStalactiteProjectionTest.UserAddressStructure.User;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PartTreeStalactiteProjectionTest {

	@Test
	void buildHierarchicMapsFromDotProperties() {
		HashMap<String, Object> root = new HashMap<>();
		PartTreeStalactiteProjection.buildHierarchicMap(AccessorChain.fromMethodReferences(
						UserAddressStructure::getUser, User::getAddress, Address::getStreet, Street::getName
				), "Main Street", root);

		// Verify the hierarchical structure
		assertThat(root).containsKey("user");
		assertThat((Map<String, Object>) root.get("user")).containsKey("address");
		assertThat((Map<String, Object>) ((Map<String, Object>) root.get("user")).get("address")).containsKey("street");
		assertThat((Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) root.get("user")).get("address")).get("street")).containsEntry("name", "Main Street");

		PartTreeStalactiteProjection.buildHierarchicMap(AccessorChain.fromMethodReferences(
				UserAddressStructure::getUser, User::getAddress, Address::getStreet, Street::getNumber
		), "42", root);

		assertThat(root).containsKey("user");
		assertThat((Map<String, Object>) root.get("user")).containsKey("address");
		assertThat((Map<String, Object>) ((Map<String, Object>) root.get("user")).get("address")).containsKey("street");
		assertThat((Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) root.get("user")).get("address")).get("street")).containsEntry("name", "Main Street");
	}
	
	/**
	 * Test classes representing the hierarchical structure for user.address.street.name property path
	 */
	public class UserAddressStructure {
		
		private User user;
		
		public User getUser() {
			return user;
		}
		
		public class User {
			private Address address;
			
			public Address getAddress() {
				return address;
			}
		}
		
		public class Address {
			private Street street;
			
			public Street getStreet() {
				return street;
			}
		}
		
		public class Street {
			private String name;
			private String number;
			
			public String getName() {
				return name;
			}
			
			public String getNumber() {
				return number;
			}
		}
	}
}