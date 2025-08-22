package org.codefilarete.stalactite.spring.repository.query;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PartTreeStalactiteProjectionTest {

	@Test
	void buildHierarchicMapsFromDotProperties() {
		HashMap<String, Object> root = new HashMap<>();
		PartTreeStalactiteProjection.buildHierarchicMap("user.address.street.name", "Main Street", root);

		// Verify the hierarchical structure
		assertThat(root).containsKey("user");
		assertThat((Map<String, Object>) root.get("user")).containsKey("address");
		assertThat((Map<String, Object>) ((Map<String, Object>) root.get("user")).get("address")).containsKey("street");
		assertThat((Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) root.get("user")).get("address")).get("street")).containsEntry("name", "Main Street");

		PartTreeStalactiteProjection.buildHierarchicMap("user.address.street.number", "42", root);

		assertThat(root).containsKey("user");
		assertThat((Map<String, Object>) root.get("user")).containsKey("address");
		assertThat((Map<String, Object>) ((Map<String, Object>) root.get("user")).get("address")).containsKey("street");
		assertThat((Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) root.get("user")).get("address")).get("street")).containsEntry("name", "Main Street");
	}
}