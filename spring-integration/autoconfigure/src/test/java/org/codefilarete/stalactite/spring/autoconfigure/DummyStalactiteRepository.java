package org.codefilarete.stalactite.spring.autoconfigure;

import org.codefilarete.stalactite.spring.autoconfigure.DummyStalactiteRepository.DummyData;
import org.codefilarete.stalactite.spring.repository.StalactiteRepository;
import org.springframework.stereotype.Repository;

/**
 * @author Guillaume Mary
 */
@Repository
public interface DummyStalactiteRepository extends StalactiteRepository<DummyData, Long> {
	
	class DummyData {
		
		private Long id;
		private String name;
		
		public Long getId() {
			return id;
		}
		
		public void setId(long id) {
			this.id = id;
		}
		
		public String getName() {
			return name;
		}
		
		public void setName(String name) {
			this.name = name;
		}
	}
}
