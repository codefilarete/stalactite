package org.codefilarete.stalactite.spring.repository.query.bean;

import java.util.Set;

import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.spring.repository.StalactiteRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

/**
 * @author Guillaume Mary
 */
@Repository
public interface AnotherBeanQueriesRepository extends StalactiteRepository<Republic, Identifier<Long>> {

	// overridden by StalactiteRepositoryContextConfiguration.findEuropeanMember
	// because it is marked by @BeanQuery (without method name)
	Republic findEuropeanMember(@Param("presidentName") String presidentName);
	
	// could be in conflict with BeanQueriesRepository.findEuropeanCountryForPresident if @BeanQuery wasn't specifying the target
	// overridden by StalactiteRepositoryContextConfiguration.anotherOverrideOfFindEuropeanMemberWithPresidentName
	// because it is marked by @BeanQuery(method = "findEuropeanCountryForPresident", repositoryClass = AnotherBeanQueriesRepository.class)
	Set<Republic> findEuropeanCountryForPresident(@Param("presidentName") String presidentName);
	
	RepublicDto searchEuropeanMember(@Param("presidentName") String presidentName);
	
	class RepublicDto {
		
		private String name;
		
		private String presidentName;
		
		public RepublicDto() {
		}
		
		public RepublicDto(String name, String presidentName) {
			this.name = name;
			this.presidentName = presidentName;
		}
	}
}
