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
	Set<Republic> findEuropeanMember(@Param("presidentName") String presidentName);
	
	// overridden by StalactiteRepositoryContextConfiguration.anotherOverrideOfFindEuropeanMemberWithPresidentName
	// because it is marked by @BeanQuery(method = "findEuropeanCountryForPresident", repositoryClass = AnotherBeanQueriesRepository.class)
	Set<Republic> findEuropeanCountryForPresident(@Param("presidentName") String presidentName);
}
