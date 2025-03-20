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
public interface BeanQueriesRepository extends StalactiteRepository<Republic, Identifier<Long>> {

	// overridden by StalactiteRepositoryContextConfiguration.aMethodThatDoesntMatchAnyRepositoryMethodName
	// because it is marked by @BeanQuery(method = "findEuropeanMemberWithPresidentName")
	Set<Republic> findEuropeanMemberWithPresidentName(@Param("presidentName") String presidentName);

	// overridden by StalactiteRepositoryContextConfiguration.anOverrideOfFindEuropeanMemberWithPresidentName
	// because it is marked by @BeanQuery(method = "findEuropeanCountryForPresident")
	Set<Republic> findEuropeanCountryForPresident(@Param("presidentName") String presidentName);

}
