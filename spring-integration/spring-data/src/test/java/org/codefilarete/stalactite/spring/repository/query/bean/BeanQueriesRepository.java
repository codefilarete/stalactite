package org.codefilarete.stalactite.spring.repository.query.bean;

import java.util.Collection;
import java.util.Set;

import org.codefilarete.stalactite.engine.model.Republic;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.spring.repository.StalactiteRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
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
	
	Slice<Republic> findEuropeanMemberWithPresidentName_withSlice(@Param("presidentName") String presidentName, Pageable pageable);
	
	Page<Republic> findEuropeanMemberWithPresidentName_withPage(@Param("presidentName") String presidentName, Pageable pageable);
	
	// could be in conflict with AnotherBeanQueriesRepository.findEuropeanCountryForPresident if @BeanQuery wasn't specifying the target
	// overridden by StalactiteRepositoryContextConfiguration.anOverrideOfFindEuropeanMemberWithPresidentName
	// because it is marked by @BeanQuery(method = "findEuropeanCountryForPresident")
	Set<Republic> findEuropeanCountryForPresident(@Param("presidentName") String presidentName);
	
	<T> Collection<T> getByName(@Param("name") String name, Class<T> type);
	
	Set<NamesOnly> getByNameLikeOrderByPresidentNameAsc(@Param("name") String name);
	
	interface NamesOnly {
		
		String getName();
		
		SimplePerson getPresident();
		
		interface SimplePerson {
			
			String getName();
			
		}
	}
	
	// This class should create a query that retrieves the whole aggregate because it has a @Value annotation
	// Indeed, values required by the @Value annotations can't be known in advance, so the query must retrieve the whole aggregate
	interface NamesOnlyWithValue extends NamesOnly {
		
		@Value("#{target.president.name + '-' + target.president.id.delegate}")
		String getPresidentName();
	}

}
