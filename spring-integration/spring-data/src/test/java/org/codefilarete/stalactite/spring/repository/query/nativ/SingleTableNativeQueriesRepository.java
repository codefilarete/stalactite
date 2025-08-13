package org.codefilarete.stalactite.spring.repository.query.nativ;

import java.util.Set;

import org.codefilarete.stalactite.engine.model.Country;
import org.codefilarete.stalactite.id.Identifier;
import org.codefilarete.stalactite.spring.repository.StalactiteRepository;
import org.codefilarete.stalactite.spring.repository.query.NativeQuery;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

/**
 * @author Guillaume Mary
 */
@Repository
public interface SingleTableNativeQueriesRepository extends StalactiteRepository<Country, Identifier<Long>> {
	
	// comparing to the original query, we set Country table alias
	@NativeQuery(value = "select"
			+ " C.name as Country_name,"
			+ " C.euMember as Country_euMember,"
			+ " C.id as Country_id,"
			+ " C.presidentId as Country_presidentId,"
			+ " C.deputeCount as Country_deputeCount,"
			+ " C.kingId as Country_kingId,"
			+ " C.DTYPE as Country_DTYPE,"
			+ " Realm_king.name as king_name,"
			+ " Realm_king.id as king_id,"
			+ " president.name as president_name,"
			+ " president.id as president_id,"
			+ " president_vehicle.color as president_vehicle_color,"
			+ " president_vehicle.id as president_vehicle_id,"
			+ " Country_languages_Language.code as Country_languages_Language_code,"
			+ " Country_languages_Language.id as Country_languages_Language_id"
			+ " from"
			+ " Country as C"
			+ " left outer join Person as president on C.presidentId = president.id"
			+ " left outer join Country_languages as Country_languages on C.id = Country_languages.country_id"
			+ " left outer join Vehicle as president_vehicle on president.vehicleId = president_vehicle.id"
			+ " left outer join Language as Country_languages_Language on Country_languages.languages_id = Country_languages_Language.id"
			+ " left outer join King as Realm_king on C.kingId = Realm_king.id"
			+ " where"
			+ " C.id in (:ids)")
	Set<Country> loadByIdIn(@Param("ids") Identifier<Long>... ids);
	
}