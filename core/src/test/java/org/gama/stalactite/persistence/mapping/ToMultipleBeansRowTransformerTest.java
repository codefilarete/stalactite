package org.gama.stalactite.persistence.mapping;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Maps;
import org.gama.sql.result.Row;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Guillaume Mary
 */
public class ToMultipleBeansRowTransformerTest {
	
	@Test
	public void testTransform() throws Exception {
		Map<Long, Toto> aCache = new LinkedHashMap<>();
		Map<Long, Tata> bCache = new LinkedHashMap<>();
		Map<Long, Titi> cCache = new LinkedHashMap<>();
		
		// preparing instance
		// Column "toto.id" should trigger building (or retreive from cache) a Toto instance 
		// Column "tata.id" should trigger building (or retreive from cache) a Tata instance 
		// Column "titi.id" should trigger building (or retreive from cache) a Titi instance 
		// Finally, thanks to assemble, all of these instance are melt together
		ToMultipleBeansRowTransformer<Toto> testInstance = new ToMultipleBeansRowTransformer<Toto>(
				Maps.asMap("toto.id", (IRowTransformer) new ToEntityRowTransformer<>(Toto.class, "toto.id", "toto.name"))
				.add("tata.id", (IRowTransformer) new ToEntityRowTransformer<>(Tata.class, "tata.id", "tata.name"))
				.add("titi.id", (IRowTransformer) new ToEntityRowTransformer<>(Titi.class, "titi.id", "titi.name"))) {
			@Override
			protected Object getCachedInstance(String key, Object value) {
				return getCache(key).get(value);
			}
			
			@Override
			protected void onNewObject(String key, Object o) {
				// put it in the cache, nothing more
				getCache(key).put(((Entity) o).id, o);
			}
			
			@Override
			protected void assembly(Object[] rowAsObjects) {
				// final building for this row: we assemble objects. Simple case.
				Toto toto = (Toto) rowAsObjects[0];
				Tata tata = (Tata) rowAsObjects[1];
				Titi titi = (Titi) rowAsObjects[2];
				if (tata != null) {
					toto.tata = tata;
					if (titi != null) {
						tata.titis.add(titi);
					}
				}
			}
			
			// simple design to give a cache for a key (column)
			protected Map getCache(String key) {
				switch (key) {
					case "toto.id":
						return aCache;
					case "tata.id":
						return bCache;
					case "titi.id":
						return cCache;
					default:
						// impossible
						return null;
				}
			}
		};
		
		// Simple test: row contains only Toto data
		Row row = new Row();
		row.put("toto.id", 1L);
		row.put("toto.name", "Toto");
		
		testInstance.transform(row);
		
		assertEquals(aCache.size(), 1);
		assertEquals(aCache.get(1L).name, "Toto");
		
		// row contains Toto and Tata data
		row = new Row();
		row.put("toto.id", 1L);
		row.put("toto.name", "Toto");
		row.put("tata.id", 1L);
		row.put("tata.name", "Tata");
		
		testInstance.transform(row);
		
		assertEquals(aCache.size(), 1);
		assertEquals(bCache.size(), 1);
		assertEquals(aCache.get(1L).name, "Toto");
		assertEquals(bCache.get(1L).name, "Tata");
		
		// row contains Toto, Tata and Titi data
		row = new Row();
		row.put("toto.id", 1L);
		row.put("toto.name", "Toto");
		row.put("tata.id", 1L);
		row.put("tata.name", "Tata");
		row.put("titi.id", 1L);
		row.put("titi.name", "Titi");
		
		testInstance.transform(row);
		
		assertEquals(aCache.size(), 1);
		assertEquals(bCache.size(), 1);
		assertEquals(cCache.size(), 1);
		assertEquals(aCache.get(1L).name, "Toto");
		assertEquals(bCache.get(1L).name, "Tata");
		assertEquals(cCache.get(1L).name, "Titi");
		
		Toto expectedToto = aCache.get(1L);
		Tata expectedTata = bCache.get(1L);
		Titi expectedTiti = cCache.get(1L);
		assertEquals(expectedToto.tata, expectedTata);
		assertEquals(expectedTata.titis, Arrays.asList(expectedTiti));
		
		
	}
	
	private static class Entity {
		protected Long id;
		protected String name;
	}
	
	private static class Toto extends Entity {
		private Tata tata;
		public Toto() {}
	}
	
	private static class Tata extends Entity {
		private List<Titi> titis = new ArrayList<>();
		public Tata() {}
	}
	
	private static class Titi extends Entity {
		public Titi() {}
	}
	
	private static class ToEntityRowTransformer<E extends Entity> extends AbstractTransformer<E> {
		
		private final String idColumn;
		private final String nameColumn;
		
		public ToEntityRowTransformer(Class<E> clazz, String idColumn, String nameColumn) throws NoSuchMethodException {
			super(clazz);
			this.idColumn = idColumn;
			this.nameColumn = nameColumn;
		}
		
		@Override
		public void applyRowToBean(Row row, E rowBean) {
			rowBean.id = (Long) row.get(idColumn);
			rowBean.name = (String) row.get(nameColumn);
		}
	}
	
}