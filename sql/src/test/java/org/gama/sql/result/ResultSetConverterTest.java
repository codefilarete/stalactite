package org.gama.sql.result;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.gama.lang.ThreadLocals;
import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.lang.function.Functions;
import org.gama.lang.function.ThrowingRunnable;
import org.gama.sql.result.ResultSetConverterTest.WingInner.FeatherInner;
import org.gama.sql.result.ResultSetRowConverterTest.Person;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.tngtech.java.junit.dataprovider.DataProviders.$;
import static com.tngtech.java.junit.dataprovider.DataProviders.$$;
import static org.gama.sql.binder.DefaultResultSetReaders.INTEGER_PRIMITIVE_READER;
import static org.gama.sql.binder.DefaultResultSetReaders.STRING_READER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/**
 * @author Guillaume Mary
 */
@RunWith(DataProviderRunner.class)
public class ResultSetConverterTest {
	
	@Test
	public void testTranform() throws SQLException {
		String chickenInstanciationColumnName = "chickenName";
		String leftFeatherColorColumnName = "leftFeatherColor";
		ResultSetConverter<String, Chicken> testInstance = new ResultSetConverter<>(Chicken.class, chickenInstanciationColumnName, STRING_READER, Chicken::new);
		testInstance.add(leftFeatherColorColumnName, STRING_READER,
				(chicken, colorName) -> chicken.getLeftWing().add(new Feather(new FeatherColor(colorName)))
		);
		
		// a ResultSet that retrieves all the feathers of a unique Chicken
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.asMap(chickenInstanciationColumnName, (Object) "rooster")
						.add("leftFeatherNumber", 1)
						.add(leftFeatherColorColumnName, "red"),
				Maps.asMap(chickenInstanciationColumnName, (Object) "rooster")
						.add("leftFeatherNumber", 1)
						.add(leftFeatherColorColumnName, "black")
		));
		
		// we must simulate bean caching done by ResultSetConverter.convert(..), otherwise we get NullPointerException
		ThreadLocals.doWithThreadLocal(ResultSetConverter.BEAN_CACHE, SimpleBeanCache::new, (ThrowingRunnable<SQLException>) () -> {
			// from first row, a new instance of Chicken is created named "rooster", it has 1 red feather
			resultSet.next();
			Chicken result = testInstance.transform(resultSet);
			assertEquals("rooster", result.getName());
			assertEquals(Arrays.asList("red"), result.getLeftWing().getFeathers().stream()
					.map(Functions.link(Feather::getColor, FeatherColor::getName)).collect(Collectors.toList()));
			
			// from second row, the previous instance of Chicken is kept (not new), and a new black feather was added
			resultSet.next();
			Chicken result1 = testInstance.transform(resultSet);
			assertSame(result, result1);
			assertEquals(2, result.getLeftWing().getFeathers().size());
			assertEquals(Arrays.asList("red", "black"), result.getLeftWing().getFeathers().stream()
					.map(Functions.link(Feather::getColor, FeatherColor::getName)).collect(Collectors.toList()));
		});
	}
	
	@Test
	public void testTranform_shareBeanInstances() throws SQLException {
		String chickenInstanciationColumnName = "chickenName";
		String leftFeatherColorColumnName = "leftFeatherColor";
		String rightFeatherColorColumnName = "rightFeatherColor";
		ResultSetConverter<String, Chicken> testInstance = new ResultSetConverter<>(Chicken.class, chickenInstanciationColumnName, STRING_READER, Chicken::new);
		testInstance.add(leftFeatherColorColumnName, STRING_READER, FeatherColor.class, FeatherColor::new, (chicken, color) -> {
			if (color != null) {	// prevent addition of Feather with a null color
				chicken.getLeftWing().add(new Feather(color));
			}
		});
		// we add almost the same as previous but for the right wing : the color must be shared between wings
		testInstance.add(rightFeatherColorColumnName, STRING_READER, FeatherColor.class, FeatherColor::new, (chicken, color) -> {
			if (color != null) {	// prevent addition of Feather with a null color
				chicken.getRightWing().add(new Feather(color));
			}
		});
		
		// a ResultSet that retrieves all the feathers of a unique Chicken
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.asMap(chickenInstanciationColumnName, (Object) "rooster")
						.add(rightFeatherColorColumnName, null).add(leftFeatherColorColumnName, "red"),
				Maps.asMap(chickenInstanciationColumnName, (Object) "rooster")
						.add(rightFeatherColorColumnName, null).add(leftFeatherColorColumnName, "black"),
				Maps.asMap(chickenInstanciationColumnName, (Object) "rooster")
						.add(rightFeatherColorColumnName, "black").add(leftFeatherColorColumnName, null)
		));
		
		// we must simulate bean caching done by ResultSetConverter.convert(..), otherwise we get NullPointerException
		ThreadLocals.doWithThreadLocal(ResultSetConverter.BEAN_CACHE, SimpleBeanCache::new, (ThrowingRunnable<SQLException>) () -> {
			// from first row, a new instance of Chicken is created named "rooster", it has 1 red feather on left wing
			resultSet.next();
			Chicken result = testInstance.transform(resultSet);
			assertEquals("rooster", result.getName());
			assertEquals(Arrays.asList("red"), result.getLeftWing().getFeathers().stream()
					.map(Functions.link(Feather::getColor, FeatherColor::getName)).collect(Collectors.toList()));
			
			// from second row, the previous instance of Chicken is kept (not new), and a new black feather was added on left wing
			resultSet.next();
			Chicken result1 = testInstance.transform(resultSet);
			assertSame(result, result1);
			assertEquals(Arrays.asList("red", "black"), result.getLeftWing().getFeathers().stream()
					.map(Functions.link(Feather::getColor, FeatherColor::getName)).collect(Collectors.toList()));
			
			// from third row, checking that right wing is black and is not polluted by any null value or whatever
			resultSet.next();
			Chicken result2 = testInstance.transform(resultSet);
			assertSame(result, result2);
			assertEquals(Arrays.asList("black"), result.getRightWing().getFeathers().stream()
					.map(Functions.link(Feather::getColor, FeatherColor::getName)).collect(Collectors.toList()));
			
			// checking that wings share the same color instance
			Function<Feather, String> colorNameAccessor = Functions.link(Feather::getColor, FeatherColor::getName);
			Map<String, FeatherColor> leftWingFeatherColors = Iterables.map(result.getLeftWing().getFeathers(), colorNameAccessor, Feather::getColor);
			
			Map<String, FeatherColor> rightWingFeatherColors = Iterables.map(result.getRightWing().getFeathers(), colorNameAccessor, Feather::getColor);
			assertSame(leftWingFeatherColors.get("black"), rightWingFeatherColors.get("black"));
		});
	}
	
	/**
	 * Test to demonstrate that inner classes can also be assembled.
	 * WingInner has inner instances of FeatherInner, they are automatically added to they enclosing WingInner instance through their constructor. 
	 */
	@Test
	public void testTranform_withInnerClass() throws SQLException {
		String wingInstanciationColumnName = "wingName";
		String leftFeatherColorColumnName = "featherColor";
		ResultSetConverter<String, WingInner> testInstance = new ResultSetConverter<>(WingInner.class, wingInstanciationColumnName, STRING_READER, WingInner::new);
		testInstance.add(leftFeatherColorColumnName, STRING_READER,
				// Simply instanciate the inner class as usual
				// No need to be added to the wing instance because the constructor does it
				(wing, colorName) -> wing.new FeatherInner(colorName)
		);
		
		// a ResultSet that retrieves all the feathers of a unique Chicken
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.asMap(wingInstanciationColumnName, (Object) "left")
						.add(leftFeatherColorColumnName, "red"),
				Maps.asMap(wingInstanciationColumnName, (Object) "left")
						.add(leftFeatherColorColumnName, "black")
		));
		
		// we must simulate bean caching done by ResultSetConverter.convert(..), otherwise we get NullPointerException
		ThreadLocals.doWithThreadLocal(ResultSetConverter.BEAN_CACHE, SimpleBeanCache::new, (ThrowingRunnable<SQLException>) () -> {
			// from first row, a new instance of Chicken is created named "rooster", it has 1 red feather
			resultSet.next();
			WingInner result = testInstance.transform(resultSet);
			assertEquals("left", result.getSide());
			assertEquals(Arrays.asList("red"), result.getFeathers().stream()
					.map(FeatherInner::getColor).collect(Collectors.toList()));
			
			// from second row, the previous instance of Chicken is kept (not new), and a new black feather was added
			resultSet.next();
			WingInner result1 = testInstance.transform(resultSet);
			assertSame(result, result1);
			assertEquals(2, result.getFeathers().size());
			assertEquals(Arrays.asList("red", "black"), result.getFeathers().stream()
					.map(FeatherInner::getColor).collect(Collectors.toList()));
		});
	}
	
	/**
	 * Same test as {@link #testTranform_shareBeanInstances()} but with the convert(..) method
	 */
	@Test
	public void testConvert() throws SQLException {
		String chickenInstanciationColumnName = "chickenName";
		String leftFeatherColorColumnName = "leftFeatherColor";
		String rightFeatherColorColumnName = "rightFeatherColor";
		ResultSetConverter<String, Chicken> testInstance = new ResultSetConverter<>(Chicken.class, chickenInstanciationColumnName, STRING_READER, Chicken::new);
		testInstance.add(leftFeatherColorColumnName, STRING_READER, FeatherColor.class, FeatherColor::new, (chicken, color) -> {
			if (color != null) {	// prevent addition of Feather with a null color
				chicken.getLeftWing().add(new Feather(color));
			}
		});
		// we add almost the same as previous but for the right wing : the color must be shared between wings
		testInstance.add(rightFeatherColorColumnName, STRING_READER, FeatherColor.class, FeatherColor::new, (chicken, color) -> {
			if (color != null) {	// prevent addition of Feather with a null color
				chicken.getRightWing().add(new Feather(color));
			}
		});
		
		// a ResultSet that retrieves all the feathers of a unique Chicken
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.asMap(chickenInstanciationColumnName, (Object) "rooster")
						.add(rightFeatherColorColumnName, null).add(leftFeatherColorColumnName, "red"),
				Maps.asMap(chickenInstanciationColumnName, (Object) "rooster")
						.add(rightFeatherColorColumnName, null).add(leftFeatherColorColumnName, "black"),
				Maps.asMap(chickenInstanciationColumnName, (Object) "rooster")
						.add(rightFeatherColorColumnName, "black").add(leftFeatherColorColumnName, null)
		));
		
		List<Chicken> result = testInstance.convert(resultSet);
		// 3 chickens because of 3 rows in ResultSet
		assertEquals(3, result.size());
		// ... but they should be all the same
		assertSame(result.get(0), result.get(1));
		assertSame(result.get(0), result.get(2));
		
		Chicken rooster = result.get(0);
		assertEquals("rooster", rooster.getName());
		// Two colors on left : red and black
		assertEquals(Arrays.asList("red", "black"), rooster.getLeftWing().getFeathers().stream()
				.map(Functions.link(Feather::getColor, FeatherColor::getName)).collect(Collectors.toList()));
		// One color on right : black
		assertEquals(Arrays.asList("black"), rooster.getRightWing().getFeathers().stream()
				.map(Functions.link(Feather::getColor, FeatherColor::getName)).collect(Collectors.toList()));
		
		// checking that wings share the same color instance
		Function<Feather, String> colorNameAccessor = Functions.link(Feather::getColor, FeatherColor::getName);
		Map<String, FeatherColor> leftWingFeatherColors = Iterables.map(rooster.getLeftWing().getFeathers(), colorNameAccessor, Feather::getColor);
		Map<String, FeatherColor> rightWingFeatherColors = Iterables.map(rooster.getRightWing().getFeathers(), colorNameAccessor, Feather::getColor);
		assertSame(leftWingFeatherColors.get("black"), rightWingFeatherColors.get("black"));
	}
	
	@DataProvider
	public static Object[][] testConvert_withReuse() {
		return $$(
				$(new ResultSetRowConverter<>(FeatherColor.class, "featherColor", STRING_READER, FeatherColor::new)),
				$(new ResultSetConverter<>(FeatherColor.class, "featherColor", STRING_READER, FeatherColor::new))
		);
	}
	
	@UseDataProvider
	@Test
	public void testConvert_withReuse(AbstractResultSetConverter featherColorTestInstance) throws SQLException {
		String chickenInstanciationColumnName = "chickenName";
		String leftFeatherColorColumnName = "leftFeatherColor";
		String rightFeatherColorColumnName = "rightFeatherColor";
		ResultSetConverter<String, Chicken> testInstance = new ResultSetConverter<>(Chicken.class, chickenInstanciationColumnName, STRING_READER, Chicken::new);
		
		BiConsumer<Chicken, FeatherColor> leftChickenFeatherColorCombiner = (chicken, color) -> {
			if (color != null) {    // prevent addition of Feather with a null color
				chicken.getLeftWing().add(new Feather(color));
			}
		};
		BiConsumer<Chicken, FeatherColor> rightChickenFeatherColorCombiner = (chicken, color) -> {
			if (color != null) {    // prevent addition of Feather with a null color
				chicken.getRightWing().add(new Feather(color));
			}
		};
		if (featherColorTestInstance instanceof ResultSetRowConverter) {
			// we reuse the FeatherColor transformer for the left wing
			testInstance.add((ResultSetRowConverter) featherColorTestInstance.copyWithMapping(Maps.asMap("featherColor", leftFeatherColorColumnName)), leftChickenFeatherColorCombiner);
			// we reuse the FeatherColor transformer for the right wing
			testInstance.add((ResultSetRowConverter) featherColorTestInstance.copyWithMapping(Maps.asMap("featherColor", rightFeatherColorColumnName)), rightChickenFeatherColorCombiner);
		} else if (featherColorTestInstance instanceof ResultSetConverter) {
			// we reuse the FeatherColor transformer for the left wing
			testInstance.add((ResultSetConverter) featherColorTestInstance.copyWithMapping(Maps.asMap("featherColor", leftFeatherColorColumnName)), leftChickenFeatherColorCombiner);
			// we reuse the FeatherColor transformer for the right wing
			testInstance.add((ResultSetConverter) featherColorTestInstance.copyWithMapping(Maps.asMap("featherColor", rightFeatherColorColumnName)), rightChickenFeatherColorCombiner);
		}
		
		// a ResultSet that retrieves all the feathers of a unique Chicken
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.asMap(chickenInstanciationColumnName, (Object) "rooster")
						.add(rightFeatherColorColumnName, null).add(leftFeatherColorColumnName, "red"),
				Maps.asMap(chickenInstanciationColumnName, (Object) "rooster")
						.add(rightFeatherColorColumnName, null).add(leftFeatherColorColumnName, "black"),
				Maps.asMap(chickenInstanciationColumnName, (Object) "rooster")
						.add(rightFeatherColorColumnName, "black").add(leftFeatherColorColumnName, null)
		));
		
		List<Chicken> result = testInstance.convert(resultSet);
		// 3 chickens because of 3 rows in ResultSet
		assertEquals(3, result.size());
		// ... but they should be all the same
		assertSame(result.get(0), result.get(1));
		assertSame(result.get(0), result.get(2));
		
		Chicken rooster = result.get(0);
		assertEquals("rooster", rooster.getName());
		// Two colors on left : red and black
		assertEquals(Arrays.asList("red", "black"), rooster.getLeftWing().getFeathers().stream()
				.map(Functions.link(Feather::getColor, FeatherColor::getName)).collect(Collectors.toList()));
		// One color on right : black
		assertEquals(Arrays.asList("black"), rooster.getRightWing().getFeathers().stream()
				.map(Functions.link(Feather::getColor, FeatherColor::getName)).collect(Collectors.toList()));
		
		// checking that wings share the same color instance
		Function<Feather, String> colorNameAccessor = Functions.link(Feather::getColor, FeatherColor::getName);
		Map<String, FeatherColor> leftWingFeatherColors = Iterables.map(rooster.getLeftWing().getFeathers(), colorNameAccessor, Feather::getColor);
		Map<String, FeatherColor> rightWingFeatherColors = Iterables.map(rooster.getRightWing().getFeathers(), colorNameAccessor, Feather::getColor);
		assertSame(leftWingFeatherColors.get("black"), rightWingFeatherColors.get("black"));
	}
	
	@Test
	public void testCopyFor() {
		String chickenInstanciationColumnName = "chickenName";
		String leftFeatherColorColumnName = "leftFeatherColor";
		String rightFeatherColorColumnName = "rightFeatherColor";
		ResultSetConverter<String, Chicken> testInstance = new ResultSetConverter<>(Chicken.class, chickenInstanciationColumnName, STRING_READER, Chicken::new);
		testInstance.add(leftFeatherColorColumnName, STRING_READER, FeatherColor.class, FeatherColor::new, (chicken, color) -> {
			if (color != null) {	// prevent addition of Feather with a null color
				chicken.getLeftWing().add(new Feather(color));
			}
		});
		// we add almost the same as previous but for the right wing : the color must be shared between wings
		testInstance.add(rightFeatherColorColumnName, STRING_READER, FeatherColor.class, FeatherColor::new, (chicken, color) -> {
			if (color != null) {	// prevent addition of Feather with a null color
				chicken.getRightWing().add(new Feather(color));
			}
		});
		
		ResultSetConverter<String, Rooster> testInstanceCopy = testInstance.copyFor(Rooster.class, Rooster::new);
		testInstanceCopy.add("chicks", INTEGER_PRIMITIVE_READER, Rooster::setChickCount);
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.asMap(chickenInstanciationColumnName, (Object) "rooster")
						.add(rightFeatherColorColumnName, null).add(leftFeatherColorColumnName, "red").add("chicks", 3),
				Maps.asMap(chickenInstanciationColumnName, (Object) "rooster")
						.add(rightFeatherColorColumnName, null).add(leftFeatherColorColumnName, "black").add("chicks", 3),
				Maps.asMap(chickenInstanciationColumnName, (Object) "rooster")
						.add(rightFeatherColorColumnName, "black").add(leftFeatherColorColumnName, null).add("chicks", 3)
		));
		
		List<Rooster> result = testInstanceCopy.convert(resultSet);
		// 3 chickens because of 3 rows in ResultSet
		assertEquals(3, result.size());
		// ... but they should be all the same
		assertSame(result.get(0), result.get(1));
		assertSame(result.get(0), result.get(2));
		
		Rooster rooster = result.get(0);
		assertEquals("rooster", rooster.getName());
		assertEquals(3, rooster.getChickCount());
		// Two colors on left : red and black
		assertEquals(Arrays.asList("red", "black"), rooster.getLeftWing().getFeathers().stream()
				.map(Functions.link(Feather::getColor, FeatherColor::getName)).collect(Collectors.toList()));
		// One color on right : black
		assertEquals(Arrays.asList("black"), rooster.getRightWing().getFeathers().stream()
				.map(Functions.link(Feather::getColor, FeatherColor::getName)).collect(Collectors.toList()));
		
		// checking that wings share the same color instance
		Function<Feather, String> colorNameAccessor = Functions.link(Feather::getColor, FeatherColor::getName);
		Map<String, FeatherColor> leftWingFeatherColors = Iterables.map(rooster.getLeftWing().getFeathers(), colorNameAccessor, Feather::getColor);
		Map<String, FeatherColor> rightWingFeatherColors = Iterables.map(rooster.getRightWing().getFeathers(), colorNameAccessor, Feather::getColor);
		assertSame(leftWingFeatherColors.get("black"), rightWingFeatherColors.get("black"));
	}
	
	@Test
	public void testAddCollection() throws SQLException {
		ResultSetConverter<String, Person> testInstance = new ResultSetConverter<>(Person.class, "name", STRING_READER, Person::new);
		
		testInstance.add("address", STRING_READER, Person::getAddresses, Person::setAddresses, ArrayList::new);
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.asMap("name", (Object) "paul").add("address", "rue Vaudirard"),
				Maps.asMap("name", (Object) "paul").add("address", "rue Menon")
		));
		
		List<Person> result = testInstance.convert(resultSet);
		assertEquals("paul", result.get(0).getName());
		assertEquals(Arrays.asList("rue Vaudirard", "rue Menon"), result.get(0).getAddresses());
	}
	
	public static class Chicken {
		
		private String name;
		
		private Wing leftWing = new Wing();
		
		private Wing rightWing = new Wing();
		
		public Chicken(String name) {
			this.name = name;
		}
		
		public String getName() {
			return name;
		}
		
		public Wing getLeftWing() {
			return leftWing;
		}
		
		public Wing getRightWing() {
			return rightWing;
		}
	}
	
	private static class Wing {
		
		private List<Feather> feathers = new ArrayList<>();
		
		public void add(Feather feather) {
			this.feathers.add(feather);
		}
		
		public List<Feather> getFeathers() {
			return feathers;
		}
	}
	
	private static class Feather {
		
		private FeatherColor color;
		
		public Feather(FeatherColor color) {
			this.color = color;
		}
		
		public FeatherColor getColor() {
			return color;
		}
		
		public void setColor(FeatherColor color) {
			this.color = color;
		}
		
		@Override
		public String toString() {
			return "Feather{" +
					"color=" + color +
					'}';
		}
	}
	
	private static class FeatherColor {
		
		private String name;
		
		public FeatherColor(String name) {
			this.name = name;
		}
		
		public String getName() {
			return name;
		}
		
		public void setName(String name) {
			this.name = name;
		}
		
		@Override
		public String toString() {
			return super.toString() + "{" +
					"name='" + name + '\'' +
					'}';
		}
	}
	
	public static class WingInner {
		
		private String side;
		
		private List<FeatherInner> feathers = new ArrayList<>();
		
		public WingInner(String side) {
			this.side = side;
		}
		
		public String getSide() {
			return side;
		}
		
		public void setSide(String side) {
			this.side = side;
		}
		
		private void add(FeatherInner feather) {
			this.feathers.add(feather);
		}
		
		public List<FeatherInner> getFeathers() {
			return feathers;
		}
		
		public class FeatherInner {
			
			private String color;
			
			public FeatherInner(String color) {
				this.color = color;
				add(this);
			}
			
			public String getColor() {
				return color;
			}
			
			public void setColor(String color) {
				this.color = color;
			}
			
			public WingInner getWing() {
				return WingInner.this;
			}
			
			@Override
			public String toString() {
				return "Feather{" +
						"color=" + color +
						'}';
			}
		}
	}
	
	public static class Rooster extends Chicken {
		
		private int chickCount;
		
		public Rooster(String name) {
			super(name);
		}
		
		public int getChickCount() {
			return chickCount;
		}
		
		public void setChickCount(int chickCount) {
			this.chickCount = chickCount;
		}
	}
	
}