package org.gama.sql.result;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.gama.lang.collection.Arrays;
import org.gama.lang.collection.Iterables;
import org.gama.lang.collection.Maps;
import org.gama.lang.function.Functions;
import org.gama.sql.result.ResultSetTransformerTest.WingInner.FeatherInner;
import org.junit.Test;

import static org.gama.sql.binder.DefaultResultSetReaders.STRING_READER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

/**
 * @author Guillaume Mary
 */
public class ResultSetTransformerTest {
	
	@Test
	public void testTranform() throws SQLException {
		String chickenInstanciationColumnName = "chickenName";
		String leftFeatherColorColumnName = "leftFeatherColor";
		ResultSetTransformer<Chicken> testInstance = new ResultSetTransformer<>(Chicken.class, chickenInstanciationColumnName, STRING_READER, Chicken::new);
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
		
		// from first row, a new instance of Chicken is created named "rooster", it has 1 red feather
		resultSet.next();
		Chicken result = testInstance.transform(resultSet);
		assertEquals("rooster", result.getName());
		assertEquals(Arrays.asList("red"), result.getLeftWing().getFeathers().stream()
				.map(((Function<Feather, FeatherColor>) Feather::getColor).andThen(FeatherColor::getName)).collect(Collectors.toList()));
		
		// from second row, the previous instance of Chicken is kept (not new), and a new black feather was added
		resultSet.next();
		Chicken result1 = testInstance.transform(resultSet);
		assertSame(result, result1);
		assertEquals(2, result.getLeftWing().getFeathers().size());
		assertEquals(Arrays.asList("red", "black"), result.getLeftWing().getFeathers().stream()
				.map(((Function<Feather, FeatherColor>) Feather::getColor).andThen(FeatherColor::getName)).collect(Collectors.toList()));
	}
	
	@Test
	public void testTranform_shareBeanInstances() throws SQLException {
		String chickenInstanciationColumnName = "chickenName";
		String leftFeatherColorColumnName = "leftFeatherColor";
		String rightFeatherColorColumnName = "rightFeatherColor";
		ResultSetTransformer<Chicken> testInstance = new ResultSetTransformer<>(Chicken.class, chickenInstanciationColumnName, STRING_READER, Chicken::new);
		testInstance.add(leftFeatherColorColumnName, STRING_READER, FeatherColor.class, FeatherColor::new,
				(chicken, color) -> chicken.getLeftWing().add(new Feather(color))
		);
		// we add almost the same as previous but for the right wing : the color must be shared between wings
		testInstance.add(rightFeatherColorColumnName, STRING_READER, FeatherColor.class, FeatherColor::new, (chicken, color) -> {
			if (color != null) {	// prevent addition of Feather with a null color
				chicken.getRightWing().add(new Feather(color));
			}
		});
		
		// a ResultSet that retrieves all the feathers of a unique Chicken
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.asMap(chickenInstanciationColumnName, (Object) "rooster")
						.add(leftFeatherColorColumnName, "red"),
				Maps.asMap(chickenInstanciationColumnName, (Object) "rooster")
						.add(leftFeatherColorColumnName, "black"),
				Maps.asMap(chickenInstanciationColumnName, (Object) "rooster")
						.add(rightFeatherColorColumnName, "black")
		));
		
		// from first row, a new instance of Chicken is created named "rooster", it has 1 red feather on left wing
		resultSet.next();
		Chicken result = testInstance.transform(resultSet);
		assertEquals("rooster", result.getName());
		assertEquals(Arrays.asList("red"), result.getLeftWing().getFeathers().stream()
				.map(((Function<Feather, FeatherColor>) Feather::getColor).andThen(FeatherColor::getName)).collect(Collectors.toList()));
		
		// from second row, the previous instance of Chicken is kept (not new), and a new black feather was added on left wing
		resultSet.next();
		Chicken result1 = testInstance.transform(resultSet);
		assertSame(result, result1);
		assertEquals(Arrays.asList("red", "black"), result.getLeftWing().getFeathers().stream()
				.map(((Function<Feather, FeatherColor>) Feather::getColor).andThen(FeatherColor::getName)).collect(Collectors.toList()));
		
		// from third row, checking that right wing is black and is not polluted by any null value or whatever
		resultSet.next();
		Chicken result2 = testInstance.transform(resultSet);
		assertSame(result, result2);
		assertEquals(Arrays.asList("black"), result.getRightWing().getFeathers().stream()
				.map(((Function<Feather, FeatherColor>) Feather::getColor).andThen(FeatherColor::getName)).collect(Collectors.toList()));
		
		// checking that wings share the same color instance
		Function<Feather, String> colorNameAccessor = Functions.link(Feather::getColor, FeatherColor::getName);
		Map<String, FeatherColor> leftWingFeatherColors = Iterables.map(result.getLeftWing().getFeathers(), colorNameAccessor, Feather::getColor);
		Map<String, FeatherColor> rightWingFeatherColors = Iterables.map(result.getRightWing().getFeathers(), colorNameAccessor, Feather::getColor);
		assertSame(leftWingFeatherColors.get("black"), rightWingFeatherColors.get("black"));
	}
	
	/**
	 * Test to demonstrate that inner classes can also be assembled.
	 * WingInner has inner instances of FeatherInner, they are automatically added to they enclosing WingInner instance throught their constructor. 
	 */
	@Test
	public void testTranform_withInnerClass() throws SQLException {
		String wingInstanciationColumnName = "wingName";
		String leftFeatherColorColumnName = "featherColor";
		ResultSetTransformer<WingInner> testInstance = new ResultSetTransformer<>(WingInner.class, wingInstanciationColumnName, STRING_READER, WingInner::new);
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
	
}