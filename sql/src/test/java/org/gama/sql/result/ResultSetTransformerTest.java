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
		ResultSetTransformer<Chicken> testInstance = new ResultSetTransformer<>(Chicken.class, "chickenName", STRING_READER, Chicken::new);
		testInstance.add("leftFeatherColor", STRING_READER, (chicken, colorName) -> chicken.getLeftWing().add(new Feather(new FeatherColor
				(colorName))));
		
		// a ResultSet that retrieves all the feathers of a unique Chicken
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.asMap("chickenName", (Object) "rooster")
						.add("leftFeatherNumber", 1)
						.add("leftFeatherColor", "red"),
				Maps.asMap("chickenName", (Object) "rooster")
						.add("leftFeatherNumber", 1)
						.add("leftFeatherColor", "black")
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
		ResultSetTransformer<Chicken> testInstance = new ResultSetTransformer<>(Chicken.class, "chickenName", STRING_READER, Chicken::new);
		testInstance.add("leftFeatherColor", STRING_READER, FeatherColor.class, FeatherColor::new, (chicken, color) -> chicken.getLeftWing().add(new
				Feather(color)));
		// we add almost the same as previous but for the right wing : the color must be shared between wings
		testInstance.add("rightFeatherColor", STRING_READER, FeatherColor.class, FeatherColor::new, (chicken, color) -> {
			if (color != null) {	// prevent addition of Feather with a null color
				chicken.getRightWing().add(new Feather(color));
			}
		});
		
		// a ResultSet that retrieves all the feathers of a unique Chicken
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.asMap("chickenName", (Object) "rooster")
						.add("leftFeatherColor", "red"),
				Maps.asMap("chickenName", (Object) "rooster")
						.add("leftFeatherColor", "black"),
				Maps.asMap("chickenName", (Object) "rooster")
						.add("rightFeatherColor", "black")
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
		Function<Feather, String> colorNameAccessor = Functions.chain(Feather::getColor, FeatherColor::getName);
		Map<String, FeatherColor> leftWingFeatherColors = Iterables.map(result.getLeftWing().getFeathers(), colorNameAccessor, Feather::getColor);
		Map<String, FeatherColor> rightWingFeatherColors = Iterables.map(result.getRightWing().getFeathers(), colorNameAccessor, Feather::getColor);
		assertSame(leftWingFeatherColors.get("black"), rightWingFeatherColors.get("black"));
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
	
}