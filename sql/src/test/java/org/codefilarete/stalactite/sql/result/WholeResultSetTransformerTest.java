package org.codefilarete.stalactite.sql.result;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.codefilarete.stalactite.sql.result.ResultSetRowTransformerTest.Person;
import org.codefilarete.stalactite.sql.result.WholeResultSetTransformer.AssemblyPolicy;
import org.codefilarete.stalactite.sql.result.WholeResultSetTransformerTest.WingInner.FeatherInner;
import org.codefilarete.tool.ThreadLocals;
import org.codefilarete.tool.collection.Arrays;
import org.codefilarete.tool.collection.Iterables;
import org.codefilarete.tool.collection.Maps;
import org.codefilarete.tool.function.Functions;
import org.codefilarete.tool.function.ThrowingRunnable;
import org.codefilarete.tool.trace.MutableInt;
import org.danekja.java.util.function.serializable.SerializableFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultResultSetReaders.INTEGER_PRIMITIVE_READER;
import static org.codefilarete.stalactite.sql.statement.binder.DefaultResultSetReaders.STRING_READER;

/**
 * @author Guillaume Mary
 */
class WholeResultSetTransformerTest {
	
	@Test
	void transform_invokedOnEachRow_returnsSameBeanInstanceIfKeyColumnContainsSameValue() throws SQLException {
		String chickenInstantiationColumnName = "chickenName";
		String leftFeatherColorColumnName = "leftFeatherColor";
		WholeResultSetTransformer<Chicken, String> testInstance = new WholeResultSetTransformer<>(
				Chicken.class, chickenInstantiationColumnName, STRING_READER, Chicken::new);
		testInstance.add(leftFeatherColorColumnName, STRING_READER,
				(chicken, colorName) -> chicken.getLeftWing().add(new Feather(new FeatherColor(colorName)))
		);
		
		// a ResultSet that retrieves all the feathers of a unique Chicken
		String keyColumnValue = "rooster";
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.forHashMap(String.class, Object.class)
						.add(chickenInstantiationColumnName, keyColumnValue)
						.add("leftFeatherNumber", 1)
						.add(leftFeatherColorColumnName, "red"),
				Maps.forHashMap(String.class, Object.class)
						.add(chickenInstantiationColumnName, keyColumnValue)
						.add("leftFeatherNumber", 1)
						.add(leftFeatherColorColumnName, "black")
		));
		
		// we must simulate bean caching done by WholeResultSetTransformer.convert(..), otherwise we get NullPointerException
		ThreadLocals.doWithThreadLocal(WholeResultSetTransformer.CURRENT_BEAN_CACHE, SimpleBeanCache::new, (ThrowingRunnable<SQLException>) () -> {
			// from first row, a new instance of Chicken is created named "rooster", it has 1 red feather
			resultSet.next();
			Chicken result1 = testInstance.transform(resultSet);
			assertThat(result1.getName()).isEqualTo("rooster");
			assertThat(result1.getLeftWing().getFeathers()).extracting(Functions.link(Feather::getColor, FeatherColor::getName))
					.containsExactly("red");
			
			// from second row, the previous instance of Chicken is kept (not new), and a new black feather was added
			resultSet.next();
			Chicken result2 = testInstance.transform(resultSet);
			assertThat(result2).isSameAs(result1);
			assertThat(result1.getLeftWing().getFeathers()).extracting(Functions.link(Feather::getColor, FeatherColor::getName))
					.containsExactly("red", "black");
		});
	}
	
	@Test
	void transform_invokedOnEachRow_returnsSameBeanInstanceIfKeyColumnContainsSameValue_asWellAsForRelation() throws SQLException {
		String chickenInstantiationColumnName = "chickenName";
		String leftFeatherColorColumnName = "leftFeatherColor";
		String rightFeatherColorColumnName = "rightFeatherColor";
		WholeResultSetTransformer<Chicken, String> testInstance = new WholeResultSetTransformer<>(Chicken.class, chickenInstantiationColumnName, STRING_READER, Chicken::new);
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
		String keyColumnValue = "rooster";
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.forHashMap(String.class, Object.class)
						.add(chickenInstantiationColumnName, keyColumnValue)
						.add(leftFeatherColorColumnName, "red")
						.add(rightFeatherColorColumnName, null),
				Maps.forHashMap(String.class, Object.class)
						.add(chickenInstantiationColumnName, keyColumnValue)
						.add(leftFeatherColorColumnName, "black")
						.add(rightFeatherColorColumnName, null),
				Maps.forHashMap(String.class, Object.class)
						.add(chickenInstantiationColumnName, keyColumnValue)
						.add(leftFeatherColorColumnName, null)
						.add(rightFeatherColorColumnName, "black")
		));
		
		// we must simulate bean caching done by WholeResultSetTransformer.convert(..), otherwise we get NullPointerException
		ThreadLocals.doWithThreadLocal(WholeResultSetTransformer.CURRENT_BEAN_CACHE, SimpleBeanCache::new, (ThrowingRunnable<SQLException>) () -> {
			// from first row, a new instance of Chicken is created named "rooster", it has 1 red feather on left wing
			resultSet.next();
			Chicken result1 = testInstance.transform(resultSet);
			assertThat(result1.getName()).isEqualTo("rooster");
			assertThat(result1.getLeftWing().getFeathers()).extracting(Functions.link(Feather::getColor, FeatherColor::getName))
					.containsExactly("red");
			
			// from second row, the previous instance of Chicken is kept (not new), and a new black feather was added on left wing
			resultSet.next();
			Chicken result2 = testInstance.transform(resultSet);
			assertThat(result2).isSameAs(result1);
			assertThat(result1.getLeftWing().getFeathers()).extracting(Functions.link(Feather::getColor, FeatherColor::getName))
					.containsExactly("red", "black");
			
			// from third row, checking that right wing is black and is not polluted by any null value or whatever
			resultSet.next();
			Chicken result3 = testInstance.transform(resultSet);
			assertThat(result3).isSameAs(result1);
			assertThat(result1.getRightWing().getFeathers()).extracting(Functions.link(Feather::getColor, FeatherColor::getName))
					.containsExactly("black");
			
			// checking that wings share the same color instance
			Function<Feather, String> colorNameAccessor = Functions.link(Feather::getColor, FeatherColor::getName);
			Map<String, FeatherColor> leftWingFeatherColors = Iterables.map(result1.getLeftWing().getFeathers(), colorNameAccessor, Feather::getColor);
			
			Map<String, FeatherColor> rightWingFeatherColors = Iterables.map(result1.getRightWing().getFeathers(), colorNameAccessor, Feather::getColor);
			assertThat(rightWingFeatherColors.get("black")).isSameAs(leftWingFeatherColors.get("black"));
		});
	}
	
	/**
	 * Test to demonstrate that inner classes can also be assembled.
	 * {@link WingInner} has inner instances of {@link FeatherInner}, they are automatically added to their enclosing
	 * {@link WingInner} instance through their constructor. 
	 */
	@Test
	void transform_withInnerClass() throws SQLException {
		String wingInstantiationColumnName = "wingName";
		String leftFeatherColorColumnName = "featherColor";
		WholeResultSetTransformer<WingInner, String> testInstance = new WholeResultSetTransformer<>(WingInner.class, wingInstantiationColumnName, STRING_READER, WingInner::new);
		testInstance.add(leftFeatherColorColumnName, STRING_READER,
				// Simply instantiate the inner class as usual
				// No need to be added to the wing instance because the constructor does it
				(wing, colorName) -> wing.new FeatherInner(colorName)
		);
		
		// a ResultSet that retrieves all the feathers of a unique Chicken
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.forHashMap(String.class, Object.class)
						.add(wingInstantiationColumnName, "left")
						.add(leftFeatherColorColumnName, "red"),
				Maps.forHashMap(String.class, Object.class)
						.add(wingInstantiationColumnName, "left")
						.add(leftFeatherColorColumnName, "black")
		));
		
		// we must simulate bean caching done by WholeResultSetTransformer.convert(..), otherwise we get NullPointerException
		ThreadLocals.doWithThreadLocal(WholeResultSetTransformer.CURRENT_BEAN_CACHE, SimpleBeanCache::new, (ThrowingRunnable<SQLException>) () -> {
			// from first row, a new instance of Chicken is created named "rooster", it has 1 red feather
			resultSet.next();
			WingInner result1 = testInstance.transform(resultSet);
			assertThat(result1.getSide()).isEqualTo("left");
			assertThat(result1.getFeathers()).extracting(FeatherInner::getColor).containsExactly("red");
			
			// from second row, the previous instance of Chicken is kept (not new), and a new black feather was added
			resultSet.next();
			WingInner result2 = testInstance.transform(resultSet);
			assertThat(result2).isSameAs(result1);
			assertThat(result1.getFeathers()).extracting(FeatherInner::getColor).containsExactly("red", "black");
		});
	}
	
	/**
	 * Same test as {@link #transform_invokedOnEachRow_returnsSameBeanInstanceIfKeyColumnContainsSameValue_asWellAsForRelation()}
	 * but with the transformAll(..) method
	 */
	@Test
	void transformAll_returnsSameBeanInstanceIfKeyColumnContainsSameValue_asWellAsForRelation() {
		String chickenInstantiationColumnName = "chickenName";
		String leftFeatherColorColumnName = "leftFeatherColor";
		String rightFeatherColorColumnName = "rightFeatherColor";
		WholeResultSetTransformer<Chicken, String> testInstance = new WholeResultSetTransformer<>(Chicken.class, chickenInstantiationColumnName, STRING_READER, Chicken::new);
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
				Maps.forHashMap(String.class, Object.class)
						.add(chickenInstantiationColumnName, "rooster")
						.add(leftFeatherColorColumnName, "red")
						.add(rightFeatherColorColumnName, null),
				Maps.forHashMap(String.class, Object.class)
						.add(chickenInstantiationColumnName, "rooster")
						.add(leftFeatherColorColumnName, "black")
						.add(rightFeatherColorColumnName, null),
				Maps.forHashMap(String.class, Object.class)
						.add(chickenInstantiationColumnName, "rooster")
						.add(leftFeatherColorColumnName, null)
						.add(rightFeatherColorColumnName, "black")
		));
		
		Set<Chicken> result = testInstance.transformAll(resultSet);
		assertThat(result.size()).isEqualTo(1);
		
		Chicken rooster = Iterables.first(result);
		assertThat(rooster.getName()).isEqualTo("rooster");
		// Two colors on left : red and black
		assertThat(rooster.getLeftWing().getFeathers()).extracting(Functions.link(Feather::getColor, FeatherColor::getName)).containsExactly("red", "black");
		// One color on right : black
		assertThat(rooster.getRightWing().getFeathers()).extracting(Functions.link(Feather::getColor, FeatherColor::getName)).containsExactly("black");
		
		// checking that wings share the same color instance
		Function<Feather, String> colorNameAccessor = Functions.link(Feather::getColor, FeatherColor::getName);
		Map<String, FeatherColor> leftWingFeatherColors = Iterables.map(rooster.getLeftWing().getFeathers(), colorNameAccessor, Feather::getColor);
		Map<String, FeatherColor> rightWingFeatherColors = Iterables.map(rooster.getRightWing().getFeathers(), colorNameAccessor, Feather::getColor);
		assertThat(rightWingFeatherColors.get("black")).isSameAs(leftWingFeatherColors.get("black"));
	}
	
	@Test
	void transformAll_withAccumulator_testToRetrieveASingleColumn() {
		String chickenInstantiationColumnName = "chickenName";
		WholeResultSetTransformer<String, String> testInstance = new WholeResultSetTransformer<>(String.class, chickenInstantiationColumnName, STRING_READER, SerializableFunction.identity());
		
		// a ResultSet that retrieves all the feathers of a unique Chicken
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.forHashMap(String.class, Object.class)
						.add(chickenInstantiationColumnName, "rooster1"),
				Maps.forHashMap(String.class, Object.class)
						.add(chickenInstantiationColumnName, "rooster2"),
				Maps.forHashMap(String.class, Object.class)
						.add(chickenInstantiationColumnName, "rooster1")
		));
		
		List<String> result = testInstance.transformAll(resultSet, Accumulators.toList());
		assertThat(result).containsExactlyInAnyOrder("rooster1", "rooster2", "rooster1");
	}
	
	@Test
	void transformAll_withCollector_testToRetrieveASingleColumn() {
		String chickenInstantiationColumnName = "chickenName";
		WholeResultSetTransformer<String, String> testInstance = new WholeResultSetTransformer<>(String.class, chickenInstantiationColumnName, STRING_READER, SerializableFunction.identity());
		
		// a ResultSet that retrieves all the feathers of a unique Chicken
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.forHashMap(String.class, Object.class)
						.add(chickenInstantiationColumnName, "rooster1"),
				Maps.forHashMap(String.class, Object.class)
						.add(chickenInstantiationColumnName, "rooster2"),
				Maps.forHashMap(String.class, Object.class)
						.add(chickenInstantiationColumnName, "rooster1")
		));
		
		List<String> result = testInstance.transformAll(resultSet, Accumulators.toList());
		assertThat(result).containsExactlyInAnyOrder("rooster1", "rooster2", "rooster1");
	}
	
	/**
	 * Same test as {@link #transform_invokedOnEachRow_returnsSameBeanInstanceIfKeyColumnContainsSameValue_asWellAsForRelation()}
	 * but with the transformAll(..) method
	 */
	@Test
	void transformAll_withCollector_returnsSameBeanInstanceIfKeyColumnContainsSameValue_asWellAsForRelation() {
		String chickenInstantiationColumnName = "chickenName";
		String leftFeatherColorColumnName = "leftFeatherColor";
		String rightFeatherColorColumnName = "rightFeatherColor";
		WholeResultSetTransformer<Chicken, String> testInstance = new WholeResultSetTransformer<>(Chicken.class, chickenInstantiationColumnName, STRING_READER, Chicken::new);
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
				Maps.forHashMap(String.class, Object.class)
						.add(chickenInstantiationColumnName, "rooster1")
						.add(leftFeatherColorColumnName, "red")
						.add(rightFeatherColorColumnName, null),
				Maps.forHashMap(String.class, Object.class)
						.add(chickenInstantiationColumnName, "rooster1")
						.add(leftFeatherColorColumnName, "black")
						.add(rightFeatherColorColumnName, null),
				Maps.forHashMap(String.class, Object.class)
						.add(chickenInstantiationColumnName, "rooster2")
						.add(leftFeatherColorColumnName, "brown")
						.add(rightFeatherColorColumnName, null),
				Maps.forHashMap(String.class, Object.class)
						.add(chickenInstantiationColumnName, "rooster1")
						.add(leftFeatherColorColumnName, null)
						.add(rightFeatherColorColumnName, "black")
		));
		
		List<Chicken> result = testInstance.transformAll(resultSet, Accumulators.toList());
		assertThat(result.size()).isEqualTo(4);
		
		Chicken rooster1 = result.get(0);
		assertThat(rooster1.getName()).isEqualTo("rooster1");
		// Two colors on left : red and black
		assertThat(rooster1.getLeftWing().getFeathers()).extracting(Functions.link(Feather::getColor, FeatherColor::getName)).containsExactly("red", "black");
		// One color on right : black
		assertThat(rooster1.getRightWing().getFeathers()).extracting(Functions.link(Feather::getColor, FeatherColor::getName)).containsExactly("black");
		
		// checking that wings share the same color instance
		Function<Feather, String> colorNameAccessor = Functions.link(Feather::getColor, FeatherColor::getName);
		Map<String, FeatherColor> leftWingFeatherColors = Iterables.map(rooster1.getLeftWing().getFeathers(), colorNameAccessor, Feather::getColor);
		Map<String, FeatherColor> rightWingFeatherColors = Iterables.map(rooster1.getRightWing().getFeathers(), colorNameAccessor, Feather::getColor);
		assertThat(rightWingFeatherColors.get("black")).isSameAs(leftWingFeatherColors.get("black"));
		
		Chicken rooster2 = result.get(2);
		assertThat(rooster2.getName()).isEqualTo("rooster2");
		// One color on left : brown
		assertThat(rooster2.getLeftWing().getFeathers()).extracting(Functions.link(Feather::getColor, FeatherColor::getName)).containsExactly("brown");
	}
	
	static Object[][] testTransform_withReuse() {
		return new Object[][] {
				new Object[] { new ResultSetRowTransformer<>(FeatherColor.class, "featherColor", STRING_READER, FeatherColor::new) },
		};
	}
	
	@ParameterizedTest
	@MethodSource("testTransform_withReuse")
	void transform_copyWithAliases(ResultSetRowTransformer<FeatherColor, String> featherColorTestInstance) {
		String chickenInstantiationColumnName = "chickenName";
		String leftFeatherColorColumnName = "leftFeatherColor";
		String rightFeatherColorColumnName = "rightFeatherColor";
		WholeResultSetTransformer<Chicken, String> testInstance = new WholeResultSetTransformer<>(Chicken.class, chickenInstantiationColumnName, STRING_READER, Chicken::new);
		
		BeanRelationFixer<Chicken, FeatherColor> leftChickenFeatherColorCombiner = (chicken, color) -> {
			chicken.getLeftWing().add(new Feather(color));
		};
		BeanRelationFixer<Chicken, FeatherColor> rightChickenFeatherColorCombiner = (chicken, color) -> {
			chicken.getRightWing().add(new Feather(color));
		};
		
		Map<String, String> leftColumnMapping = Maps.forHashMap(String.class, String.class)
				.add("featherColor", leftFeatherColorColumnName);
		Map<String, String> rightColumnMapping = Maps.forHashMap(String.class, String.class)
				.add("featherColor", rightFeatherColorColumnName);
		// we reuse the FeatherColor transformer for the left wing
		testInstance.add(leftChickenFeatherColorCombiner, (ResultSetRowTransformer) featherColorTestInstance.copyWithAliases(leftColumnMapping));
		// we reuse the FeatherColor transformer for the right wing
		testInstance.add(rightChickenFeatherColorCombiner, (ResultSetRowTransformer) featherColorTestInstance.copyWithAliases(rightColumnMapping));
		
		String sneakyFeatherColorColumnName = "sneakyFeatherColor";
		ResultSetRowAssembler<Chicken> rawAssembler = new ResultSetRowAssembler<Chicken>() {
			@Override
			public void assemble(Chicken rootBean, ResultSet resultSet) throws SQLException {
				rootBean.addLeftFeather(new FeatherColor(resultSet.getString(sneakyFeatherColorColumnName)));
			}
			
			@Override
			public ResultSetRowAssembler<Chicken> copyWithAliases(Function<String, String> columnMapping) {
				return (rootBean, resultSet) -> rootBean.addLeftFeather(new FeatherColor(resultSet.getString(columnMapping.apply(sneakyFeatherColorColumnName))));
			}
		};
		testInstance.add(rawAssembler);
		
		Function<String, String> translatingColumnFunction = (String s) -> "_" + s + "_";
		testInstance = testInstance.copyWithAliases(translatingColumnFunction);
		
		// a ResultSet that retrieves all the feathers of a unique Chicken
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.forHashMap(String.class, Object.class)
						.add(translatingColumnFunction.apply(chickenInstantiationColumnName), "rooster")
						.add(translatingColumnFunction.apply(leftFeatherColorColumnName), "red")
						.add(translatingColumnFunction.apply(rightFeatherColorColumnName), null)
						.add(translatingColumnFunction.apply(sneakyFeatherColorColumnName), "pink"),
				Maps.forHashMap(String.class, Object.class)
						.add(translatingColumnFunction.apply(chickenInstantiationColumnName), "rooster")
						.add(translatingColumnFunction.apply(leftFeatherColorColumnName), "black")
						.add(translatingColumnFunction.apply(rightFeatherColorColumnName), null)
						.add(translatingColumnFunction.apply(sneakyFeatherColorColumnName), "pink"),
				Maps.forHashMap(String.class, Object.class)
						.add(translatingColumnFunction.apply(chickenInstantiationColumnName), "rooster")
						.add(translatingColumnFunction.apply(leftFeatherColorColumnName), null)
						.add(translatingColumnFunction.apply(rightFeatherColorColumnName), "black")
						.add(translatingColumnFunction.apply(sneakyFeatherColorColumnName), "pink")
		));
		
		Set<Chicken> result = testInstance.transformAll(resultSet);
		assertThat(result.size()).isEqualTo(1);
		
		Chicken rooster = Iterables.first(result);
		assertThat(rooster.getName()).isEqualTo("rooster");
		// Colors on left : red, black and pink put by dummy row transformer
		assertThat(rooster.getLeftWing().getFeathers()).extracting(Functions.link(Feather::getColor, FeatherColor::getName))
				.containsExactly("red", "pink", "black", "pink", "pink");
		// One color on right : black
		assertThat(rooster.getRightWing().getFeathers()).extracting(Functions.link(Feather::getColor, FeatherColor::getName))
				.containsExactly("black");
		
		// checking that wings share the same color instance
		Function<Feather, String> colorNameAccessor = Functions.link(Feather::getColor, FeatherColor::getName);
		Map<String, FeatherColor> leftWingFeatherColors = Iterables.map(rooster.getLeftWing().getFeathers(), colorNameAccessor, Feather::getColor);
		Map<String, FeatherColor> rightWingFeatherColors = Iterables.map(rooster.getRightWing().getFeathers(), colorNameAccessor, Feather::getColor);
		assertThat(rightWingFeatherColors.get("black")).isSameAs(leftWingFeatherColors.get("black"));
	}
	
	@Test
	void copyFor() {
		String chickenInstantiationColumnName = "chickenName";
		String leftFeatherColorColumnName = "leftFeatherColor";
		String rightFeatherColorColumnName = "rightFeatherColor";
		WholeResultSetTransformer<Chicken, String> testInstance = new WholeResultSetTransformer<>(Chicken.class, chickenInstantiationColumnName, STRING_READER, Chicken::new);
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
		
		// we add a dummy assembler to ensure that they are also taken into account by copyFor(..)
		testInstance.add((rootBean, resultSet) -> rootBean.addLeftFeather(new FeatherColor("pink")), AssemblyPolicy.ON_EACH_ROW);
		testInstance.add((rootBean, resultSet) -> rootBean.addLeftFeather(new FeatherColor("yellow")), AssemblyPolicy.ONCE_PER_BEAN);
		
		MutableInt headCreationCounter = new MutableInt();
		testInstance.add(Chicken::setHead, new ResultSetRowTransformer<>(Head.class, chickenInstantiationColumnName, STRING_READER, s -> {
			headCreationCounter.increment();	// this will be done once because of cache put by WholeResultSetTransformer
			return new Head();
		}));
		
		WholeResultSetTransformer<Rooster, String> testInstanceCopy = testInstance.copyFor(Rooster.class, Rooster::new);
		testInstanceCopy.add("chicks", INTEGER_PRIMITIVE_READER, Rooster::setChickCount);
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.forHashMap(String.class, Object.class)
						.add(chickenInstantiationColumnName, "rooster")
						.add(leftFeatherColorColumnName, "red")
						.add(rightFeatherColorColumnName, null)
						.add("chicks", 3),
				Maps.forHashMap(String.class, Object.class)
						.add(chickenInstantiationColumnName, "rooster")
						.add(leftFeatherColorColumnName, "black")
						.add(rightFeatherColorColumnName, null)
						.add("chicks", 3),
				Maps.forHashMap(String.class, Object.class)
						.add(chickenInstantiationColumnName, "rooster")
						.add(leftFeatherColorColumnName, null)
						.add(rightFeatherColorColumnName, "black")
						.add("chicks", 3)
		));
		
		Set<Rooster> result = testInstanceCopy.transformAll(resultSet);
		assertThat(result.size()).isEqualTo(1);
		assertThat(headCreationCounter.getValue()).isEqualTo(1);
		
		Rooster rooster = Iterables.first(result);
		assertThat(rooster.getName()).isEqualTo("rooster");
		assertThat(rooster.getChickCount()).isEqualTo(3);
		// Colors on left : red, black, and as many as were added by 
		assertThat(rooster.getLeftWing().getFeathers()).extracting(Functions.link(Feather::getColor, FeatherColor::getName))
				.containsExactly("red", "pink", "yellow", "black", "pink", "pink");
		// One color on right : black
		assertThat(rooster.getRightWing().getFeathers()).extracting(Functions.link(Feather::getColor, FeatherColor::getName))
				.containsExactly("black");
		
		// checking that wings share the same color instance
		Function<Feather, String> colorNameAccessor = Functions.link(Feather::getColor, FeatherColor::getName);
		Map<String, FeatherColor> leftWingFeatherColors = Iterables.map(rooster.getLeftWing().getFeathers(), colorNameAccessor, Feather::getColor);
		Map<String, FeatherColor> rightWingFeatherColors = Iterables.map(rooster.getRightWing().getFeathers(), colorNameAccessor, Feather::getColor);
		assertThat(rightWingFeatherColors.get("black")).isSameAs(leftWingFeatherColors.get("black"));
	}
	
	@Test
	void transformAll_withRelationDefiningAssemblyPolicy() {
		String chickenInstantiationColumnName = "chickenName";
		WholeResultSetTransformer<Chicken, String> testInstance = new WholeResultSetTransformer<>(Chicken.class, chickenInstantiationColumnName, STRING_READER, Chicken::new);
		// we add a dummy assembler to ensure that they are also taken into account by copyFor(..)
		testInstance.add((rootBean, resultSet) -> rootBean.addLeftFeather(new FeatherColor("pink")), AssemblyPolicy.ON_EACH_ROW);
		testInstance.add((rootBean, resultSet) -> rootBean.addLeftFeather(new FeatherColor("yellow")), AssemblyPolicy.ONCE_PER_BEAN);
		
		MutableInt headCreationCounter = new MutableInt();
		testInstance.add(Chicken::setHead, new ResultSetRowTransformer<>(Head.class, chickenInstantiationColumnName, STRING_READER, s -> {
			headCreationCounter.increment();
			return new Head();
		}));
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.forHashMap(String.class, Object.class)
						.add(chickenInstantiationColumnName, "rooster"),
				Maps.forHashMap(String.class, Object.class)
						.add(chickenInstantiationColumnName, "rooster"),
				Maps.forHashMap(String.class, Object.class)
						.add(chickenInstantiationColumnName, "rooster")
		));
		
		Set<Chicken> result = testInstance.transformAll(resultSet);
		assertThat(result.size()).isEqualTo(1);
		assertThat(headCreationCounter.getValue()).isEqualTo(1);
		
		Chicken rooster = Iterables.first(result);
		assertThat(rooster.getName()).isEqualTo("rooster");
		// Colors on left : red, black, and as many as were added by assembler, according to their AssemblyPolicy 
		assertThat(rooster.getLeftWing().getFeathers()).extracting(Functions.link(Feather::getColor, FeatherColor::getName))
				.containsExactly("pink", "yellow", "pink", "pink");
	}
	
	@Test
	void transformAll_withRowAssemblerForACollection() {
		WholeResultSetTransformer<Person, String> testInstance = new WholeResultSetTransformer<>(Person.class, "name", STRING_READER, Person::new);
		
		testInstance.add("address", STRING_READER, Person::getAddresses, Person::setAddresses, ArrayList::new);
		
		InMemoryResultSet resultSet = new InMemoryResultSet(Arrays.asList(
				Maps.forHashMap(String.class, Object.class)
						.add("name", "paul").add("address", "rue Vaugirard"),
				Maps.forHashMap(String.class, Object.class)
						.add("name", "paul").add("address", "rue Menon")
		));
		
		Set<Person> result = testInstance.transformAll(resultSet);
		assertThat(Iterables.first(result).getName()).isEqualTo("paul");
		assertThat(Iterables.first(result).getAddresses()).isEqualTo(Arrays.asList("rue Vaugirard", "rue Menon"));
	}
	
	private static class Chicken {
		
		private String name;
		
		private Wing leftWing = new Wing();
		
		private Wing rightWing = new Wing();
		
		private Head head;
		
		private Chicken(String name) {
			this.name = name;
		}
		
		public String getName() {
			return name;
		}
		
		public Wing getLeftWing() {
			return leftWing;
		}
		
		public void addLeftFeather(FeatherColor color) {
			getLeftWing().add(new Feather(color));
		}
		
		public Wing getRightWing() {
			return rightWing;
		}
		
		public Head getHead() {
			return head;
		}
		
		public Chicken setHead(Head head) {
			this.head = head;
			return this;
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
	
	private static class Head {
		
		
	}
	
	static class WingInner {
		
		private String side;
		
		private List<FeatherInner> feathers = new ArrayList<>();
		
		WingInner(String side) {
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
	
	private static class Rooster extends Chicken {
		
		private int chickCount;
		
		private Rooster(String name) {
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