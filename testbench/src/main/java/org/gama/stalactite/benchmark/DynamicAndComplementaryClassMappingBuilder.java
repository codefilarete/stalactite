package org.gama.stalactite.benchmark;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.ClassLoadingStrategy;
import net.bytebuddy.dynamic.DynamicType.Builder;
import org.gama.reflection.PropertyAccessor;
import org.gama.stalactite.persistence.engine.SeparateTransactionExecutor;
import org.gama.stalactite.persistence.id.generator.AutoAssignedIdentifierGenerator;
import org.gama.stalactite.persistence.id.generator.sequence.PooledSequenceIdentifierGenerator;
import org.gama.stalactite.persistence.id.generator.sequence.PooledSequenceIdentifierGeneratorOptions;
import org.gama.stalactite.persistence.id.generator.sequence.PooledSequencePersistenceOptions;
import org.gama.stalactite.persistence.mapping.ClassMappingStrategy;
import org.gama.stalactite.persistence.mapping.PersistentFieldHarverster;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.Column;

/**
 * @author Guillaume Mary
 */
public class DynamicAndComplementaryClassMappingBuilder implements IMappingBuilder {
	
	private DynamicTable targetTable;
	private DynamicTable targetNilTable;
	private List<Table> indexTables = new ArrayList<>();
	
	
	Class<? extends DynamicEntity> dynamicType;
	Class<? extends DynamicEntity> nilDynamicType;
	Map<Field, ClassMappingStrategy> indexDynamicTypes = new HashMap<>();
	
	Map<String, Field> dynamicTypeFields;
	Map<String, Field> nilDynamicTypeFields;
	private ClassMappingStrategy<? extends DynamicAndComplementaryClassMappingBuilder.DynamicEntity, Long> classMappingStrategyNil;
	
	@Override
	public ClassMappingStrategy getClassMappingStrategy() {
		targetTable = new DynamicTable("DynamicTable", DynamicEntity.QUESTION_COUNT);
		this.dynamicType = buildDynamicType(targetTable);
		
		PersistentFieldHarverster persistentFieldHarverster = new PersistentFieldHarverster();
		Map<PropertyAccessor, Column> fieldColumnMap = persistentFieldHarverster.mapFields(dynamicType, targetTable);
		dynamicTypeFields = new HashMap<>();
		for (Field declaredField : persistentFieldHarverster.getFields(dynamicType)) {
//			if (declaredField.getName().startsWith("q")) {
				dynamicTypeFields.put(declaredField.getName(), declaredField);
//			}
		}
		
		PooledSequenceIdentifierGeneratorOptions options = new PooledSequenceIdentifierGeneratorOptions(100, targetTable.getName(), PooledSequencePersistenceOptions.DEFAULT);
		PooledSequenceIdentifierGenerator identifierGenerator = new PooledSequenceIdentifierGenerator(options,
				PersistenceContexts.getCurrent().getDialect(), (SeparateTransactionExecutor) PersistenceContexts.getCurrent().getConnectionProvider(), PersistenceContexts.getCurrent().getJDBCBatchSize());
//		PersistenceContext.getCurrent().add(identifierGenerator.getPooledSequencePersister().getMappingStrategy());
		ClassMappingStrategy<? extends DynamicEntity, Long> classMappingStrategy = new ClassMappingStrategy<>(dynamicType, targetTable,
				fieldColumnMap, PropertyAccessor.forProperty(persistentFieldHarverster.getField("id")), identifierGenerator);
		getClassMappingStrategy_nil();
		getClassMappingStrategy_indexes();
		return classMappingStrategy;
	}
	
	public ClassMappingStrategy getClassMappingStrategy_nil() {
		targetNilTable = new DynamicTable(targetTable.getName() + "_nil", DynamicEntity.QUESTION_COUNT) {
			@Override
			protected Column newColum(int i) {
				return new Column("q"+i, Boolean.TYPE);
			}
		};
		this.nilDynamicType = buildDynamicType(targetNilTable);
		
		PersistentFieldHarverster persistentFieldHarverster = new PersistentFieldHarverster();
		Map<PropertyAccessor, Column> fieldColumnMap = persistentFieldHarverster.mapFields(nilDynamicType, targetNilTable);
		nilDynamicTypeFields = new HashMap<>();
		for (Field declaredField : persistentFieldHarverster.getFields(nilDynamicType)) {
			nilDynamicTypeFields.put(declaredField.getName(), declaredField);
		}
		
		// NB: AutoAssignedIdentifierGenerator car l'id vient de l'instance de DynamicType
		classMappingStrategyNil = new ClassMappingStrategy<>(nilDynamicType, targetNilTable,
				fieldColumnMap, PropertyAccessor.forProperty(persistentFieldHarverster.getField("id")), new AutoAssignedIdentifierGenerator());
		return classMappingStrategyNil;
	}
	
	public Map<Field, ClassMappingStrategy> getClassMappingStrategy_indexes() {
		// tables des colonnes à indexer
		for (Field columnToIndex : shuffle(new ArrayList<>(dynamicTypeFields.values()), 5)) {
			DynamicTable indexTable = new DynamicTable(targetTable.getName() + "_" + columnToIndex.getName(), 0);
			indexTable.new Column(targetTable.getPrimaryKey().getName(), targetTable.getPrimaryKey().getJavaType());
			Column indexedColumn = indexTable.new Column(columnToIndex.getName(), columnToIndex.getType());
			indexTable.new Index(indexedColumn, "IDX_" + indexedColumn.getName());
			indexTables.add(indexTable);
			
			Class<? extends DynamicEntity> indexDynamicType = buildDynamicType(indexTable);
			
			PersistentFieldHarverster persistentFieldHarverster = new PersistentFieldHarverster();
			Map<PropertyAccessor, Column> fieldColumnMap = persistentFieldHarverster.mapFields(indexDynamicType, indexTable);
			
			ClassMappingStrategy<? extends DynamicEntity, Long> classMappingStrategy = new ClassMappingStrategy<>(indexDynamicType, indexTable,
					fieldColumnMap, PropertyAccessor.forProperty(persistentFieldHarverster.getField("id")), new AutoAssignedIdentifierGenerator());
			indexDynamicTypes.put(columnToIndex, classMappingStrategy);
		}
		return indexDynamicTypes;
	}
	
	private Class<? extends DynamicEntity> buildDynamicType(DynamicTable targetTable) {
		Builder<DynamicEntity> dynamicClassBuilder = new ByteBuddy().subclass(DynamicEntity.class);
		
		for (Column column : targetTable.getColumns()) {
			if (!column.isPrimaryKey()) {
				dynamicClassBuilder = dynamicClassBuilder.defineField(column.getName(), column.getJavaType(), Modifier.PUBLIC);
			}
		}
		return dynamicClassBuilder
				//.name(getClass().getPackage().getName()+"Tata")
				.make()
				.load(getClass().getClassLoader(), ClassLoadingStrategy.Default.WRAPPER)
				.getLoaded();
	}
	
	public static <E> List<E> shuffle(List<E> list, int count) {
		List<E> result = new ArrayList<>(count);
		Random random = new Random();
		int size = list.size();
		for (int i = 0; i < count; i++) {
			result.add(list.get(random.nextInt(size)));
		}
		return result;
	}
	
	public static class DynamicTable extends Table {
		
		public final Column id;
		public final Map<Long, Column> dynamicColumns = new HashMap<>();
		
		public DynamicTable(String tableName, int columnCount) {
			super(null, tableName);
			id = new Column("id", Long.TYPE);
			id.setPrimaryKey(true);
			for (int i = 0; i < columnCount; i++) {
				Column column = newColum(i);
				dynamicColumns.put((long) i, column);
			}
		}
		
		protected Column newColum(int i) {
			Class columnType = Integer.class;
			return new Column("q" + i, columnType);
		}
	}
	
	@Target({ElementType.METHOD, ElementType.FIELD})
	@Retention(RetentionPolicy.RUNTIME)
	public @interface Id {
	}
	
	public static class DynamicEntity {
		
		public static int QUESTION_COUNT = 100;
		
		@Id
		private Long id;
		
		public DynamicEntity() {
		}
		
		public void setId(long id) {
			this.id = id;
		}
		
		public Long getId() {
			return id;
		}
	}
	
}
