package org.gama.stalactite.persistence.sql.ddl;

import org.gama.lang.collection.Collections;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.ForeignKey;
import org.gama.stalactite.persistence.structure.Index;

import java.util.*;

/**
 * @author Guillaume Mary
 */
public class DDLSchemaGenerator implements DDLGenerator {
	
	private Collection<Table> tables = new LinkedHashSet<>();
	
	private Collection<DDLGenerator> ddlGenerators = new LinkedHashSet<>();
	
	private DDLTableGenerator ddlTableGenerator;
	
	public DDLSchemaGenerator(JavaTypeToSqlTypeMapping typeMapping) {
		this.tables = new ArrayList<>();
		this.ddlTableGenerator = new DDLTableGenerator(typeMapping);
	}
	
	public DDLTableGenerator getDdlTableGenerator() {
		return ddlTableGenerator;
	}
	
	public void setDdlTableGenerator(DDLTableGenerator ddlTableGenerator) {
		this.ddlTableGenerator = ddlTableGenerator;
	}
	
	public void setTables(Collection<Table> tables) {
		this.tables = tables;
	}
	
	public void addTables(Table table, Table ... tables) {
		this.tables.add(table);
		this.tables.addAll(Arrays.asList(tables));
	}
	
	public void setDDLGenerators(Collection<DDLGenerator> ddlParticipants) {
		this.ddlGenerators = ddlParticipants;
	}
	
	public void addDDLGenerators(DDLGenerator ddlGenerator, DDLGenerator... ddlGenerators) {
		this.ddlGenerators.add(ddlGenerator);
		this.ddlGenerators.addAll(Arrays.asList(ddlGenerators));
	}
	
	@Override
	public List<String> getCreationScripts() {
		// DDLParticipants is supposed to be post treatments (creation of sequences, triggers, ...)
		return Collections.cat(generateTableCreationScripts(), generateDDLParticipantsCreationScripts());
	}
	
	protected List<String> generateDDLParticipantsCreationScripts() {
		List<String> participantsScripts = new ArrayList<>();
		for (DDLGenerator ddlGenerator : ddlGenerators) {
			participantsScripts.addAll(ddlGenerator.getCreationScripts());
		}
		return participantsScripts;
	}
	
	protected List<String> generateTableCreationScripts() {
		List<String> tableCreationScripts = new ArrayList<>();
		List<String> foreignKeysCreationScripts = new ArrayList<>();
		List<String> indexesCreationScripts = new ArrayList<>();
		
		for (Table table : tables) {
			tableCreationScripts.add(generateCreationScript(table));
			foreignKeysCreationScripts.addAll(getForeignKeyCreationScripts(table));
			indexesCreationScripts.addAll(generateIndexCreationScripts(table));
		}
		
		// foreign keys must be after table scripts, index is fine tuning
		return Collections.cat(tableCreationScripts, foreignKeysCreationScripts, indexesCreationScripts);
	}
	
	protected String generateCreationScript(Table table) {
		return this.ddlTableGenerator.generateCreateTable(table);
	}
	
	protected List<String> generateIndexCreationScripts(Table table) {
		List<String> indexesCreationScripts = new ArrayList<>();
		for (Index index : table.getIndexes()) {
			indexesCreationScripts.add(generateCreationScript(index));
		}
		return indexesCreationScripts;
	}
	
	protected String generateCreationScript(Index index) {
		return this.ddlTableGenerator.generateCreateIndex(index);
	}
	
	protected List<String> getForeignKeyCreationScripts(Table table) {
		List<String> foreignKeysCreationScripts = new ArrayList<>();
		for (ForeignKey foreignKey : table.getForeignKeys()) {
			foreignKeysCreationScripts.add(generateCreationScript(foreignKey));
		}
		return foreignKeysCreationScripts;
	}
	
	protected String generateCreationScript(ForeignKey foreignKey) {
		return this.ddlTableGenerator.generateCreateForeignKey(foreignKey);
	}
	
	@Override
	public List<String> getDropScripts() {
		// DDLParticipants is supposed to be post treatments (creation of sequences, triggers, ...)
		return Collections.cat(generateTableDropScripts(), generateDDLParticipantsDropScripts());
	}
	
	protected List<String> generateTableDropScripts() {
		List<String> tableCreationScripts = new ArrayList<>();
		
		for (Table table : tables) {
			tableCreationScripts.add(this.ddlTableGenerator.generateDropTable(table));
		}
		
		// foreign keys must be after table scripts, index is fine tuning
		return tableCreationScripts;
	}
	
	protected List<String> generateDDLParticipantsDropScripts() {
		List<String> participantsScripts = new ArrayList<>();
		for (DDLGenerator ddlGenerator : ddlGenerators) {
			List<String> dropScripts = ddlGenerator.getDropScripts();
			if (!Collections.isEmpty(dropScripts)) {
				participantsScripts.addAll(dropScripts);
			}
		}
		return participantsScripts;
	}
}
