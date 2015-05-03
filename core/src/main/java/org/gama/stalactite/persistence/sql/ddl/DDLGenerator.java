package org.gama.stalactite.persistence.sql.ddl;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.gama.lang.collection.Collections;
import org.gama.stalactite.persistence.structure.Table;
import org.gama.stalactite.persistence.structure.Table.ForeignKey;
import org.gama.stalactite.persistence.structure.Table.Index;

/**
 * @author mary
 */
public class DDLGenerator implements DDLParticipant {
	
	private final Iterable<Table> tables;
	
	private final DDLTableGenerator ddlTableGenerator;
	
	private final Set<DDLParticipant> ddlParticipants = new LinkedHashSet<>();
	
	public DDLGenerator(Iterable<Table> tablesToCreate, JavaTypeToSqlTypeMapping typeMapping) {
		this.tables = tablesToCreate;
		this.ddlTableGenerator = newDDLTableGenerator(typeMapping);
	}
	
	protected DDLTableGenerator newDDLTableGenerator(JavaTypeToSqlTypeMapping typeMapping) {
		return new DDLTableGenerator(typeMapping);
	}
	
	public void add(DDLParticipant ddlParticipant) {
		this.ddlParticipants.add(ddlParticipant);
	}
	
	@Override
	public List<String> getCreationScripts() {
		// DDLParticipants is supposed to be post treatments (creation of sequences, triggers, ...)
		return Collections.cat(generateTableCreationScripts(), generateDDLParticipantsCreationScripts());
	}
	
	protected List<String> generateDDLParticipantsCreationScripts() {
		List<String> participantsScripts = new ArrayList<>();
		for (DDLParticipant ddlParticipant : ddlParticipants) {
			participantsScripts.addAll(ddlParticipant.getCreationScripts());
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
		for (DDLParticipant ddlParticipant : ddlParticipants) {
			List<String> dropScripts = ddlParticipant.getDropScripts();
			if (!Collections.isEmpty(dropScripts)) {
				participantsScripts.addAll(dropScripts);
			}
		}
		return participantsScripts;
	}
	
	
}
