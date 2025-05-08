package org.codefilarete.stalactite.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.function.BiFunction;

import org.codefilarete.stalactite.engine.runtime.load.MergeJoinNode.MergeJoinRowConsumer;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.mapping.EntityMapping;
import org.codefilarete.stalactite.sql.result.Row;

/**
 * 
 * @param <D>
 * @param <I>
 * @author Guillaume Mary
 */
public class PolymorphicMergeJoinRowConsumer<D, I> extends MergeJoinRowConsumer<D> {
	
	private final BiFunction<Row, ColumnedRow, I> identifierProvider;
	
	private final ColumnedRow columnedRow;
	
	public PolymorphicMergeJoinRowConsumer(EntityMapping<D, I, ?> entityInflater,
										   ColumnedRow columnedRow) {
		super(entityInflater.copyTransformerWithAliases(columnedRow));
		this.identifierProvider = entityInflater.getIdMapping().getIdentifierAssembler()::assemble;
		this.columnedRow = columnedRow;
	}
	
	@Nullable
	public I giveIdentifier(Row row) {
		return identifierProvider.apply(row, columnedRow);
	}
	
	public D transform(Row row) {
		return super.merger.transform(row);
	}
}
