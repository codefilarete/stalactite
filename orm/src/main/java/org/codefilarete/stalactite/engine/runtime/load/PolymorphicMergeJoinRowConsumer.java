package org.codefilarete.stalactite.engine.runtime.load;

import javax.annotation.Nullable;
import java.util.function.BiFunction;

import org.codefilarete.stalactite.engine.runtime.load.MergeJoinNode.MergeJoinRowConsumer;
import org.codefilarete.stalactite.mapping.ColumnedRow;
import org.codefilarete.stalactite.sql.result.Row;

/**
 * 
 * @param <C>
 * @param <D>
 * @param <I>
 * @author Guillaume Mary
 */
public class PolymorphicMergeJoinRowConsumer<C, D extends C, I> extends MergeJoinRowConsumer<D> {
	
	private final BiFunction<Row, ColumnedRow, I> identifierProvider;
	
	private final ColumnedRow columnedRow;
	
	public PolymorphicMergeJoinRowConsumer(PolymorphicEntityInflater<C, D, I, ?> entityInflater,
										   ColumnedRow columnedRow) {
		super(entityInflater.copyTransformerWithAliases(columnedRow));
		this.identifierProvider = entityInflater::giveIdentifier;
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
