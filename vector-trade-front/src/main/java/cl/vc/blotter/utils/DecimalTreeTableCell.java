package cl.vc.blotter.utils;

import javafx.scene.control.TreeTableCell;

public class DecimalTreeTableCell<T> extends TreeTableCell<T, Number> {
    @Override
    protected void updateItem(Number item, boolean empty) {
        super.updateItem(item, empty);
        if (empty || item == null) {
            setText(null);
        } else {
            setText(formatDecimal(item));
        }
    }

    private String formatDecimal(Number number) {
        return String.format("%,.2f", number.doubleValue());
    }
}