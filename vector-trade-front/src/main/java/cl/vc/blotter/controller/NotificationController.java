package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import com.google.protobuf.Timestamp;
import javafx.beans.property.SimpleObjectProperty;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.TableCell;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableRow;
import javafx.scene.control.TableView;
import javafx.util.Callback;
import lombok.Getter;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class NotificationController {

    @FXML
    private TableView<NotificationMessage.Notification> notificationTableView;

    @FXML
    private TableColumn<NotificationMessage.Notification, NotificationMessage.TypeState> type;

    @FXML
    private TableColumn<NotificationMessage.Notification, String> message;

    @FXML
    private TableColumn<NotificationMessage.Notification, String> title;

    @FXML
    private TableColumn<NotificationMessage.Notification, String> comments;

    @FXML
    private TableColumn<NotificationMessage.Notification, String> securityExchange;

    @FXML
    private TableColumn<NotificationMessage.Notification, NotificationMessage.Component> component;

    @FXML
    private TableColumn<NotificationMessage.Notification, NotificationMessage.Level> state;

    @FXML
    private TableColumn<NotificationMessage.Notification, Timestamp> time;

    @Getter
    private ObservableList<NotificationMessage.Notification> data;

    @FXML
    private void initialize() {


        data = FXCollections.observableArrayList();
        notificationTableView.setItems(data);

        this.type.setCellValueFactory(new Callback<TableColumn.CellDataFeatures<NotificationMessage.Notification, NotificationMessage.TypeState>, ObservableValue<NotificationMessage.TypeState>>() {
            @Override
            public ObservableValue<NotificationMessage.TypeState> call(TableColumn.CellDataFeatures<NotificationMessage.Notification, NotificationMessage.TypeState> cellData) {
                NotificationMessage.Notification action = cellData.getValue();
                return new SimpleObjectProperty<>(action.getTypeState());
            }
        });

        this.message.setCellValueFactory(new Callback<TableColumn.CellDataFeatures<NotificationMessage.Notification, String>, ObservableValue<String>>() {
            @Override
            public ObservableValue<String> call(TableColumn.CellDataFeatures<NotificationMessage.Notification, String> cellData) {
                NotificationMessage.Notification action = cellData.getValue();
                return new SimpleObjectProperty<>(action.getMessage());
            }
        });

        this.title.setCellValueFactory(new Callback<TableColumn.CellDataFeatures<NotificationMessage.Notification, String>, ObservableValue<String>>() {
            @Override
            public ObservableValue<String> call(TableColumn.CellDataFeatures<NotificationMessage.Notification, String> cellData) {
                NotificationMessage.Notification action = cellData.getValue();
                return new SimpleObjectProperty<>(action.getTitle());
            }
        });

        this.comments.setCellValueFactory(new Callback<TableColumn.CellDataFeatures<NotificationMessage.Notification, String>, ObservableValue<String>>() {
            @Override
            public ObservableValue<String> call(TableColumn.CellDataFeatures<NotificationMessage.Notification, String> cellData) {
                NotificationMessage.Notification action = cellData.getValue();
                return new SimpleObjectProperty<>(action.getComments());
            }
        });

        this.securityExchange.setCellValueFactory(new Callback<TableColumn.CellDataFeatures<NotificationMessage.Notification, String>, ObservableValue<String>>() {
            @Override
            public ObservableValue<String> call(TableColumn.CellDataFeatures<NotificationMessage.Notification, String> cellData) {
                NotificationMessage.Notification action = cellData.getValue();
                return new SimpleObjectProperty<>(action.getSecurityExchange());
            }
        });

        this.component.setCellValueFactory(new Callback<TableColumn.CellDataFeatures<NotificationMessage.Notification, NotificationMessage.Component>, ObservableValue<NotificationMessage.Component>>() {
            @Override
            public ObservableValue<NotificationMessage.Component> call(TableColumn.CellDataFeatures<NotificationMessage.Notification, NotificationMessage.Component> cellData) {
                NotificationMessage.Notification action = cellData.getValue();
                return new SimpleObjectProperty<>(action.getComponent());
            }
        });

        this.state.setCellValueFactory(new Callback<TableColumn.CellDataFeatures<NotificationMessage.Notification, NotificationMessage.Level>, ObservableValue<NotificationMessage.Level>>() {
            @Override
            public ObservableValue<NotificationMessage.Level> call(TableColumn.CellDataFeatures<NotificationMessage.Notification, NotificationMessage.Level> cellData) {
                NotificationMessage.Notification action = cellData.getValue();
                return new SimpleObjectProperty<>(action.getLevel());
            }
        });

        this.time.setCellValueFactory(new Callback<TableColumn.CellDataFeatures<NotificationMessage.Notification, Timestamp>, ObservableValue<Timestamp>>() {
            @Override
            public ObservableValue<Timestamp> call(TableColumn.CellDataFeatures<NotificationMessage.Notification, Timestamp> cellData) {
                NotificationMessage.Notification action = cellData.getValue();
                return new SimpleObjectProperty<>(action.getTime());
            }
        });


        this.notificationTableView.setRowFactory(tableView -> new TableRow<NotificationMessage.Notification>() {
            @Override
            protected void updateItem(NotificationMessage.Notification item, boolean empty) {
                super.updateItem(item, empty);

                if (empty || item == null) {
                    setStyle("");
                } else {
                    NotificationMessage.Level state = item.getLevel();
                    getStyleClass().removeAll("row-success", "row-info", "row-error", "row-warn");

                    if (state == NotificationMessage.Level.SUCCESS) {
                        getStyleClass().add("row-success");
                    } else if (state == NotificationMessage.Level.ERROR || state == NotificationMessage.Level.FATAL) {
                        getStyleClass().add("row-error");
                    } else if (state == NotificationMessage.Level.WARN) {
                        getStyleClass().add("row-warn");
                    } else {
                        getStyleClass().add("row-info");
                    }
                }
            }
        });


        time.setCellFactory(column -> {
            TableCell<NotificationMessage.Notification, Timestamp> cell = new TableCell<NotificationMessage.Notification, Timestamp>() {
                @Override
                protected void updateItem(Timestamp item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || item == null) {
                        setText(null);
                    } else {
                        Instant instant = Instant.ofEpochSecond(item.getSeconds(), item.getNanos());
                        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);
                        ZonedDateTime zonedDateTimeChile = zonedDateTime.withZoneSameInstant(Repository.getZoneID());
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
                        String formattedDateTime = zonedDateTimeChile.format(formatter);
                        setText(formattedDateTime);
                    }
                }
            };
            return cell;
        });


        notificationTableView.getSortOrder().add(time);
        time.setSortType(TableColumn.SortType.DESCENDING);

    }

}
