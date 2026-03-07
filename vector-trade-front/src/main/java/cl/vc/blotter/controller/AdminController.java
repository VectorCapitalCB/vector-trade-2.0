package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.beans.property.SimpleObjectProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.collections.transformation.SortedList;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.control.cell.TextFieldTableCell;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import javafx.util.StringConverter;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.keycloak.representations.idm.GroupRepresentation;

import java.util.ArrayList;

@Slf4j
@Data
public class AdminController {

    public FilteredList<BlotterMessage.User> filteredData;
    public ObservableList<BlotterMessage.User> data;
    public SortedList<BlotterMessage.User> sortedData;
    @FXML
    private TableView<BlotterMessage.User> users;
    @FXML
    private TableColumn<BlotterMessage.User, String> id;
    @FXML
    private TableColumn<BlotterMessage.User, String> username;
    @FXML
    private TableColumn<BlotterMessage.User, String> email;
    @FXML
    private TableColumn<BlotterMessage.User, String> fname;
    @FXML
    private TableColumn<BlotterMessage.User, String> lname;
    @FXML
    private TableColumn<BlotterMessage.User, String> margin;
    @FXML
    private TableColumn<BlotterMessage.User, String> phone;
    @FXML
    private TableColumn<BlotterMessage.User, String> account;
    @FXML
    private TableColumn<BlotterMessage.User, String> codeoperator;
    @FXML
    private TextField filter;
    @FXML
    private TabPane tabPrincipalAdmin;
    @FXML
    private VBox vboxAdmind;
    @FXML
    private ComboBox<GroupRepresentation> cmbGroupDetails;
    @FXML
    private TextField txtUsername;
    @FXML
    private TextField txtEmail;
    @FXML
    private TextField txtFirstName;
    @FXML
    private TextField txtLastName;
    @FXML
    private TextField txtPassword;
    @FXML
    private TextField txtPassword2;
    @FXML
    private TextField txtMargin;
    @FXML
    private TextField txtPhone;
    @FXML
    private Button btnAceptar;
    @FXML
    private Button btnCancelar;
    @FXML
    private AnchorPane addusercontroller;
    @FXML
    private CheckBox myCheckBox;

    @FXML
    private void initialize() {
        try {

            Repository.setAdminController(this);

            TableColumn<BlotterMessage.User, Void> chkcol = new TableColumn<>("Enable Users");
            chkcol.setCellFactory(col -> new TableCell<BlotterMessage.User, Void>() {
                private final CheckBox myCheckBox = new CheckBox("");

                {
                    myCheckBox.setOnAction(event -> {
                        BlotterMessage.User.Builder user = getTableView().getItems().get(getIndex()).toBuilder();
                        //user.setActive();
                        boolean newStatus = myCheckBox.isSelected();
                        user.setActive(newStatus);
                        Repository.getClientService().sendMessage(user.build());
                        myCheckBox.setSelected(newStatus);

                    });
                }

                @Override
                protected void updateItem(Void item, boolean empty) {
                    super.updateItem(item, empty);
                    if (empty || getTableRow() == null || getTableRow().getItem() == null) {
                        setGraphic(null);
                    } else {
                        BlotterMessage.User user = getTableView().getItems().get(getIndex());
                        myCheckBox.setSelected(user.getActive());
                        setGraphic(myCheckBox);
                    }
                }
            });

            users.getColumns().add(chkcol);
            this.data = FXCollections.observableArrayList(new ArrayList<>());
            this.filteredData = new FilteredList<>(this.data, p -> true);
            this.sortedData = new SortedList<>(this.filteredData);
            this.sortedData.comparatorProperty().bind(users.comparatorProperty());
            this.users.setItems(this.sortedData);
            this.users.setEditable(true);
            this.users.getSortOrder().add(this.username);
            this.users.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);
            this.users.autosize();
            users.sort();

            id.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getId()));
            username.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getUsername()));
            email.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getEmail()));
            fname.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getFname()));
            lname.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getLname()));
            //margin.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getMargin()));
            phone.setCellValueFactory(cellData -> new SimpleObjectProperty<>(cellData.getValue().getPhone()));

            email.setCellFactory(TextFieldTableCell.forTableColumn());
            fname.setCellFactory(TextFieldTableCell.forTableColumn());
            lname.setCellFactory(TextFieldTableCell.forTableColumn());

            phone.setCellFactory(TextFieldTableCell.forTableColumn());
            margin.setCellFactory(TextFieldTableCell.forTableColumn(new StringConverter<String>() {
                @Override
                public String toString(String object) {
                    return object;
                }

                @Override
                public String fromString(String string) {
                    return string;
                }
            }));
            email.setCellFactory(TextFieldTableCell.forTableColumn(new StringConverter<String>() {
                @Override
                public String toString(String object) {
                    return object;
                }

                @Override
                public String fromString(String string) {
                    return string;
                }
            }));

            margin.setOnEditCommit(event -> {
                BlotterMessage.User user = event.getTableView().getItems().get(event.getTablePosition().getRow());
                /*
                user.setMargin(event.getNewValue());
                try {
                    Repository.getKeycloakService().updateUserMargin(user.getId(), event.getNewValue());
                    Platform.runLater(() -> {
                        Alert alert = new Alert(Alert.AlertType.INFORMATION);
                        alert.setTitle("Successful Update");
                        alert.setHeaderText(null);
                        DialogPane dialogPane = alert.getDialogPane();
                        dialogPane.getStylesheets().add(Repository.getSTYLE());
                        dialogPane.getStyleClass().add("my-alert");
                        alert.setContentText("The margin has been updated successfully.");
                        alert.showAndWait();
                    });
                } catch (Exception e) {
                    log.error("No se pudo actualizar el margen para el usuario: " + user.getId(), e);
                }
                 */
            });

            email.setOnEditCommit(event -> {

                BlotterMessage.User.Builder user = event.getTableView().getItems().get(event.getTablePosition().getRow()).toBuilder();
                user.setStatusUser(BlotterMessage.StatusUser.UPDATE_USER);
                user.setEmail(event.getNewValue());

                try {

                    Repository.getClientService().sendMessage(user.build());

                    Platform.runLater(() -> {
                        Alert alert = new Alert(Alert.AlertType.INFORMATION);
                        alert.setTitle("Successful Update");
                        alert.setHeaderText(null);
                        DialogPane dialogPane = alert.getDialogPane();
                        dialogPane.getStylesheets().add(Repository.getSTYLE());
                        dialogPane.getStyleClass().add("my-alert");
                        alert.setContentText("The email has been updated successfully.");
                        alert.showAndWait();
                    });
                } catch (Exception e) {
                    log.error("No se pudo actualizar el email para el usuario.", e);
                }
            });

            fname.setOnEditCommit(event -> {
                BlotterMessage.User.Builder user = event.getTableView().getItems().get(event.getTablePosition().getRow()).toBuilder();
                user.setStatusUser(BlotterMessage.StatusUser.UPDATE_USER);
                user.setFname(event.getNewValue());
                try {
                    Repository.getClientService().sendMessage(user.build());
                    Platform.runLater(() -> {
                        Alert alert = new Alert(Alert.AlertType.INFORMATION);
                        alert.setTitle("Successful Update");
                        alert.setHeaderText(null);
                        DialogPane dialogPane = alert.getDialogPane();
                        dialogPane.getStylesheets().add(Repository.getSTYLE());
                        dialogPane.getStyleClass().add("my-alert");
                        alert.setContentText("The first name has been updated successfully.");
                        alert.showAndWait();
                    });
                } catch (Exception e) {
                    log.error("No se pudo actualizar el nombre para el usuario.", e);
                }
            });
            lname.setOnEditCommit(event -> {
                BlotterMessage.User.Builder user = event.getTableView().getItems().get(event.getTablePosition().getRow()).toBuilder();
                user.setStatusUser(BlotterMessage.StatusUser.UPDATE_USER);
                user.setLname(event.getNewValue());
                try {
                    Repository.getClientService().sendMessage(user.build());
                    Platform.runLater(() -> {
                        alert("The last name has been updated successfully.");
                    });
                } catch (Exception e) {
                    log.error("No se pudo actualizar el apellido para el usuario.", e);
                }
            });
            phone.setOnEditCommit(event -> {
                BlotterMessage.User.Builder user = event.getRowValue().toBuilder();
                user.setStatusUser(BlotterMessage.StatusUser.UPDATE_USER);
                user.setPhone(event.getNewValue());
                try {
                    Repository.getClientService().sendMessage(user.build());
                    Platform.runLater(() -> {
                        alert("The phone has been updated successfully.");
                    });
                } catch (Exception e) {
                    log.error("No se pudo actualizar el teléfono para el usuario.", e);
                }
            });

            filter.textProperty().addListener((observable, oldValue, newValue) -> setFilter(newValue));

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }


    public void setFilter(String filter) {
        filteredData.predicateProperty().bind(Bindings.createObjectBinding(() -> exec -> {
                    try {
                        if (filter.equals("")) {
                            return true;
                        }
                        if (exec.getUsername().contains(filter) || exec.getEmail().contains(filter)
                                || exec.getFname().contains(filter) || exec.getLname().contains(filter) || exec.getId().contains(filter)
                                || exec.getPhone().contains(filter)) {
                            return true;
                        }
                        return false;
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        return false;
                    }
                },
                this.filter.textProperty()
        ));
        users.refresh();
    }

    private void alert(String text) {
        Alert alert = new Alert(Alert.AlertType.INFORMATION);
        alert.setTitle("Successful Update");
        alert.setHeaderText(null);
        DialogPane dialogPane = alert.getDialogPane();
        dialogPane.getStylesheets().add(Repository.getSTYLE());
        dialogPane.getStyleClass().add("my-alert");
        alert.setContentText(text);
        alert.showAndWait();
    }

    @FXML
    public void addUsername(ActionEvent event) {
        try {
            Stage stage = (Stage) ((Node) event.getSource()).getScene().getWindow();
            stage.close();
            FXMLLoader loader = new FXMLLoader(getClass().getResource("/view/AddUser.fxml"));
            Parent root = loader.load();
            Scene scene = new Scene(root);
            scene.getStylesheets().add(Repository.getSTYLE());
            stage.setScene(scene);
            stage.show();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void updateUsers(BlotterMessage.UserList userList) {
        Platform.runLater(() -> {
            data.clear();
            data.addAll(userList.getUsersList());
            users.refresh();
        });

    }

}
