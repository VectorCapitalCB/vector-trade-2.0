package cl.vc.blotter.controller;

import cl.vc.blotter.Repository;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.VBox;
import javafx.stage.Stage;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.keycloak.representations.idm.GroupRepresentation;

import java.io.IOException;

@Slf4j
@Data
public class AddUserController {

    @FXML
    private TabPane tabPrincipalAdmin;
    @FXML
    private VBox vboxAdmind;
    @FXML
    private ComboBox<String> cmbGrupos;
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
    private TextField txtAccount;
    @FXML
    private TextField txtCodeoperator;

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


/*
        Platform.runLater(() -> {
            try {
                List<GroupRepresentation> groups = Repository.getKeycloakService().getAllGroups();
                if (groups != null && !groups.isEmpty()) {
                    ObservableList<String> groupNames = FXCollections.observableArrayList();
                    for (GroupRepresentation group : groups) {
                        groupNames.add(group.getName());
                    }

                    if(cmbGrupos == null){
                        return;
                    }
                    cmbGrupos.setItems(groupNames);
                    cmbGrupos.getSelectionModel().selectedItemProperty().addListener((obs, oldSelection, newSelection) -> {
                        if (newSelection != null) {
                            GroupRepresentation selectedGroup = groups.stream()
                                    .filter(group -> group.getName().equals(newSelection))
                                    .findFirst()
                                    .orElse(null);
                            if (selectedGroup != null) {
                                GroupRepresentation getGroupDetails = Repository.getKeycloakService().getGroupDetails(selectedGroup.getId());
                                ObservableList<GroupRepresentation> subGroupDetails = FXCollections.observableArrayList();
                                if (getGroupDetails.getSubGroups() != null && !getGroupDetails.getSubGroups().isEmpty()) {
                                    subGroupDetails.addAll(getGroupDetails.getSubGroups());
                                    cmbGroupDetails.setItems(subGroupDetails);
                                    cmbGroupDetails.setDisable(false);
                                    cmbGroupDetails.setCellFactory(lv -> new ListCell<GroupRepresentation>() {
                                        @Override
                                        protected void updateItem(GroupRepresentation group, boolean empty) {
                                            super.updateItem(group, empty);
                                            setText(empty ? "" : group.getName());
                                        }
                                    });
                                    cmbGroupDetails.setConverter(new StringConverter<GroupRepresentation>() {
                                        @Override
                                        public String toString(GroupRepresentation group) {
                                            return group == null ? null : group.getName();
                                        }
                                        @Override
                                        public GroupRepresentation fromString(String groupName) {
                                            return null;
                                        }
                                    });
                                    cmbGroupDetails.getSelectionModel().selectFirst();
                                } else {
                                    cmbGroupDetails.getItems().clear();
                                    cmbGroupDetails.setDisable(true);
                                }
                            }
                        }
                    });
                } else {
                    log.info("No se encontraron grupos.");
                }
            } catch (Exception e) {
                log.error("Error al obtener los grupos: {}", e.getMessage(), e);
            }
        });

 */

    }


    @FXML
    public void handleSaveUserAction(ActionEvent event) {
        try {
            String username = txtUsername.getText();
            String email = txtEmail.getText();
            String firstName = txtFirstName.getText();
            String lastName = txtLastName.getText();
            String password = txtPassword.getText();
            String password2 = txtPassword2.getText();
            String margin = txtMargin.getText();
            String phone = txtPhone.getText();
            String account = txtAccount.getText();
            String codeoperator = txtCodeoperator.getText();
            String groupName = cmbGrupos.getSelectionModel().getSelectedItem();
            GroupRepresentation selectedSubGroup = cmbGroupDetails.getSelectionModel().getSelectedItem();
            String subgroupName = selectedSubGroup != null ? selectedSubGroup.getName() : null;

            if (username.isEmpty() || email.isEmpty() || firstName.isEmpty() || lastName.isEmpty() ||
                    password.isEmpty() || password2.isEmpty() || margin.isEmpty() || phone.isEmpty() ||
                    groupName == null || subgroupName == null) {
                alert("One or more fields are empty.");
                return;
            }
            if (!password.equals(password2)) {
                alert("The passwords do not match.");
                return;
            }
            //Repository.getKeycloakService().createUser(username, email, firstName, lastName, password,password2, margin, phone, groupName, subgroupName, account, codeoperator);
            alert("User created successfully.");
            txtUsername.clear();
            txtEmail.clear();
            txtFirstName.clear();
            txtLastName.clear();
            txtPassword.clear();
            txtPassword2.clear();
            txtMargin.clear();
            txtPhone.clear();
            txtAccount.clear();
            txtCodeoperator.clear();
            cmbGrupos.getSelectionModel().clearSelection();
            cmbGroupDetails.getSelectionModel().clearSelection();

        } catch (IllegalStateException e) {
            alert("Error in component initialization.");
        } catch (Exception e) {
            e.printStackTrace();
            alert("The user already exists in the system");
        }
    }

    private void alert(String message) {
        Alert alert = new Alert(Alert.AlertType.INFORMATION);
        alert.setTitle("Information Dialog");
        alert.setHeaderText(null);
        alert.setContentText(message);
        alert.showAndWait();
    }

    @FXML
    private void handleCancelarAction(ActionEvent event) throws IOException {
        Node source = (Node) event.getSource();
        Stage stage = (Stage) source.getScene().getWindow();
        stage.close();
        FXMLLoader loader = new FXMLLoader(this.getClass().getResource("/view/AdminView.fxml"));

        AnchorPane mainPane = loader.load();
        AdminController adminController = loader.getController();
        Repository.setAdminController(adminController);

        Scene scene = new Scene(mainPane);
        scene.getStylesheets().add(getClass().getResource(Repository.getSTYLE()).toExternalForm());
        stage.setScene(scene);
        stage.show();
    }

}



