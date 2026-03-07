package cl.vc.blotter.utils;

import javafx.beans.property.BooleanProperty;
import javafx.beans.property.SimpleBooleanProperty;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public class UsernameObject {
    private BooleanProperty active = new SimpleBooleanProperty();
    private String id;
    private String username;
    private String email;
    private String fname;
    private String lname;
    private String margin;
    private String phone;
    private String account;
    private String codeoperator;

    public UsernameObject() {
        this.active.addListener((obs, wasActive, isActive) -> {
            System.out.println("Status changed for: " + getUsername() + " from " + (wasActive ? "active" : "inactive") + " to " + (isActive ? "active" : "inactive"));
        });
    }

    public final BooleanProperty activeProperty() {
        return this.active;
    }

    public final boolean isActive() {
        return this.active.get();
    }

    public final void setActive(boolean active) {
        this.active.set(active);
    }

}
