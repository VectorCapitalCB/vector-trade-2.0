package cl.vc.blotter.utils;

import javafx.scene.media.Media;
import javafx.scene.media.MediaPlayer;
import javafx.util.Duration;

import java.net.URL;

public class SoundPlayer {

    public void playSound(String soundFileName) {
        try {
            URL resource = getClass().getResource("/" + soundFileName);
            if (resource == null) {
                throw new IllegalArgumentException("Archivo no encontrado: " + soundFileName);
            }
            Media media = new Media(resource.toString());
            MediaPlayer mediaPlayer = new MediaPlayer(media);
            mediaPlayer.setOnError(() -> System.err.println("Error de MediaPlayer: " + mediaPlayer.getError()));
            mediaPlayer.setStartTime(Duration.ZERO);
            mediaPlayer.play();
        } catch (Exception e) {
            throw new IllegalArgumentException("Error al intentar reproducir el archivo: " + soundFileName, e);
        }
    }

    public void playSoundNew() {
        playSound("sounds/new.mp3");
    }
}





