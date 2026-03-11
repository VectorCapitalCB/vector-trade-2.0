package cl.vc.module.protocolbuff.generator;

import com.google.protobuf.Timestamp;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class TimeGenerator {


    public static Timestamp toProtoTimestampUTC(LocalDateTime localDateTime, ZoneId zoneId) {
        Instant instant = localDateTime.atZone(zoneId).toInstant(); // convertir con la zona dada
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    public static Timestamp toProtoTimestampUTC(LocalDateTime localDateTime) {
        return Timestamp.newBuilder()
                .setSeconds(localDateTime.toEpochSecond(ZoneOffset.UTC))
                .setNanos(localDateTime.getNano())
                .build();
    }

    public static synchronized Timestamp getTimeXSGO(){
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("America/Santiago"));
        return Timestamp.newBuilder()
                .setSeconds(now.toEpochSecond())
                .setNanos(now.getNano())
                .build();
    }

    public static synchronized Timestamp getTimeGeneral(ZoneId zoneId){
        ZonedDateTime now = ZonedDateTime.now(zoneId);
        return Timestamp.newBuilder()
                .setSeconds(now.toEpochSecond())
                .setNanos(now.getNano())
                .build();
    }



    public  static synchronized Timestamp getTimeProto() {
        return Timestamp.newBuilder()
                .setSeconds(System.currentTimeMillis() / 1000)
                .setNanos((int) ((System.currentTimeMillis() % 1000) * 1_000_000))
                .build();
    }


    //for adr
    public static Long timeConverter(String timeStr) {
        try {

            if (timeStr == null) {
                return null;
            }

            String[] pieces = timeStr.trim().split(":");
            if (pieces.length < 2) {
                return null;
            }

            Calendar currentTime = Calendar.getInstance();
            currentTime.set(Calendar.HOUR_OF_DAY, Integer.parseInt(pieces[0]));
            currentTime.set(Calendar.MINUTE, Integer.parseInt(pieces[1]));
            currentTime.set(Calendar.SECOND, (pieces.length > 2) ? Integer.parseInt(pieces[2]) : 0);
            currentTime.set(Calendar.MILLISECOND, 0);

            return currentTime.getTimeInMillis();

        } catch (Exception exc) {
            return null;
        }
    }

    public static synchronized String getTimeProto(Timestamp timestamp, ZoneId id) {
        Instant instant = Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);
        ZonedDateTime zonedDateTimeChile = zonedDateTime.withZoneSameInstant(id);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
        return zonedDateTimeChile.format(formatter);
    }

    public static synchronized ZonedDateTime getTimeZonedDateTime(Timestamp timestamp, ZoneId id) {
        Instant instant = Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);
        return zonedDateTime.withZoneSameInstant(id);
    }

    public static Timestamp localDateTimeToTimestamp(LocalDateTime localDateTime, ZoneId zoneId) {
        Instant instant = localDateTime.toInstant(zoneId.getRules().getOffset(localDateTime));
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build();
    }

    public static Timestamp localTimeToTimestamp(LocalTime localTime, LocalDate date, ZoneId zoneId) {
        LocalDateTime localDateTime = LocalDateTime.of(date, localTime);
        return localDateTimeToTimestamp(localDateTime, zoneId);
    }

    public static LocalDateTime timestampToLocalDateTime(Timestamp timestamp, ZoneId zoneId) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos()).atZone(zoneId).toLocalDateTime();
    }

    public static Timestamp localDateTimeToTimestampUTC(LocalDateTime localDateTime) {
        return localDateTimeToTimestamp(localDateTime, ZoneOffset.UTC);
    }

    public static Timestamp localTimeToTimestampUTC(LocalTime localTime, LocalDate date) {
        return localTimeToTimestamp(localTime, date, ZoneOffset.UTC);
    }

    public static LocalDateTime timestampToLocalDateTimeInSystemZone(Timestamp timestamp) {
        return timestampToLocalDateTime(timestamp, ZoneId.systemDefault());
    }

}
