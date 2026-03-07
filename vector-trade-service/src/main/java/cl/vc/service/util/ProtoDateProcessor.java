package cl.vc.service.util;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;

import java.time.Instant;

public final class ProtoDateProcessor {

    private ProtoDateProcessor() {}

    public static void setDateProcesorIfMissing(Message.Builder builder) {
        if (builder == null) {
            return;
        }

        try {
            Descriptors.FieldDescriptor fd = builder.getDescriptorForType().findFieldByName("date_procesor");
            if (fd == null) {
                return;
            }

            if (builder.hasField(fd)) {
                return;
            }

            Instant now = Instant.now();
            Timestamp ts = Timestamp.newBuilder()
                    .setSeconds(now.getEpochSecond())
                    .setNanos(now.getNano())
                    .build();
            builder.setField(fd, ts);
        } catch (Exception ignored) {
            // Keep compatibility with older proto jars where the field may not exist.
        }
    }
}

