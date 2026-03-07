package cl.vc.inyectorcandle.fix;

import cl.vc.inyectorcandle.model.InstrumentKey;
import cl.vc.inyectorcandle.model.SecurityDefinition;
import quickfix.FieldNotFound;
import quickfix.field.*;
import quickfix.fix44.SecurityList;

import java.util.ArrayList;
import java.util.List;

public final class SecurityListParser {

    private SecurityListParser() {
    }

    public static List<SecurityDefinition> parse(SecurityList message) throws FieldNotFound {
        int count = message.getInt(NoRelatedSym.FIELD);
        List<SecurityDefinition> result = new ArrayList<>(count);

        for (int i = 1; i <= count; i++) {
            SecurityList.NoRelatedSym group = new SecurityList.NoRelatedSym();
            message.getGroup(i, group);

            String symbol = read(group, Symbol.FIELD, "UNKNOWN");
            String settlement = read(group, SettlType.FIELD, read(group, BookingRefID.FIELD, "UNKNOWN_SETTL"));
            String destination = read(group, SecurityExchange.FIELD, read(message, SecurityExchange.FIELD, "BCS"));
            String currency = read(group, Currency.FIELD, read(message, Currency.FIELD, "CLP"));
            String securityId = read(group, SecurityID.FIELD, null);
            String desc = read(group, SecurityDesc.FIELD, null);
            String securityType = read(group, SecurityType.FIELD, null);
            String bookingRefId = read(group, BookingRefID.FIELD, null);

            InstrumentKey key = InstrumentKey.fromValues(symbol, settlement, destination, currency, securityType);
            result.add(new SecurityDefinition(key, securityId, desc, securityType, bookingRefId));
        }

        return result;
    }

    private static String read(quickfix.FieldMap fieldMap, int field, String fallback) {
        try {
            if (fieldMap.isSetField(field)) {
                return fieldMap.getString(field);
            }
            return fallback;
        } catch (FieldNotFound e) {
            return fallback;
        }
    }
}
