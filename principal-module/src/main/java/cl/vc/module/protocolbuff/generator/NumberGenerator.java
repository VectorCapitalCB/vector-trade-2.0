package cl.vc.module.protocolbuff.generator;

import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.Locale;

public class NumberGenerator {

    public static DecimalFormatSymbols formatoMilesDecimal() {
        DecimalFormatSymbols symbols = new DecimalFormatSymbols(new Locale("es", "ES"));
        symbols.setDecimalSeparator('.');
        symbols.setGroupingSeparator(',');
        return symbols;
    }

    public static DecimalFormat getFormatNumberMil() {
        DecimalFormat decFormat = new DecimalFormat("#,###,###");
        decFormat.setDecimalFormatSymbols(formatoMilesDecimal());
        return decFormat;
    }


    public static String formatDouble(Double px) {
        NumberFormat numberFormat = NumberFormat.getNumberInstance(new Locale("es", "ES"));
        return numberFormat.format(px);
    }

    public static DecimalFormat getFormatNumberMilDec() {
        DecimalFormat decFormat = new DecimalFormat("#,###,###,##0.00000");
        decFormat.setDecimalFormatSymbols(formatoMilesDecimal());
        return decFormat;
    }

    public static DecimalFormat getFormatNumberMilDec(RoutingMessage.SecurityExchangeRouting securityExchangeRouting) {

        if (securityExchangeRouting.equals(RoutingMessage.SecurityExchangeRouting.XSGO) ||
                securityExchangeRouting.equals(RoutingMessage.SecurityExchangeRouting.IB_SMART) ||
                securityExchangeRouting.equals(RoutingMessage.SecurityExchangeRouting.BBG) ||
                securityExchangeRouting.equals(RoutingMessage.SecurityExchangeRouting.BASKETS) ||
                securityExchangeRouting.equals(RoutingMessage.SecurityExchangeRouting.XSGO_OFS)) {

            DecimalFormat decFormat = new DecimalFormat("#,###,###,##0");
            decFormat.setDecimalFormatSymbols(formatoMilesDecimal());
            return decFormat;

        } else if (securityExchangeRouting.equals(RoutingMessage.SecurityExchangeRouting.BINANCE) ||
                securityExchangeRouting.equals(RoutingMessage.SecurityExchangeRouting.CRYPTO_MARKET) ||
                securityExchangeRouting.equals(RoutingMessage.SecurityExchangeRouting.MEXC)) {

            DecimalFormat decFormat = new DecimalFormat("#,###,###,##0.00000");
            decFormat.setDecimalFormatSymbols(formatoMilesDecimal());
            return decFormat;
        }


        DecimalFormat decFormat = new DecimalFormat("#,###,###,##0.00000");
        decFormat.setDecimalFormatSymbols(formatoMilesDecimal());
        return decFormat;

    }

    public static DecimalFormat getFormatNumberMilDec(MarketDataMessage.SecurityExchangeMarketData securityExchangemkd) {

        if (securityExchangemkd.equals(MarketDataMessage.SecurityExchangeMarketData.BCS) ||
                securityExchangemkd.equals(MarketDataMessage.SecurityExchangeMarketData.FH_IBKR) ||
                securityExchangemkd.equals(MarketDataMessage.SecurityExchangeMarketData.BVC) ||
                securityExchangemkd.equals(MarketDataMessage.SecurityExchangeMarketData.BVL)) {

            DecimalFormat decFormat = new DecimalFormat("#,###,###,##0");
            decFormat.setDecimalFormatSymbols(formatoMilesDecimal());
            return decFormat;

        } else if (securityExchangemkd.equals(MarketDataMessage.SecurityExchangeMarketData.BINANCE_MKD) ||
                securityExchangemkd.equals(MarketDataMessage.SecurityExchangeMarketData.CRYPTO_MARKET_MKD) ||
                securityExchangemkd.equals(MarketDataMessage.SecurityExchangeMarketData.MEXC_MKD)) {

            DecimalFormat decFormat = new DecimalFormat("#,###,###,##0.0000000");
            decFormat.setDecimalFormatSymbols(formatoMilesDecimal());
            return decFormat;
        }


        DecimalFormat decFormat = new DecimalFormat("#,###,###,##0.00000");
        decFormat.setDecimalFormatSymbols(formatoMilesDecimal());
        return decFormat;

    }

    public static DecimalFormat getFormatNumberMilDecPrice(MarketDataMessage.SecurityExchangeMarketData securityExchangemkd) {

        if (securityExchangemkd.equals(MarketDataMessage.SecurityExchangeMarketData.BCS) ||
                securityExchangemkd.equals(MarketDataMessage.SecurityExchangeMarketData.FH_IBKR) ||
                securityExchangemkd.equals(MarketDataMessage.SecurityExchangeMarketData.BVC) ||
                securityExchangemkd.equals(MarketDataMessage.SecurityExchangeMarketData.BVL)) {

            DecimalFormat decFormat = new DecimalFormat("#,###,###,##0.00");
            decFormat.setDecimalFormatSymbols(formatoMilesDecimal());
            return decFormat;

        } else if (securityExchangemkd.equals(MarketDataMessage.SecurityExchangeMarketData.BINANCE_MKD) ||
                securityExchangemkd.equals(MarketDataMessage.SecurityExchangeMarketData.CRYPTO_MARKET_MKD) ||
                securityExchangemkd.equals(MarketDataMessage.SecurityExchangeMarketData.MEXC_MKD)) {

            DecimalFormat decFormat = new DecimalFormat("#,###,###,##0.0000000");
            decFormat.setDecimalFormatSymbols(formatoMilesDecimal());
            return decFormat;
        }


        DecimalFormat decFormat = new DecimalFormat("#,###,###,##0.00000");
        decFormat.setDecimalFormatSymbols(formatoMilesDecimal());
        return decFormat;

    }


    public static DecimalFormat formetByticks(BigDecimal ticks) {
        DecimalFormat decFormat;
        Double ticksD = ticks.doubleValue();

        if (ticksD <= 0) {
            decFormat = new DecimalFormat("#,###,###,##0.0000");
            decFormat.setDecimalFormatSymbols(NumberGenerator.formatoMilesDecimal());
            return decFormat;
        } else if(ticksD == 0.01) {
            decFormat = new DecimalFormat("#,###,###,##0.00");
            decFormat.setDecimalFormatSymbols(formatoMilesDecimal());
            return decFormat;
        } else if(ticksD == 0.001) {
            decFormat = new DecimalFormat("#,###,###,##0.0000");
            decFormat.setDecimalFormatSymbols(formatoMilesDecimal());
            return decFormat;
        } else if(ticksD == 1) {
            decFormat = new DecimalFormat("#,###,###,##0");
            decFormat.setDecimalFormatSymbols(formatoMilesDecimal());
            return decFormat;
        } else if(ticksD == 10) {
            decFormat = new DecimalFormat("#,###,###,##0");
            decFormat.setDecimalFormatSymbols(formatoMilesDecimal());
            return decFormat;
        } else {
            decFormat = new DecimalFormat("#,###,###,##0.0000");
            decFormat.setDecimalFormatSymbols(formatoMilesDecimal());
            return decFormat;
        }
    }

}
