package cl.vc.module.protocolbuff.ticks;

import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class Ticks {

    public static final RoundingMode DEFAULT_ROUNDING_MODE = RoundingMode.HALF_UP;
    public static final BigDecimal DEFAULT_TOLERANCE = BigDecimal.valueOf(1e-5);



    public static BigDecimal conversorExdestination(MarketDataMessage.SecurityExchangeMarketData securityExchangeRouting, BigDecimal price) {

        if(securityExchangeRouting.equals(MarketDataMessage.SecurityExchangeMarketData.BCS)){
            return xsgo(price);
        } else if(securityExchangeRouting.equals(MarketDataMessage.SecurityExchangeMarketData.FH_IBKR)){
            return eeuu(price);
        } else if(securityExchangeRouting.equals(MarketDataMessage.SecurityExchangeMarketData.BINANCE_MKD)){
            return binance(price);
        } else if(securityExchangeRouting.equals(MarketDataMessage.SecurityExchangeMarketData.MEXC_MKD)){
            return mexc(price);
        } else if(securityExchangeRouting.equals(MarketDataMessage.SecurityExchangeMarketData.CRYPTO_MARKET_MKD)){
            return cryptomarket(price);
        }

        return price;

    }



    public static BigDecimal getTick(RoutingMessage.SecurityExchangeRouting securityExchangeRouting, BigDecimal price) {

        if(securityExchangeRouting.equals(RoutingMessage.SecurityExchangeRouting.XSGO) || securityExchangeRouting.equals(RoutingMessage.SecurityExchangeRouting.XSGO_OFS)){
           return xsgo(price);
        } else if(securityExchangeRouting.equals(RoutingMessage.SecurityExchangeRouting.IB_SMART)
                || securityExchangeRouting.equals(RoutingMessage.SecurityExchangeRouting.RT_XP) ){
            return eeuu(price);
        } else if(securityExchangeRouting.equals(RoutingMessage.SecurityExchangeRouting.BINANCE)){
            return binance(price);
        } else if(securityExchangeRouting.equals(RoutingMessage.SecurityExchangeRouting.MEXC)){
            return mexc(price);
        } else if(securityExchangeRouting.equals(RoutingMessage.SecurityExchangeRouting.CRYPTO_MARKET)){
            return cryptomarket(price);
        }

        return price;

    }


    public static BigDecimal roundToTick(BigDecimal price, BigDecimal tick) {
        return roundToTick(price, tick, DEFAULT_ROUNDING_MODE);
    }



    public static BigDecimal roundToTick(BigDecimal price, BigDecimal tick, RoundingMode roundingMode) {
        if ((price == null) || (tick == null) || (tick.abs().compareTo(DEFAULT_TOLERANCE) <= 0)) {
            return price;

        } else {
            BigDecimal newPrice = price.divide(tick, 0, roundingMode);
            return newPrice.multiply(tick).setScale(tick.scale(), DEFAULT_ROUNDING_MODE);
        }
    }

    public static BigDecimal xsgo(BigDecimal price) {
        if ((price.compareTo(BigDecimal.ZERO) > 0) && (price.compareTo(BigDecimal.valueOf(10d)) < 0)) {
            return BigDecimal.valueOf(0.001).setScale(3, DEFAULT_ROUNDING_MODE);

        } else if ((price.compareTo(BigDecimal.valueOf(10d)) >= 0)
                && (price.compareTo(BigDecimal.valueOf(1000d)) < 0)) {
            return BigDecimal.valueOf(0.01).setScale(2, DEFAULT_ROUNDING_MODE);

        } else if ((price.compareTo(BigDecimal.valueOf(1000d)) >= 0)
                && (price.compareTo(BigDecimal.valueOf(10000d)) < 0)) {
            return BigDecimal.valueOf(0.1).setScale(1, DEFAULT_ROUNDING_MODE);

        } else if (price.compareTo(BigDecimal.valueOf(10000d)) >= 0 && price.compareTo(BigDecimal.valueOf(100000d)) < 0 ) {
            return BigDecimal.ONE;

        } else if (price.compareTo(BigDecimal.valueOf(100000d)) >= 0) {
            return BigDecimal.TEN;

        } else {
            return BigDecimal.ZERO;
        }
    }



    public static BigDecimal eeuu(BigDecimal price) {
        return BigDecimal.valueOf(0.01).setScale(3, DEFAULT_ROUNDING_MODE);
    }

    public static BigDecimal binance(BigDecimal price) {

        if ((price.compareTo(BigDecimal.valueOf(0.1d)) > 0)
                && (price.compareTo(BigDecimal.valueOf(1d)) <= 0)) {

            return BigDecimal.valueOf(0.0001).setScale(4, DEFAULT_ROUNDING_MODE);

        }else if ((price.compareTo(BigDecimal.valueOf(0.00001d)) > 0)
                && (price.compareTo(BigDecimal.valueOf(0.00010d)) <= 0)) {

            return BigDecimal.valueOf(0.000001).setScale(6, DEFAULT_ROUNDING_MODE);


        } else if ((price.compareTo(BigDecimal.valueOf(1500d)) > 0)
                && (price.compareTo(BigDecimal.valueOf(80000d)) <= 0)) {

            return BigDecimal.valueOf(0.01).setScale(2, DEFAULT_ROUNDING_MODE);


        } else {
            return BigDecimal.ZERO;
        }
    }

    public static BigDecimal mexc(BigDecimal price) {

        if ((price.compareTo(BigDecimal.valueOf(0.1d)) > 0)
                && (price.compareTo(BigDecimal.valueOf(1d)) <= 0)) {

            return BigDecimal.valueOf(0.0001).setScale(4, DEFAULT_ROUNDING_MODE);

        }else if ((price.compareTo(BigDecimal.valueOf(0.00001d)) > 0)
                && (price.compareTo(BigDecimal.valueOf(0.00010d)) <= 0)) {

            return BigDecimal.valueOf(0.000001).setScale(6, DEFAULT_ROUNDING_MODE);


        } else if ((price.compareTo(BigDecimal.valueOf(1500d)) > 0)
                && (price.compareTo(BigDecimal.valueOf(80000d)) <= 0)) {

            return BigDecimal.valueOf(0.01).setScale(2, DEFAULT_ROUNDING_MODE);


        } else {
            return BigDecimal.ZERO;
        }
    }


    public static BigDecimal cryptomarket(BigDecimal price) {

        if ((price.compareTo(BigDecimal.valueOf(0.1d)) > 0)
                && (price.compareTo(BigDecimal.valueOf(1d)) <= 0)) {

            return BigDecimal.valueOf(0.0001).setScale(4, DEFAULT_ROUNDING_MODE);

        }else if ((price.compareTo(BigDecimal.valueOf(0.00001d)) > 0)
                && (price.compareTo(BigDecimal.valueOf(0.00010d)) <= 0)) {

            return BigDecimal.valueOf(0.000001).setScale(6, DEFAULT_ROUNDING_MODE);


        } else if ((price.compareTo(BigDecimal.valueOf(1500d)) > 0)
                && (price.compareTo(BigDecimal.valueOf(80000d)) <= 0)) {

            return BigDecimal.valueOf(0.01).setScale(2, DEFAULT_ROUNDING_MODE);


        } else {
            return BigDecimal.ZERO;
        }
    }


    public static Double applyRulePrice(RoutingMessage.SecurityExchangeRouting securityExchangeRouting, Double px){
        BigDecimal pxb = BigDecimal.valueOf(px);
        BigDecimal tr = getTick(securityExchangeRouting, pxb);
        return roundToTick(pxb, tr).doubleValue();
    }


    public static void main(String[] arg){

        applyRulePrice(RoutingMessage.SecurityExchangeRouting.XSGO, 106.111111d);


        System.out.println(xsgo(new BigDecimal(106.111111d)));
        System.out.println();
    }

}
