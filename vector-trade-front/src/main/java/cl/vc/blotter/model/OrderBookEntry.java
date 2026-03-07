package cl.vc.blotter.model;

import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.generator.NumberGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.ticks.Ticks;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import lombok.extern.slf4j.Slf4j;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;

@Slf4j
public class OrderBookEntry {

    private StringProperty price = new SimpleStringProperty();
    private StringProperty size = new SimpleStringProperty();

    private DecimalFormat decimalFormat;

    private DecimalFormat decimalFormatBkp = new DecimalFormat("#,##0.0000");
    private DecimalFormatSymbols usSymbols = DecimalFormatSymbols.getInstance(Locale.US);

    private StringProperty operator = new SimpleStringProperty();
    private StringProperty account = new SimpleStringProperty();
    private StringProperty symbol = new SimpleStringProperty();

    private MarketDataMessage.SecurityExchangeMarketData securityExchangeMarketData;
    private RoutingMessage.SecurityExchangeRouting securityExchangeRouting;

    private String id ;


    public OrderBookEntry(String id , double price, double size, DecimalFormat decimalFormat, String symbol, String account, String operator, MarketDataMessage.SecurityExchangeMarketData securityExchangeMarketData) {

        try {

            this.id = id;
            this.decimalFormat = decimalFormat;
            this.symbol.set(symbol);
            this.operator.set(operator);
            this.account.set(account);
            this.securityExchangeMarketData = securityExchangeMarketData;
            securityExchangeRouting = IDGenerator.conversorExdestination(securityExchangeMarketData);

            if (this.decimalFormat != null && price > 0d) {
                BigDecimal tick = Ticks.conversorExdestination(securityExchangeMarketData, BigDecimal.valueOf(price));
                this.decimalFormat = NumberGenerator.formetByticks(tick);
                this.decimalFormat.setDecimalFormatSymbols(usSymbols);
                this.price.set(this.decimalFormat.format(price));
                this.size.set(Repository.getFormatter0dec().format(size));


            } else if(this.decimalFormat == null && price > 0d) {
                BigDecimal tick = Ticks.conversorExdestination(securityExchangeMarketData, BigDecimal.valueOf(price));
                this.decimalFormat = NumberGenerator.formetByticks(tick);
                this.decimalFormat.setDecimalFormatSymbols(usSymbols);
                this.price.set(this.decimalFormat.format(price));
                this.size.set(Repository.getFormatter0dec().format(size));

            } else {
                this.decimalFormatBkp.setDecimalFormatSymbols(usSymbols);
                this.price.set(this.decimalFormatBkp.format(price));
                this.size.set(Repository.getFormatter0dec().format(size));
            }

        } catch (Exception e) {
            log.error("price is {} ", price);
            log.error(e.getMessage(), e);
        }
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public DecimalFormatSymbols getUsSymbols() {
        return usSymbols;
    }

    public void setUsSymbols(DecimalFormatSymbols usSymbols) {
        this.usSymbols = usSymbols;
    }

    public DecimalFormat getDecimalFormatBkp() {
        return decimalFormatBkp;
    }

    public void setDecimalFormatBkp(DecimalFormat decimalFormatBkp) {
        this.decimalFormatBkp = decimalFormatBkp;
    }

    public String getPrice() {
        return price.get();
    }

    public StringProperty priceProperty() {
        return price;
    }

    public void setPrice(String price) {
        this.price.set(price);
    }

    public String getSize() {
        return size.get();
    }

    public StringProperty sizeProperty() {
        return size;
    }

    public void setSize(String size) {
        this.size.set(size);
    }

    public DecimalFormat getDecimalFormat() {
        return decimalFormat;
    }

    public void setDecimalFormat(DecimalFormat decimalFormat) {
        this.decimalFormat = decimalFormat;
    }

    public String getOperator() {
        return operator.get();
    }

    public StringProperty operatorProperty() {
        return operator;
    }

    public String getAccount() {
        return account.get();
    }

    public StringProperty accountProperty() {
        return account;
    }

    public String getSymbol() {
        return symbol.get();
    }

    public StringProperty symbolProperty() {
        return symbol;
    }

    public MarketDataMessage.SecurityExchangeMarketData getSecurityExchangeMarketData() {
        return securityExchangeMarketData;
    }

    public void setSecurityExchangeMarketData(MarketDataMessage.SecurityExchangeMarketData securityExchangeMarketData) {
        this.securityExchangeMarketData = securityExchangeMarketData;
    }

    public RoutingMessage.SecurityExchangeRouting getSecurityExchangeRouting() {
        return securityExchangeRouting;
    }

    public void setSecurityExchangeRouting(RoutingMessage.SecurityExchangeRouting securityExchangeRouting) {
        this.securityExchangeRouting = securityExchangeRouting;
    }

    public void setOperator(String operator) {
        this.operator.set(operator);
    }

    public void setAccount(String account) {
        this.account.set(account);
    }

    public void setSymbol(String symbol) {
        this.symbol.set(symbol);
    }
}
