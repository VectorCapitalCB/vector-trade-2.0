package cl.vc.blotter.model;

import cl.vc.blotter.Repository;
import cl.vc.module.protocolbuff.generator.NumberGenerator;
import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.ticks.Ticks;
import javafx.application.Platform;
import javafx.beans.property.DoubleProperty;
import javafx.beans.property.SimpleDoubleProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.text.DecimalFormat;

@Data
@Slf4j
public class StatisticVO {

    private MarketDataMessage.Statistic statistic;
    private BigDecimal tick = BigDecimal.ZERO;
    private DecimalFormat decimalFormat;
    private DecimalFormat decimalFormatBkp = new DecimalFormat("#,##0.0000");
    private DecimalFormat decimalFormatPor = new DecimalFormat("#,##0.00");
    private StringProperty settlType = new SimpleStringProperty();
    private StringProperty securityType = new SimpleStringProperty();
    private StringProperty securityExchange = new SimpleStringProperty();
    private SimpleDoubleProperty imbalance = new SimpleDoubleProperty();
    private SimpleDoubleProperty bidQty = new SimpleDoubleProperty();
    private StringProperty bidPx = new SimpleStringProperty();
    private StringProperty askPx = new SimpleStringProperty();
    private SimpleDoubleProperty askQty = new SimpleDoubleProperty();
    private SimpleDoubleProperty open = new SimpleDoubleProperty();
    private SimpleDoubleProperty close = new SimpleDoubleProperty();
    private SimpleDoubleProperty high = new SimpleDoubleProperty();
    private SimpleDoubleProperty low = new SimpleDoubleProperty();
    private SimpleDoubleProperty amount = new SimpleDoubleProperty();
    private SimpleDoubleProperty volume = new SimpleDoubleProperty();
    private SimpleDoubleProperty vwap = new SimpleDoubleProperty();

    private StringProperty tradeVolume = new SimpleStringProperty();
    private StringProperty delta = new SimpleStringProperty();
    private StringProperty previusClose = new SimpleStringProperty();
    private StringProperty referencialPrice = new SimpleStringProperty();
    private StringProperty indicativeOpening = new SimpleStringProperty();
    private StringProperty last = new SimpleStringProperty();
    private StringProperty tickDirecion = new SimpleStringProperty();
    private StringProperty amountTheoric = new SimpleStringProperty();
    private StringProperty priceTheoric = new SimpleStringProperty();
    private StringProperty desbalTheoric = new SimpleStringProperty();
    private StringProperty ownDemand = new SimpleStringProperty();
    private StringProperty totalDeamnd = new SimpleStringProperty();



    private StringProperty mid = new SimpleStringProperty();
    private StringProperty symbol = new SimpleStringProperty();
    private StringProperty ratio = new SimpleStringProperty();
    private String id;

    // ==================== Helpers ====================

    /** Ejecuta r en el JavaFX Application Thread. */
    private void runOnFx(Runnable r) {
        if (Platform.isFxApplicationThread()) r.run();
        else Platform.runLater(r);
    }

    /** Clona un DecimalFormat para evitar compartir estado interno (fastPathData) entre hilos. */
    private DecimalFormat safeClone(DecimalFormat base) {
        DecimalFormat f = (DecimalFormat) base.clone();
        // "Calienta" el fast-path en este hilo para evitar lazy-init concurrente
        f.format(0d);
        return f;
    }


    public StatisticVO(MarketDataMessage.Statistic statistic) {
        try {
            id = TopicGenerator.getTopicMKD(statistic);

            runOnFx(() -> {
                this.settlType.set(statistic.getSettlType().name());
                this.securityExchange.set(statistic.getSecurityExchange().name());
                this.securityType.set(statistic.getSecurityType().name());
                this.symbol.set(statistic.getSymbol());
            });

            if (decimalFormat == null) {
                try {
                    if (statistic.getBidPx() > 0d) {
                        tick = Ticks.conversorExdestination(statistic.getSecurityExchange(), BigDecimal.valueOf(statistic.getBidPx()));
                        decimalFormat = NumberGenerator.formetByticks(tick);
                    } else if (statistic.getAskPx() > 0d) {
                        tick = Ticks.conversorExdestination(statistic.getSecurityExchange(), BigDecimal.valueOf(statistic.getAskPx()));
                        decimalFormat = NumberGenerator.formetByticks(tick);
                    } else if (statistic.getPreviusClose() > 0d) {
                        tick = Ticks.conversorExdestination(statistic.getSecurityExchange(), BigDecimal.valueOf(statistic.getPreviusClose()));
                        decimalFormat = NumberGenerator.formetByticks(tick);
                    } else if (statistic.getOhlcv().getClose() > 0d) {
                        tick = Ticks.conversorExdestination(statistic.getSecurityExchange(), BigDecimal.valueOf(statistic.getOhlcv().getClose()));
                        decimalFormat = NumberGenerator.formetByticks(tick);
                    }
                } catch (Exception ignore) {
                    // Si Ohlcv viniera nulo o incompleto, simplemente no seteamos tick aquí
                }
            }

            update(statistic);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public StatisticVO(MarketDataMessage.IncrementalBook.Builder incremental) {
        try {
            id = TopicGenerator.getTopicMKD(incremental.build());

            runOnFx(() -> {
                this.settlType.set(incremental.getSettlType().name());
                this.securityExchange.set(incremental.getSecurityExchange().name());
                this.securityType.set(incremental.getSecurityType().name());
                this.symbol.set(incremental.getSymbol());
            });

            if (!incremental.getBidsList().isEmpty()) {
                double px = incremental.getBidsList().get(0).getPrice();
                double size = incremental.getBidsList().get(0).getSize();

                // FIX: usar incremental.getSecurityExchange(), no "statistic"
                tick = Ticks.conversorExdestination(incremental.getSecurityExchange(), BigDecimal.valueOf(px));
                this.decimalFormat = NumberGenerator.formetByticks(tick);

                final DecimalFormat fmt = safeClone(this.decimalFormat);
                runOnFx(() -> {
                    this.bidPx.set(fmt.format(px));
                    this.bidQty.set(statistic.getBidQty());
                });
            }

            if (!incremental.getAsksList().isEmpty()) {
                double px = incremental.getAsksList().get(0).getPrice();
                double size = incremental.getAsksList().get(0).getSize();

                tick = Ticks.conversorExdestination(incremental.getSecurityExchange(), BigDecimal.valueOf(px));
                this.decimalFormat = NumberGenerator.formetByticks(tick);

                final DecimalFormat fmt = safeClone(this.decimalFormat);
                runOnFx(() -> {
                    this.askPx.set(fmt.format(px));
                    this.askQty.set(statistic.getAskQty());


                });
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public StatisticVO(MarketDataMessage.Subscribe subscribe) {
        try {
            id = TopicGenerator.getTopicMKD(subscribe);

            runOnFx(() -> {
                this.settlType.set(subscribe.getSettlType().name());
                this.securityExchange.set(subscribe.getSecurityExchange().name());
                this.securityType.set(subscribe.getSecurityType().name());
                this.symbol.set(subscribe.getSymbol());
            });

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    // ==================== Actualizaciones ====================

    public void update(MarketDataMessage.Statistic statistic) {

        this.statistic = statistic;

        try {
            if (decimalFormat == null) {
                try {
                    if (statistic.getBidPx() > 0d) {
                        tick = Ticks.conversorExdestination(statistic.getSecurityExchange(), BigDecimal.valueOf(statistic.getBidPx()));
                        decimalFormat = NumberGenerator.formetByticks(tick);
                    } else if (statistic.getAskPx() > 0d) {
                        tick = Ticks.conversorExdestination(statistic.getSecurityExchange(), BigDecimal.valueOf(statistic.getAskPx()));
                        decimalFormat = NumberGenerator.formetByticks(tick);
                    } else if (statistic.getPreviusClose() > 0d) {
                        tick = Ticks.conversorExdestination(statistic.getSecurityExchange(), BigDecimal.valueOf(statistic.getPreviusClose()));
                        decimalFormat = NumberGenerator.formetByticks(tick);
                    } else if (statistic.getOhlcv().getClose() > 0d) {
                        tick = Ticks.conversorExdestination(statistic.getSecurityExchange(), BigDecimal.valueOf(statistic.getOhlcv().getClose()));
                        decimalFormat = NumberGenerator.formetByticks(tick);
                    }
                } catch (Exception ignore) {
                    // Ohlcv puede venir nulo/incompleto; si no logramos determinar tick, se usará el BKP
                }
            }

            final DecimalFormat baseFmt = (decimalFormat != null ? decimalFormat : decimalFormatBkp);

            // Todas las mutaciones de propiedades deben correr en el hilo FX.
            runOnFx(() -> updateStatistic(baseFmt, statistic));

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void updateStatistic(DecimalFormat decimalFormat, MarketDataMessage.Statistic statistic) {

        try {
            // Clones locales y "calentamiento" para evitar fastPathData nulo
            final DecimalFormat fmt = safeClone(decimalFormat != null ? decimalFormat : decimalFormatBkp);
            final DecimalFormat fmtPor = safeClone(decimalFormatPor);

            this.amount.set(statistic.getAmount());
            this.vwap.set(statistic.getVwap());
            this.imbalance.set(statistic.getImbalance());

            if (statistic.getTradeVolume() > 0d) {
                this.tradeVolume.set(fmt.format(statistic.getTradeVolume()));
            } else {
                this.tradeVolume.set("0");
            }

            this.delta.set(fmt.format(statistic.getDelta()));
            this.previusClose.set(fmt.format(statistic.getPreviusClose()));
            this.referencialPrice.set(fmt.format(statistic.getReferencialPrice()));
            this.indicativeOpening.set(fmt.format(statistic.getIndicativeOpening()));
            this.last.set(fmt.format(statistic.getLast()));
            this.tickDirecion.set(fmt.format(statistic.getTickDirecion()));

            this.bidPx.set(fmt.format(statistic.getBidPx()));
            this.bidQty.set(statistic.getBidQty());

            this.askPx.set(fmt.format(statistic.getAskPx()));
            this.askQty.set(statistic.getAskQty());

            if (statistic.getBidPx() > 0d && statistic.getAskPx() > 0d) {
                mid.set(fmt.format((statistic.getBidPx() + statistic.getAskPx()) / 2d));
            } else {
                mid.set("0");
            }

            this.amountTheoric.set(fmt.format(statistic.getAmountTheoric()));
            this.priceTheoric.set(fmt.format(statistic.getPriceTheoric()));
            this.desbalTheoric.set(fmt.format(statistic.getDesbalTheoric()));
            this.totalDeamnd.set(fmt.format(statistic.getTotalDeamnd()));
            this.ownDemand.set(fmt.format(statistic.getOwnDemand()));

            try {
                if (statistic.getOhlcv().getClose() > 0d) {
                    this.close.set(statistic.getOhlcv().getClose());
                } else {
                    this.close.set(0d);
                }
                this.open.set(statistic.getOhlcv().getOpen());
                this.low.set((statistic.getOhlcv().getLow()));
                this.high.set(statistic.getOhlcv().getHigh());
            } catch (Exception ignore) {
                this.close.set(0d);
                this.open.set(0d);
                this.low.set(0d);
                this.high.set(0d);
            }

            this.volume.set(statistic.getTradeVolume());

            this.ratio.set(statistic.getRatio());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    // ==================== Getters/Setters generados ====================

    public String getSettlType() {
        return settlType.get();
    }

    public void setSettlType(String settlType) {
        this.settlType.set(settlType);
    }

    public StringProperty settlTypeProperty() {
        return settlType;
    }

    public String getSecurityType() {
        return securityType.get();
    }

    public void setSecurityType(String securityType) {
        this.securityType.set(securityType);
    }

    public StringProperty securityTypeProperty() {
        return securityType;
    }

    public String getSecurityExchange() {
        return securityExchange.get();
    }

    public void setSecurityExchange(String securityExchange) {
        this.securityExchange.set(securityExchange);
    }

    public StringProperty securityExchangeProperty() {
        return securityExchange;
    }

    public String getSymbol() {
        return symbol.get();
    }

    public void setSymbol(String symbol) {
        this.symbol.set(symbol);
    }

    public StringProperty symbolProperty() {
        return symbol;
    }

    public String getRatio() {
        return ratio.get();
    }

    public void setRatio(String ratio) {
        this.ratio.set(ratio);
    }

    public StringProperty ratioProperty() {
        return ratio;
    }

    public MarketDataMessage.Statistic getStatistic() {
        return statistic;
    }

    public void setStatistic(MarketDataMessage.Statistic statistic) {
        this.statistic = statistic;
    }

    public BigDecimal getTick() {
        return tick;
    }

    public void setTick(BigDecimal tick) {
        this.tick = tick;
    }

    public DecimalFormat getDecimalFormat() {
        return decimalFormat;
    }

    public void setDecimalFormat(DecimalFormat decimalFormat) {
        this.decimalFormat = decimalFormat;
    }

    public DecimalFormat getDecimalFormatBkp() {
        return decimalFormatBkp;
    }

    public void setDecimalFormatBkp(DecimalFormat decimalFormatBkp) {
        this.decimalFormatBkp = decimalFormatBkp;
    }

    public Double getAmount() {
        return amount.get();
    }

    public void setAmount(Double amount) {
        this.amount.set(amount);
    }

    public DoubleProperty amountProperty() {
        return amount;
    }

    public Double getVwap() {
        return vwap.get();
    }

    public void setVwap(Double vwap) {
        this.vwap.set(vwap);
    }

    public DoubleProperty vwapProperty() {
        return vwap;
    }

    public Double getImbalance() {
        return imbalance.get();
    }

    public void setImbalance(Double imbalance) {
        this.imbalance.set(imbalance);
    }

    public DoubleProperty imbalanceProperty() {
        return imbalance;
    }

    public String getTradeVolume() {
        return tradeVolume.get();
    }

    public void setTradeVolume(String tradeVolume) {
        this.tradeVolume.set(tradeVolume);
    }

    public StringProperty tradeVolumeProperty() {
        return tradeVolume;
    }

    public String getDelta() {
        return delta.get();
    }

    public void setDelta(String delta) {
        this.delta.set(delta);
    }

    public StringProperty deltaProperty() {
        return delta;
    }

    public String getPreviusClose() {
        return previusClose.get();
    }

    public void setPreviusClose(String previusClose) {
        this.previusClose.set(previusClose);
    }

    public StringProperty previusCloseProperty() {
        return previusClose;
    }

    public String getReferencialPrice() {
        return referencialPrice.get();
    }

    public void setReferencialPrice(String referencialPrice) {
        this.referencialPrice.set(referencialPrice);
    }

    public StringProperty referencialPriceProperty() {
        return referencialPrice;
    }

    public String getIndicativeOpening() {
        return indicativeOpening.get();
    }

    public void setIndicativeOpening(String indicativeOpening) {
        this.indicativeOpening.set(indicativeOpening);
    }

    public StringProperty indicativeOpeningProperty() {
        return indicativeOpening;
    }

    public String getLast() {
        return last.get();
    }

    public void setLast(String last) {
        this.last.set(last);
    }

    public StringProperty lastProperty() {
        return last;
    }

    public String getTickDirecion() {
        return tickDirecion.get();
    }

    public void setTickDirecion(String tickDirecion) {
        this.tickDirecion.set(tickDirecion);
    }

    public StringProperty tickDirecionProperty() {
        return tickDirecion;
    }

    public String getBidPx() {
        return bidPx.get();
    }

    public void setBidPx(String bidPx) {
        this.bidPx.set(bidPx);
    }

    public StringProperty bidPxProperty() {
        return bidPx;
    }

    public Double getBidQty() {
        return bidQty.get();
    }

    public void setBidQty(Double bidQty) {
        this.bidQty.set(bidQty);
    }

    public DoubleProperty bidQtyProperty() {
        return bidQty;
    }

    public String getAskPx() {
        return askPx.get();
    }

    public void setAskPx(String askPx) {
        this.askPx.set(askPx);
    }

    public StringProperty askPxProperty() {
        return askPx;
    }

    public Double getAskQty() {
        return askQty.get();
    }

    public void setAskQty(Double askQty) {
        this.askQty.set(askQty);
    }

    public DoubleProperty askQtyProperty() {
        return askQty;
    }

    public String getAmountTheoric() {
        return amountTheoric.get();
    }

    public void setAmountTheoric(String amountTheoric) {
        this.amountTheoric.set(amountTheoric);
    }

    public StringProperty amountTheoricProperty() {
        return amountTheoric;
    }

    public String getPriceTheoric() {
        return priceTheoric.get();
    }

    public void setPriceTheoric(String priceTheoric) {
        this.priceTheoric.set(priceTheoric);
    }

    public StringProperty priceTheoricProperty() {
        return priceTheoric;
    }

    public String getDesbalTheoric() {
        return desbalTheoric.get();
    }

    public void setDesbalTheoric(String desbalTheoric) {
        this.desbalTheoric.set(desbalTheoric);
    }

    public StringProperty desbalTheoricProperty() {
        return desbalTheoric;
    }

    public String getOwnDemand() {
        return ownDemand.get();
    }

    public void setOwnDemand(String ownDemand) {
        this.ownDemand.set(ownDemand);
    }

    public StringProperty ownDemandProperty() {
        return ownDemand;
    }

    public String getTotalDeamnd() {
        return totalDeamnd.get();
    }

    public void setTotalDeamnd(String totalDeamnd) {
        this.totalDeamnd.set(totalDeamnd);
    }

    public StringProperty totalDeamndProperty() {
        return totalDeamnd;
    }

    public Double getClose() {
        return close.get();
    }

    public void setClose(Double close) {
        this.close.set(close);
    }

    public DoubleProperty closeProperty() {
        return close;
    }

    public Double getOpen() {
        return open.get();
    }

    public void setOpen(Double open) {
        this.open.set(open);
    }

    public DoubleProperty openProperty() {
        return open;
    }

    public Double getLow() {
        return low.get();
    }

    public void setLow(Double low) {
        this.low.set(low);
    }

    public DoubleProperty lowProperty() {
        return low;
    }

    public Double getHigh() {
        return high.get();
    }

    public void setHigh(Double high) {
        this.high.set(high);
    }

    public DoubleProperty highProperty() {
        return high;
    }

    public Double getVolume() {
        return volume.get();
    }

    public void setVolume(Double volume) {
        this.volume.set(volume);
    }

    public DoubleProperty volumeProperty() {
        return volume;
    }

    public String getMid() {
        return mid.get();
    }

    public DecimalFormat getDecimalFormatPor() {
        return decimalFormatPor;
    }

    public void setDecimalFormatPor(DecimalFormat decimalFormatPor) {
        this.decimalFormatPor = decimalFormatPor;
    }

    public void setMid(String mid) {
        this.mid.set(mid);
    }

    public StringProperty midProperty() {
        return mid;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
