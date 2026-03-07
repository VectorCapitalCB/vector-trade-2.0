package cl.vc.blotter.utils;

public class ColumnConfig {


    private boolean symbol = true;
    private boolean settlTypeCol = true;
    private boolean imbalanceGen = true;
    private boolean market = true;
    private boolean bidQtyGen = true;
    private boolean bidpriceGen = true;
    private boolean offerpriceGen = true;
    private boolean offerQtyGen = true;
    private boolean openpriceGen = true;
    private boolean closepriceGen = true;
    private boolean highpriceGen = true;
    private boolean lowpriceGen = true;
    private boolean amountGen = true;
    private boolean volumeGen = true;
    private boolean vwapGen = true;
    private boolean desbalancetheoric = true;
    private boolean priceTheoric = true;
    private boolean amountTheoric = true;


    public boolean isSymbol() {
        return symbol;
    }

    public void setSymbol(boolean symbol) {
        this.symbol = symbol;
    }

    public boolean isSettlTypeCol() {
        return settlTypeCol;
    }

    public void setSettlTypeCol(boolean settlTypeCol) {
        this.settlTypeCol = settlTypeCol;
    }

    public boolean isImbalanceGen() {
        return imbalanceGen;
    }

    public void setImbalanceGen(boolean imbalanceGen) {
        this.imbalanceGen = imbalanceGen;
    }

    public boolean isMarket() {
        return market;
    }

    public void setMarket(boolean market) {
        this.market = market;
    }

    public boolean isBidQtyGen() {
        return bidQtyGen;
    }

    public void setBidQtyGen(boolean bidQtyGen) {
        this.bidQtyGen = bidQtyGen;
    }

    public boolean isBidpriceGen() {
        return bidpriceGen;
    }

    public void setBidpriceGen(boolean bidpriceGen) {
        this.bidpriceGen = bidpriceGen;
    }

    public boolean isOfferpriceGen() {
        return offerpriceGen;
    }

    public void setOfferpriceGen(boolean offerpriceGen) {
        this.offerpriceGen = offerpriceGen;
    }

    public boolean isOfferQtyGen() {
        return offerQtyGen;
    }

    public void setOfferQtyGen(boolean offerQtyGen) {
        this.offerQtyGen = offerQtyGen;
    }

    public boolean isOpenpriceGen() {
        return openpriceGen;
    }

    public void setOpenpriceGen(boolean openpriceGen) {
        this.openpriceGen = openpriceGen;
    }

    public boolean isClosepriceGen() {
        return closepriceGen;
    }

    public void setClosepriceGen(boolean closepriceGen) {
        this.closepriceGen = closepriceGen;
    }

    public boolean isHighpriceGen() {
        return highpriceGen;
    }

    public void setHighpriceGen(boolean highpriceGen) {
        this.highpriceGen = highpriceGen;
    }

    public boolean isLowpriceGen() {
        return lowpriceGen;
    }

    public void setLowpriceGen(boolean lowpriceGen) {
        this.lowpriceGen = lowpriceGen;
    }

    public boolean isAmountGen() {
        return amountGen;
    }

    public void setAmountGen(boolean amountGen) {
        this.amountGen = amountGen;
    }

    public boolean isVolumeGen() {
        return volumeGen;
    }

    public void setVolumeGen(boolean volumeGen) {
        this.volumeGen = volumeGen;
    }

    public boolean isVwapGen() {
        return vwapGen;
    }

    public void setVwapGen(boolean vwapGen) {
        this.vwapGen = vwapGen;
    }

    public boolean isDesbalancetheoric() {
        return desbalancetheoric;
    }

    public void setDesbalancetheoric(boolean desbalancetheoric) {
        this.desbalancetheoric = desbalancetheoric;
    }

    public boolean isPriceTheoric() {
        return priceTheoric;
    }

    public void setPriceTheoric(boolean priceTheoric) {
        this.priceTheoric = priceTheoric;
    }

    public boolean isAmountTheoric() {
        return amountTheoric;
    }

    public void setAmountTheoric(boolean amountTheoric) {
        this.amountTheoric = amountTheoric;
    }
}