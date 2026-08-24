package cl.vc.service.akka.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.service.MainApp;
import lombok.extern.slf4j.Slf4j;
import java.util.*;
import java.util.stream.Collectors;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ActorTradeGeneralStats extends AbstractActor {

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private final Object statsLock = new Object();
    private final Map<String, TradeAccumulator> tradeStatsByTopic = new HashMap<>();
    private final Map<String, MarketDataMessage.RankinSymbol.Builder> mapRankinSymbol = new HashMap<>();
    private long totalTrades;

    private final long emitEveryMs = 60_000;

    private MarketDataMessage.BolsaStats.Builder bolsaStats;


    public static Props props() {
        return Props.create(ActorTradeGeneralStats.class);
    }


    @Override
    public void preStart() {
        try {

            bolsaStats = MarketDataMessage.BolsaStats.newBuilder().setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.BCS)
                    .setId(IDGenerator.getID());

            MainApp.getMessageEventBus().subscribe(getSelf(), "TradeGeneral");

            scheduler.scheduleAtFixedRate(() -> {
                try {
                    MarketDataMessage.BolsaStats bolsaStatsAux;
                    synchronized (statsLock) {
                        calculateBolsaStats();
                        bolsaStats.setHoraFin(java.time.Instant.now().toString());
                        bolsaStatsAux = bolsaStats.build();
                    }
                    BuySideConnect.getActorPerSessionMaps().values().forEach(s->{
                        s.tell(bolsaStatsAux, ActorRef.noSender());
                    });
                } catch (Exception e) {
                    log.error("Error calculando BolsaStats", e);
                }
            }, emitEveryMs, emitEveryMs, TimeUnit.MILLISECONDS); // Inicia inmediatamente y repite cada 60 segundos


        } catch (Exception e) {
            log.error("No pude suscribirme a TradeGeneral", e);
        }

    }

    @Override
    public void postStop() {
        try {
            MainApp.getMessageEventBus().unsubscribe(getSelf(), "TradeGeneral");
            scheduler.shutdownNow();
        } catch (Exception ignore) {
            log.error("No pude suscribirme a TradeGeneral", ignore);
        }

    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(MarketDataMessage.TradeGeneral.class, this::onTradeGeneral)
                .build();
    }

    private void onTradeGeneral(MarketDataMessage.TradeGeneral t) {
        try {
            String id = TopicGenerator.getTopicMKD(t);
            synchronized (statsLock) {
                TradeAccumulator accumulator = tradeStatsByTopic.computeIfAbsent(id, ignored -> new TradeAccumulator());
                accumulator.add(t);
                totalTrades++;
                mapRankinSymbol.put(id, accumulator.toRankin(id, t).toBuilder());
            }
        } catch (Exception e) {
            log.error("No pude suscribirme a TradeGeneral", e);
        }

    }

    private static double calculateEMA(List<Double> prices, int period) {
        // Verificamos que haya suficientes precios para calcular el EMA
        if (prices.size() < period) {
            // Si no hay suficientes precios, retornamos 0.0 o alguna otra lógica de erro
            return 0.0;
        }

        // Calculamos el EMA comenzando desde el primer precio dentro del período
        double multiplier = 2.0 / (period + 1);
        double ema = prices.get(prices.size() - period);  // El primer valor de EMA es el valor de precios del período

        // Recorremos la lista de precios a partir de la posición "period" para calcular el EMA
        for (int i = prices.size() - period + 1; i < prices.size(); i++) {
            ema = (prices.get(i) - ema) * multiplier + ema;
        }
        return ema;
    }

    private void calculateBolsaStats() {
        // Paso 1: Inicialización de variables
        double totalVolumen = 0.0;
        double totalMonto = 0.0;
        double totalCapitalizacion = 0.0;
        double totalPrecioUltimo = 0.0;
        double totalPrecioMaximo = 0.0;
        double totalVariacionPositiva = 0.0;
        double totalVariacionNegativa = 0.0;
        double totalTendencia = 0.0;
        int totalAssets = mapRankinSymbol.size();



        List<MarketDataMessage.RankinSymbol.Builder> rankinSymbols = new ArrayList<>(mapRankinSymbol.values());
        List<MarketDataMessage.RankinSymbol> masVolatil = new ArrayList<>();
        List<MarketDataMessage.RankinSymbol> masCayo = new ArrayList<>();
        List<MarketDataMessage.RankinSymbol> menosCayo = new ArrayList<>();
        List<MarketDataMessage.RankinSymbol> masTranzado = new ArrayList<>();
        List<MarketDataMessage.RankinSymbol> bestRankin = new ArrayList<>();
        List<MarketDataMessage.RankinSymbol> worseRankin = new ArrayList<>();

        for (MarketDataMessage.RankinSymbol.Builder rankin : rankinSymbols) {
            double volumen = rankin.getVolumen();
            double monto = rankin.getMonto();
            double precioUltimo = rankin.getPrecioUltimo();
            double precioMaximo = rankin.getPrecioMaximo();
            double variacionPct = rankin.getVariacionPct();
            double capitalizacion = precioUltimo * volumen;

            totalVolumen += volumen;
            totalMonto += monto;
            totalCapitalizacion += capitalizacion;
            totalPrecioUltimo += precioUltimo;
            totalPrecioMaximo += precioMaximo;
            totalTendencia += variacionPct;

            if (variacionPct > 0) {
                totalVariacionPositiva++;
                bestRankin.add(rankin.build());
            } else if (variacionPct < 0) {
                totalVariacionNegativa++;
                worseRankin.add(rankin.build());
            } else {
                // Variacion exactamente 0 no es sentimiento negativo. Contarla como negativa hacia
                // que sentimientoPositivo + sentimientoNegativo diera 100,00% exacto y que la
                // Tendencia General saliera "bajista" con la mayoria de los papeles planos.
                worseRankin.add(rankin.build());
            }

            if (variacionPct < 0) {
                masCayo.add(rankin.build());
            } else {
                menosCayo.add(rankin.build());
            }

            // Categorizamos por volatilidad y volumen
            masVolatil.add(rankin.build()); // Aquí puedes usar alguna métrica de volatilidad si lo deseas
            masTranzado.add(rankin.build()); // Aquí puedes usar volumen o alguna otra métrica si lo deseas
        }

        masVolatil = sortByVolatilidad(masVolatil);  // Ordenado por volatilidad
        // "Mas tranzado" en la bolsa chilena es mayor MONTO transado, no mayor numero de acciones.
        // Ordenando por volumen, un papel caro como SQM-B (el #1 del dia por monto, con solo
        // ~184.000 acciones) quedaba sepultado bajo papeles baratos de alto volumen.
        masTranzado = sortByMonto(masTranzado);
        bestRankin = sortByVariacionPct(bestRankin); // Ordenado por variación positiva
        worseRankin = sortByVariacionPctNegativa(worseRankin); // Ordenado por variación negativa
        masCayo = sortByVariacionPctNegativa(masCayo); // Ordenado por caída en el precio
        menosCayo = sortByVariacionPct(menosCayo);   //

        // Paso 2: Calculamos el sentimiento positivo/negativo
        double sentimientoPositivo = (totalAssets > 0) ? (totalVariacionPositiva / totalAssets) * 100 : 0.0;
        double sentimientoNegativo = (totalAssets > 0) ? (totalVariacionNegativa / totalAssets) * 100 : 0.0;

        // Paso 3: Capitalización total y promedio
        double capitalizacionPromedio = (totalAssets > 0) ? (totalCapitalizacion / totalAssets) : 0.0;

        // Paso 4: Precio promedio acumulado y precio máximo acumulado
        double precioPromedioAcumulado = (totalAssets > 0) ? (totalPrecioUltimo / totalAssets) : 0.0;
        double precioMaximoAcumulado = totalPrecioMaximo;

        // Paso 5: Tendencia general
        String tendenciaGeneral = (sentimientoPositivo > 50) ? "alcista" : (sentimientoNegativo > 50) ? "bajista" : "neutral";
        double tendenciaPromedio = (totalAssets > 0) ? (totalTendencia / totalAssets) : 0.0;

        // Paso 6: Rango promedio (diferencia entre max y min precios)
        double sumRango = rankinSymbols.stream()
                .mapToDouble(s -> s.getPrecioMaximo() - s.getPrecioMinimo())
                .sum();
        // Sin dividir por cero: el scheduler emite cada 60 s desde el arranque, tambien antes del
        // primer trade, y 0/0 = NaN se propagaba al proto y el front pintaba "NaN" en el header.
        double rangoPromedio = rankinSymbols.isEmpty() ? 0.0 : sumRango / rankinSymbols.size();

        // Paso 7: Volatilidad promedio (utilizando desviación estándar de los log-returns)
        double sumVolatilityWeighted = 0.0;
        double sumVolWeights = 0.0;

        for (MarketDataMessage.RankinSymbol.Builder rankin : rankinSymbols) {
            String id = rankin.getId();
            TradeAccumulator accumulator = tradeStatsByTopic.get(id);
            double volRealizada = accumulator != null ? accumulator.realizedVolatility() : 0.0;

            if (!Double.isNaN(volRealizada) && !Double.isInfinite(volRealizada)) {
                double weight = Math.max(1.0, rankin.getVolumen()); // pondero por volumen (mínimo 1)
                sumVolatilityWeighted += volRealizada * weight;
                sumVolWeights += weight;
            }
        }
        double volatilidadPromedio = (sumVolWeights > 0) ? (sumVolatilityWeighted / sumVolWeights) : 0.0;

        // Paso 8: Índice promedio, máximo y mínimo
        double sumPxUltVol = rankinSymbols.stream().mapToDouble(s -> s.getPrecioUltimo() * s.getVolumen()).sum();
        double sumPxMaxVol = rankinSymbols.stream().mapToDouble(s -> s.getPrecioMaximo() * s.getVolumen()).sum();
        double sumPxMinVol = rankinSymbols.stream().mapToDouble(s -> s.getPrecioMinimo() * s.getVolumen()).sum();

        double indicePromedio = (totalVolumen > 0) ? (sumPxUltVol / totalVolumen) : 0.0;
        double indiceMaximo   = (totalVolumen > 0) ? (sumPxMaxVol / totalVolumen) : 0.0;
        double indiceMinimo   = (totalVolumen > 0) ? (sumPxMinVol / totalVolumen) : 0.0;

        // Paso 9: Liquidez media
        double liquidezMedia  = (totalMonto > 0) ? (totalVolumen / totalMonto) : 0.0;

        // Paso 10: Número total de transacciones
        long numeroTotalTrades = totalTrades;

        bolsaStats
                .clearMasVolatil()
                .clearMasCayo()
                .clearMenosCayo()
                .clearMasTranzado()
                .clearBestRankin()
                .clearWorseRankin();

        // Paso 11: Setear en bolsaStats
        bolsaStats
                .setTotalVolumen(totalVolumen)
                .setMontoTotal(totalMonto)
                .setVolatilidadPromedio(volatilidadPromedio)
                .setRangoPromedio(rangoPromedio)
                .setIndicePromedio(indicePromedio)
                .setIndiceMaximo(indiceMaximo)
                .setIndiceMinimo(indiceMinimo)
                .setLiquidezMedia(liquidezMedia)
                .setNumeroTotalTrades(numeroTotalTrades)
                .setSentimientoPositivo(sentimientoPositivo)
                .setSentimientoNegativo(sentimientoNegativo)
                .setCapitalizacionTotal(totalCapitalizacion)
                .setCapitalizacionPromedio(capitalizacionPromedio)
                .setPrecioPromedioAcumulado(precioPromedioAcumulado)
                .setPrecioMaximoAcumulado(precioMaximoAcumulado)
                .setTendenciaGeneral(tendenciaGeneral)
                .setTendenciaPromedio(tendenciaPromedio)
                .addAllMasVolatil(masVolatil)
                .addAllMasCayo(masCayo)
                .addAllMenosCayo(menosCayo)
                .addAllMasTranzado(masTranzado)
                .addAllBestRankin(bestRankin)
                .addAllWorseRankin(worseRankin);
    }

    static final class TradeAccumulator {
        private static final int MAX_RECENT_PRICES = 26;

        private final ArrayDeque<Double> recentPrices = new ArrayDeque<>(MAX_RECENT_PRICES);
        private long count;
        private double firstPrice;
        private double lastPrice;
        private double maxPrice;
        private double minPrice;
        private double sumPrice;
        private double sumQty;
        private double sumAmount;
        private double sumPriceQty;
        private double gainSum;
        private double lossSum;
        private double logReturnSum;
        private double logReturnSquaredSum;
        private long logReturnCount;

        void add(MarketDataMessage.TradeGeneral trade) {
            double price = trade.getPrice();
            if (count == 0) {
                firstPrice = price;
                maxPrice = price;
                minPrice = price;
            } else {
                double difference = price - lastPrice;
                if (difference > 0) {
                    gainSum += difference;
                } else {
                    lossSum += -difference;
                }
                if (lastPrice > 0 && price > 0) {
                    double logReturn = Math.log(price / lastPrice);
                    logReturnSum += logReturn;
                    logReturnSquaredSum += logReturn * logReturn;
                    logReturnCount++;
                }
                maxPrice = Math.max(maxPrice, price);
                minPrice = Math.min(minPrice, price);
            }

            lastPrice = price;
            sumPrice += price;
            sumQty += trade.getQty();
            sumAmount += trade.getAmount();
            sumPriceQty += price * trade.getQty();
            count++;

            recentPrices.addLast(price);
            if (recentPrices.size() > MAX_RECENT_PRICES) {
                recentPrices.removeFirst();
            }
        }

        MarketDataMessage.RankinSymbol toRankin(String id, MarketDataMessage.TradeGeneral lastTrade) {
            List<Double> prices = new ArrayList<>(recentPrices);
            double averagePrice = count > 0 ? sumPrice / count : 0.0;
            double vwap = sumQty != 0 ? sumPriceQty / sumQty : 0.0;
            double variationPct = firstPrice != 0 ? ((lastPrice - firstPrice) / firstPrice) * 100.0 : 0.0;
            double rsi = gainSum == 0 && lossSum == 0
                    ? 50.0
                    : 100.0 - (100.0 / (1.0 + (gainSum / lossSum)));
            double movingAverage = prices.stream()
                    .skip(Math.max(0, prices.size() - 10L))
                    .mapToDouble(Double::doubleValue)
                    .average()
                    .orElse(0.0);
            double macd = calculateEMA(prices, 12) - calculateEMA(prices, 26);
            double liquidRatio = sumAmount != 0 ? sumQty / sumAmount : 0.0;

            return MarketDataMessage.RankinSymbol.newBuilder()
                    .setId(id)
                    .setSecurityExchange(lastTrade.getSecurityExchange())
                    .setSymbol(lastTrade.getSymbol())
                    .setSettlType(lastTrade.getSettlType())
                    .setSecurityType(lastTrade.getSecurityType())
                    .setPrecioUltimo(lastPrice)
                    .setPrecioMaximo(maxPrice)
                    .setPrecioMinimo(minPrice)
                    .setPrecioPromedio(averagePrice)
                    .setVwap(vwap)
                    .setVolumen(sumQty)
                    .setMonto(sumAmount)
                    .setVariacionPct(variationPct)
                    .setRsi(rsi)
                    .setMa(movingAverage)
                    .setMacd(macd)
                    .setLiquidRatio(liquidRatio)
                    .setImpliedVolatility(Math.abs(variationPct))
                    .build();
        }

        double realizedVolatility() {
            if (logReturnCount <= 1) {
                return 0.0;
            }
            double mean = logReturnSum / logReturnCount;
            double variance = (logReturnSquaredSum / logReturnCount) - (mean * mean);
            return Math.sqrt(Math.max(variance, 0.0));
        }

        long count() {
            return count;
        }

        int retainedPriceCount() {
            return recentPrices.size();
        }
    }

    private List<MarketDataMessage.RankinSymbol> sortByVolatilidad(List<MarketDataMessage.RankinSymbol> src) {
        return src.stream()
                .sorted(Comparator.comparingDouble(MarketDataMessage.RankinSymbol::getImpliedVolatility).reversed()) // De mayor a menor volatilidad
                .collect(Collectors.toList());
    }

    private List<MarketDataMessage.RankinSymbol> sortByMonto(List<MarketDataMessage.RankinSymbol> src) {
        return src.stream()
                .sorted(Comparator.comparingDouble(MarketDataMessage.RankinSymbol::getMonto).reversed()) // De mayor a menor monto transado
                .collect(Collectors.toList());
    }

    private List<MarketDataMessage.RankinSymbol> sortByVariacionPct(List<MarketDataMessage.RankinSymbol> src) {
        return src.stream()
                .sorted(Comparator.comparingDouble(MarketDataMessage.RankinSymbol::getVariacionPct).reversed()) // De mayor a menor variación (positiva)
                .collect(Collectors.toList());
    }

    private List<MarketDataMessage.RankinSymbol> sortByVariacionPctNegativa(List<MarketDataMessage.RankinSymbol> src) {
        return src.stream()
                .sorted(Comparator.comparingDouble(MarketDataMessage.RankinSymbol::getVariacionPct)) // De menor a mayor variación (negativa)
                .collect(Collectors.toList());
    }

}
