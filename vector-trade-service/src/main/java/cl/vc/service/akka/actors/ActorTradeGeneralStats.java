package cl.vc.service.akka.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.service.MainApp;
import com.google.protobuf.util.JsonFormat;
import lombok.extern.slf4j.Slf4j;
import java.util.*;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ActorTradeGeneralStats extends AbstractActor {

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private final Map<String, List<MarketDataMessage.TradeGeneral>> mapListAllTradeGenerales = new HashMap<>();

    private final Map<String, MarketDataMessage.RankinSymbol.Builder> mapRankinSymbol = new ConcurrentHashMap<>();

    private  JsonFormat.Printer printer;

    private final long emitEveryMs = 60_000;

    private MarketDataMessage.BolsaStats.Builder bolsaStats;


    public static Props props() {
        return Props.create(ActorTradeGeneralStats.class);
    }


    @Override
    public void preStart() {
        try {

            this.printer = JsonFormat.printer().includingDefaultValueFields().omittingInsignificantWhitespace();

            bolsaStats = MarketDataMessage.BolsaStats.newBuilder().setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.BCS)
                    .setId(IDGenerator.getID());

            MainApp.getMessageEventBus().subscribe(getSelf(), "TradeGeneral");

            scheduler.scheduleAtFixedRate(() -> {
                try {

                    calculateBolsaStats();

                    MarketDataMessage.BolsaStats bolsaStatsAux = bolsaStats.build();



                    BuySideConnect.getActorPerSessionMaps().values().forEach(s->{
                        s.tell(bolsaStatsAux, ActorRef.noSender());
                    });

                  //  System.out.println(printer.print(bolsaStatsAux));

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


            if (mapListAllTradeGenerales.containsKey(id)) {

                List<MarketDataMessage.TradeGeneral> list = mapListAllTradeGenerales.get(id);
                list.add(t);
                mapListAllTradeGenerales.put(id, list);

            } else {

                List<MarketDataMessage.TradeGeneral> list = new ArrayList<>();
                list.add(t);
                mapListAllTradeGenerales.put(id, list);

            }

            if (!mapRankinSymbol.containsKey(id)) {
                mapRankinSymbol.put(id, MarketDataMessage.RankinSymbol.newBuilder());

            }

            calculateRankinSymbol(id, mapListAllTradeGenerales.get(id));




        } catch (Exception e) {
            log.error("No pude suscribirme a TradeGeneral", e);
        }

    }

    private void calculateRankinSymbol(String id, List<MarketDataMessage.TradeGeneral> trades) {
        if (trades == null || trades.isEmpty()) return;

        double precioMax = Double.NEGATIVE_INFINITY;
        double precioMin = Double.POSITIVE_INFINITY;
        double sumPrecio = 0.0;
        double sumQty = 0.0;
        double sumAmount = 0.0;
        double sumPrecioQty = 0.0;
        double sumPrecioDiferencia = 0.0; // Para RSI
        double lastPrice = trades.getLast().getPrice();
        double firstPrice = trades.getFirst().getPrice();

        // Variables para RSI, MA, MACD
        List<Double> precios = new ArrayList<>();
        List<Double> ganancias = new ArrayList<>();
        List<Double> perdidas = new ArrayList<>();


        for (MarketDataMessage.TradeGeneral t : trades) {
            double price = t.getPrice();
            double qty = t.getQty();
            double amount = t.getAmount();

            precioMax = Math.max(precioMax, price);
            precioMin = Math.min(precioMin, price);
            sumPrecio += price;
            sumQty += qty;
            sumAmount += amount;
            sumPrecioQty += price * qty;

            // Acumulamos precios para RSI y MA
            precios.add(price);
            if (precios.size() > 1) {
                double prevPrice = precios.get(precios.size() - 2);
                double diff = price - prevPrice;
                if (diff > 0) {
                    ganancias.add(diff);
                    perdidas.add(0.0);
                } else {
                    ganancias.add(0.0);
                    perdidas.add(-diff);
                }
            }


        }

        double precioPromedio = sumPrecio / trades.size();
        double vwap = (sumQty != 0) ? (sumPrecioQty / sumQty) : 0.0;
        double variacionPct = ((lastPrice - firstPrice) / firstPrice) * 100.0;

        // Cálculo de RSI (simplificado)
        double promedioGanancias = ganancias.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double promedioPerdidas = perdidas.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double rsi = (promedioGanancias == 0 && promedioPerdidas == 0) ? 50.0 : 100 - (100 / (1 + (promedioGanancias / promedioPerdidas)));

        // Cálculo de MA (Media móvil simple de los últimos 10 precios)
        double ma = precios.stream().skip(Math.max(0, precios.size() - 10)).mapToDouble(Double::doubleValue).average().orElse(0.0);

        // Cálculo de MACD (simplificado como diferencia de medias móviles)
        double ema12 = calculateEMA(precios, 12);
        double ema26 = calculateEMA(precios, 26);
        double macd = ema12 - ema26;

        // Cálculo de Liquidez (Volumen / Monto)
        double liquidRatio = (sumAmount != 0) ? (sumQty / sumAmount) : 0.0;

        // Crear RankinSymbol
        MarketDataMessage.TradeGeneral lastTrade = trades.getLast();
        MarketDataMessage.RankinSymbol.Builder rankin = mapRankinSymbol.get(id);
        rankin
                .setId(id)
                .setSecurityExchange(lastTrade.getSecurityExchange())
                .setSymbol(lastTrade.getSymbol())
                .setSettlType(lastTrade.getSettlType())
                .setSecurityType(lastTrade.getSecurityType())
                .setPrecioUltimo(lastPrice)
                .setPrecioMaximo(precioMax)
                .setPrecioMinimo(precioMin)
                .setPrecioPromedio(precioPromedio)
                .setVwap(vwap)
                .setVolumen(sumQty)
                .setMonto(sumAmount)
                .setVariacionPct(variacionPct)
                .setRsi(rsi)
                .setMa(ma)
                .setMacd(macd)
                .setLiquidRatio(liquidRatio)
                .setImpliedVolatility(0.0); // No calculado

    }

    private double calculateEMA(List<Double> prices, int period) {
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
            } else {
                totalVariacionNegativa++;
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
        masTranzado = sortByVolumen(masTranzado);   // Ordenado por volumen
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
        double rangoPromedio = sumRango / rankinSymbols.size();

        // Paso 7: Volatilidad promedio (utilizando desviación estándar de los log-returns)
        double sumVolatilityWeighted = 0.0;
        double sumVolWeights = 0.0;

        for (MarketDataMessage.RankinSymbol.Builder rankin : rankinSymbols) {
            String id = rankin.getId();
            List<MarketDataMessage.TradeGeneral> trades = mapListAllTradeGenerales.getOrDefault(id, Collections.emptyList());
            double volRealizada = realizedVolatilityFromTrades(trades); // desvío estándar de log-returns

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
        long numeroTotalTrades = mapListAllTradeGenerales.values().stream()
                .mapToLong(List::size).sum();

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

    private double realizedVolatilityFromTrades(List<MarketDataMessage.TradeGeneral> trades) {
        if (trades == null || trades.size() < 2) {
            // Si no hay suficientes datos (menos de 2 precios), no podemos calcular la volatilidad
            return 0.0;
        }

        double sum = 0.0, sumSq = 0.0;
        int n = 0;

        // Calculamos la volatilidad realizada con los log-returns
        double prevPrice = trades.get(0).getPrice(); // Primer precio
        for (int i = 1; i < trades.size(); i++) {
            double currentPrice = trades.get(i).getPrice();
            if (prevPrice > 0 && currentPrice > 0) {
                // Calculamos el log-return: ln(P_t / P_{t-1})
                double logReturn = Math.log(currentPrice / prevPrice);
                sum += logReturn;
                sumSq += logReturn * logReturn;
                n++;
            }
            prevPrice = currentPrice; // Actualizamos el precio anterior para el siguiente cálculo
        }

        if (n <= 1) {
            // Si no tenemos suficientes log-returns, retornamos 0
            return 0.0;
        }

        double mean = sum / n; // Promedio de los log-returns
        double variance = (sumSq / n) - (mean * mean); // Varianza de los log-returns
        return Math.sqrt(Math.max(variance, 0.0)); // Volatilidad (raíz cuadrada de la varianza)
    }

    private List<MarketDataMessage.RankinSymbol> sortByVolatilidad(List<MarketDataMessage.RankinSymbol> src) {
        return src.stream()
                .sorted(Comparator.comparingDouble(MarketDataMessage.RankinSymbol::getImpliedVolatility).reversed()) // De mayor a menor volatilidad
                .collect(Collectors.toList());
    }

    private List<MarketDataMessage.RankinSymbol> sortByVolumen(List<MarketDataMessage.RankinSymbol> src) {
        return src.stream()
                .sorted(Comparator.comparingDouble(MarketDataMessage.RankinSymbol::getVolumen).reversed()) // De mayor a menor volumen
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
