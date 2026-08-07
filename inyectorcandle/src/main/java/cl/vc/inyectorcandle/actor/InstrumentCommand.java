package cl.vc.inyectorcandle.actor;

import cl.vc.inyectorcandle.model.InstrumentStats;
import cl.vc.inyectorcandle.model.MarketDataEvent;
import cl.vc.inyectorcandle.model.TradeEvent;

public sealed interface InstrumentCommand permits InstrumentCommand.OnMarketData, InstrumentCommand.OnTrade,
        InstrumentCommand.Flush, InstrumentCommand.Stop {

    record OnMarketData(MarketDataEvent event) implements InstrumentCommand {
    }

    record OnTrade(TradeEvent event) implements InstrumentCommand {
    }

    /**
     * El throttle de stats y de vela abierta solo se libera con el tick siguiente: un instrumento
     * que opera en rafaga y despues queda quieto deja persistido el estado del primer trade de la
     * rafaga. Este comando corre en el hilo del actor y publica lo que quedo pendiente.
     */
    record Flush() implements InstrumentCommand {
    }

    record Stop() implements InstrumentCommand {
    }
}
