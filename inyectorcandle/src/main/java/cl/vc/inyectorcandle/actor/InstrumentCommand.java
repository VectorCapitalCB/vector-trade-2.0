package cl.vc.inyectorcandle.actor;

import cl.vc.inyectorcandle.model.InstrumentStats;
import cl.vc.inyectorcandle.model.MarketDataEvent;
import cl.vc.inyectorcandle.model.TradeEvent;

public sealed interface InstrumentCommand permits InstrumentCommand.OnMarketData, InstrumentCommand.OnTrade, InstrumentCommand.Stop {

    record OnMarketData(MarketDataEvent event) implements InstrumentCommand {
    }

    record OnTrade(TradeEvent event) implements InstrumentCommand {
    }

    record Stop() implements InstrumentCommand {
    }
}
