package cl.vc.blotter.controller;

import cl.vc.blotter.model.HistoricalTradingAnalytics;
import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import com.google.protobuf.Timestamp;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HistoricalOrdersWorkbookTest {

    @Test
    void generaReporteCorporativoConOrderIdFixYContraparteReal() throws Exception {
        List<BlotterMessage.HistoricalOrderGroup> groups = List.of(
                group("INTERNAL-1", "FIX-90001", "LTM", RoutingMessage.Side.BUY,
                        24.02, 120_000, "VCC", 1),
                group("INTERNAL-2", "FIX-90002", "SQM-B", RoutingMessage.Side.SELL,
                        66_270, 300, "BTG", 2),
                group("INTERNAL-3", "FIX-90003", "CHILE", RoutingMessage.Side.BUY,
                        186.43, 32_000, "", 3),
                group("INTERNAL-4", "FIX-90004", "CENCOSUD", RoutingMessage.Side.SELL,
                        1_985.42, 3_500, "VCC", 4));
        HistoricalTradingAnalytics.Snapshot analytics = HistoricalTradingAnalytics.calculate(groups);

        try (Workbook workbook = new HistoricalOrdersController().createHistoricalWorkbook(
                groups, analytics,
                new HistoricalOrdersController.ExportContext("18415523/0",
                        LocalDate.of(2026, 8, 5), LocalDate.of(2026, 8, 12), "",
                        "12-08-2026 14:30:00"))) {
            assertEquals(List.of("Resumen", "Órdenes", "Ejecuciones"),
                    List.of(workbook.getSheetName(0), workbook.getSheetName(1), workbook.getSheetName(2)));

            Sheet overview = workbook.getSheet("Resumen");
            assertEquals("ÓRDENES HISTÓRICAS", overview.getRow(0).getCell(2).getStringCellValue());
            assertTrue(overview.getNumMergedRegions() >= 10);
            assertNotNull(((XSSFSheet) overview).getDrawingPatriarch());
            assertFalse(((XSSFSheet) overview).getDrawingPatriarch().getCharts().isEmpty());

            Sheet orders = workbook.getSheet("Órdenes");
            assertEquals("OrderID FIX", orders.getRow(0).getCell(11).getStringCellValue());
            assertEquals("FIX-90001", orders.getRow(1).getCell(11).getStringCellValue());

            Sheet executions = workbook.getSheet("Ejecuciones");
            assertEquals("OrderID FIX", executions.getRow(0).getCell(1).getStringCellValue());
            assertEquals("Contraparte", executions.getRow(0).getCell(11).getStringCellValue());
            assertEquals("FIX-90001", executions.getRow(1).getCell(1).getStringCellValue());
            assertEquals("VCC", executions.getRow(1).getCell(11).getStringCellValue());

            writePreviewWhenRequested(workbook);
        }
    }

    @Test
    void omiteColumnaContraparteCuandoTodasLasEjecucionesVienenVacias() throws Exception {
        List<BlotterMessage.HistoricalOrderGroup> groups = List.of(
                group("INTERNAL-1", "FIX-100", "LTM", RoutingMessage.Side.BUY,
                        24.02, 10, "", 1));
        HistoricalTradingAnalytics.Snapshot analytics = HistoricalTradingAnalytics.calculate(groups);

        try (Workbook workbook = new HistoricalOrdersController().createHistoricalWorkbook(
                groups, analytics,
                new HistoricalOrdersController.ExportContext("Todas",
                        LocalDate.of(2026, 8, 12), LocalDate.of(2026, 8, 12), "LTM",
                        "12-08-2026 14:30:00"))) {
            Sheet executions = workbook.getSheet("Ejecuciones");
            assertEquals(11, executions.getRow(0).getLastCellNum());
            assertEquals("Estado", executions.getRow(0).getCell(10).getStringCellValue());
        }
    }

    private static BlotterMessage.HistoricalOrderGroup group(
            String internalId, String fixId, String symbol, RoutingMessage.Side side,
            double price, double quantity, String counterparty, long seconds) {
        RoutingMessage.Order execution = RoutingMessage.Order.newBuilder()
                .setId(internalId)
                .setOrderID(fixId)
                .setExecId("EXEC-" + fixId)
                .setAccount("18415523/0")
                .setSymbol(symbol)
                .setSide(side)
                .setOrderQty(quantity)
                .setLastQty(quantity)
                .setLastPx(price)
                .setCumQty(quantity)
                .setAvgPrice(price)
                .setOrdStatus(RoutingMessage.OrderStatus.FILLED)
                .setExecType(RoutingMessage.ExecutionType.EXEC_TRADE)
                .setContraBroker(counterparty)
                .setTime(Timestamp.newBuilder().setSeconds(1_786_544_000L + seconds))
                .build();
        return BlotterMessage.HistoricalOrderGroup.newBuilder()
                .setSummary(execution)
                .addExecutions(execution)
                .build();
    }

    private static void writePreviewWhenRequested(Workbook workbook) throws Exception {
        String previewPath = System.getProperty("historicalWorkbookPreview", "").trim();
        if (previewPath.isEmpty()) return;
        Path output = Path.of(previewPath);
        if (output.getParent() != null) Files.createDirectories(output.getParent());
        try (OutputStream stream = Files.newOutputStream(output)) {
            workbook.write(stream);
        }
    }
}
