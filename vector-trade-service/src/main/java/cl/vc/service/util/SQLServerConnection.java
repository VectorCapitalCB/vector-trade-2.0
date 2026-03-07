package cl.vc.service.util;

import cl.vc.module.protocolbuff.blotter.BlotterMessage;
import cl.vc.module.protocolbuff.generator.TimeGenerator;
import cl.vc.service.MainApp;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@Slf4j
public class SQLServerConnection {


    public static Connection connection = null;

    public static Connection getConnection(Properties prop) {

        try {

            String url = "jdbc:sqlserver://" + prop.getProperty("server.sql") + ";databaseName=" + prop.getProperty("database.sql") + ";ApplicationIntent=ReadOnly";
            String user = prop.getProperty("username.sql");
            String password = prop.getProperty("password.sql");

            connection = DriverManager.getConnection(url, user, password);
            log.info("Conexión exitosa a SQL Server.");

        } catch (Exception e) {
            log.info("Error al registrar el controlador JDBC.");
        }

        return connection;
    }


    public static List<String> getAccountByrut(String rut) {


        List<String> accountsUser = new ArrayList<>();

        try {

            Statement statement = connection.createStatement();

            String query = "select distinct num_cuenta , identificador\n" +
                    "            from [dbo].[VIEW_CUENTAS] WHERE identificador LIKE ?";


            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, rut + "%");
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {

                try {

                    accountsUser.add(resultSet.getString("num_cuenta"));

                } catch (SQLException e) {
                    log.error(e.getMessage(), e);
                    return null;
                }
            }

            resultSet.close();
            statement.close();

            return accountsUser;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return null;


    }


    public static List<BlotterMessage.SaldoCaja.Builder> saldoCaja(String rut) {
        List<BlotterMessage.SaldoCaja.Builder> saldoList = new ArrayList<>();


        String combinedQuery =
                "WITH SumaTotalCTE_Corrected AS (                                         \n" +
                        "    SELECT ISNULL(SUM(MontoCalculado), 0) AS MONTO_SUMA_TOTAL_GENERAL      \n" +
                        "    FROM (                                                                 \n" +
                        "        SELECT ISNULL(PCalc.CANTIDAD_VIGENTE, 0) *                         \n" +
                        "               ISNULL(PX.PRECIO_AYER, 0) AS MontoCalculado                 \n" +
                        "        FROM (                                                             \n" +
                        "            SELECT DISTINCT                                                \n" +
                        "                   PRE.NEMOTECNICO     AS PRE_NEMOTECNICO,                 \n" +
                        "                   CUS.CANTIDAD_VIGENTE                                    \n" +
                        "            FROM (                                                         \n" +
                        "                    SELECT DISTINCT                                        \n" +
                        "                           I.NEMOTECNICO, PD.CTA_PRESTATARIO,              \n" +
                        "                           P.FECHA_OPERACION, PD.CANTIDAD_ASIGNADA         \n" +
                        "                    FROM  dbo.INSTRUMENTO        I                         \n" +
                        "                    JOIN  dbo.PRESTAMO           P  ON I.ID_INSTRUMENTO = P.ID_INSTRUMENTO\n" +
                        "                    JOIN  dbo.VIEW_CUENTAS       V  ON P.ID_CUENTA_PRESTATARIO = V.ID_CUENTA\n" +
                        "                                                   AND V.NUM_CUENTA LIKE ? \n" + // parámetro 1
                        "                    JOIN (SELECT D.ID_PRESTAMO      AS IDPRESTAMO,         \n" +
                        "                                 V.ID_CUENTA        AS CTA_PRESTATARIO,    \n" +
                        "                                 D.CANTIDAD_ASIGNADA                       \n" +
                        "                          FROM dbo.PRESTAMO_DETALLE D                      \n" +
                        "                          JOIN dbo.VIEW_CUENTAS V ON D.ID_CUENTA_PRESTAMISTA = V.ID_CUENTA)\n" +
                        "                                 PD ON P.ID_PRESTAMO = PD.IDPRESTAMO       \n" +
                        "                    WHERE P.ESTADO_PRESTAMO = 'VIG'                        \n" +
                        "                 ) PRE                                                     \n" +
                        "            JOIN (SELECT c.ID_CUENTA  AS PRESTATARIO2,                     \n" +
                        "                         c.NEMOTECNICO AS NEMO2,                           \n" +
                        "                         c.ING_FECHA  AS ING_FECHA2,                       \n" +
                        "                         SUM(c.CANTIDAD) AS CANTIDAD_VIGENTE               \n" +
                        "                  FROM  dbo.Rcp_FN_Consulta_Cartera_OnLine(                \n" +
                        "                         CONVERT(varchar, GETDATE(), 112), 5, NULL,'CLP','PRE') c\n" +
                        "                  GROUP BY c.ID_CUENTA, c.NEMOTECNICO,                     \n" +
                        "                           c.ING_ID_OPERACION_DETALLE, c.ING_FECHA) CUS    \n" +
                        "              ON PRE.CTA_PRESTATARIO = CUS.PRESTATARIO2                    \n" +
                        "             AND PRE.NEMOTECNICO     = CUS.NEMO2                           \n" +
                        "             AND CAST(PRE.FECHA_OPERACION AS date) = CAST(CUS.ING_FECHA2 AS date)\n" +
                        "             AND PRE.CANTIDAD_ASIGNADA = CUS.CANTIDAD_VIGENTE              \n" +
                        "        ) PCalc                                                            \n" +
                        "        OUTER APPLY (SELECT TOP 1 pr.PRECIO AS PRECIO_AYER                 \n" +
                        "                     FROM dbo.PUBLICADOR_PRECIO pr                         \n" +
                        "                     WHERE pr.NEMOTECNICO = PCalc.PRE_NEMOTECNICO          \n" +
                        "                       AND pr.FECHA < CAST(GETDATE() AS date)              \n" +
                        "                     ORDER BY pr.FECHA DESC) PX                            \n" +
                        "    ) MontosParaSumar                                                      \n" +
                        ")                                                                          \n" +
                        "SELECT S.IDENTIFICADOR, S.ID_CUENTA, S.NUM_CUENTA, S.DSC_CUENTA,           \n" +
                        "       S.FECHA_CIERRE, S.EST_SALDO_CAJA, S.TIPO_CAJA,                      \n" +
                        "       S.MONTO_MON_CAJA, S.MONTO_EN_PESOS,                                 \n" +
                        "       S.MONTO_X_COBRAR_MON_CTA, S.MONTO_X_PAGAR_MON_CTA,                  \n" +
                        "       (S.MONTO_X_COBRAR_MON_CTA - S.MONTO_X_PAGAR_MON_CTA) AS MONTO_TRANSITO,\n" +
                        "       G.GTA_EFEC,                                                         \n" +
                        "       (SELECT MONTO_SUMA_TOTAL_GENERAL FROM SumaTotalCTE_Corrected) AS MONTO_SUMA_TOTAL_GENERAL\n" +
                        "FROM   [Capitaria].[dbo].[saldos_de_caja_T_1] S                            \n" +
                        "LEFT JOIN (                                                                 \n" +
                        "    SELECT CLI.IDENTIFICADOR, CLI.NUM_CUENTA, SUM(CV.CANTIDAD) AS GTA_EFEC  \n" +
                        "    FROM (SELECT CASE WHEN cu.num_cuenta <> '' THEN 'Gta_Efect' END AS CARTERA,\n" +
                        "                 cu.num_cuenta, c.NEMOTECNICO, c.CANTIDAD, c.COD_MONEDA_CARTERA\n" +
                        "          FROM dbo.Rcp_FN_Consulta_Cartera_OnLine(                          \n" +
                        "                 CONVERT(VARCHAR, DATEADD(DAY,-1,GETDATE()),112),5,NULL,'CLP','GCJ') c\n" +
                        "          INNER JOIN dbo.cuenta cu ON cu.id_cuenta = c.id_cuenta) CV        \n" +
                        "    INNER JOIN [Capitaria].[dbo].[VIEW_CUENTAS] CLI                         \n" +
                        "      ON CV.num_cuenta = CLI.NUM_CUENTA AND CV.COD_MONEDA_CARTERA = CLI.COD_MONEDA\n" +
                        "    WHERE CV.CARTERA LIKE '%GTA%' AND CV.NEMOTECNICO = 'CAJA $$'            \n" +
                        "    GROUP BY CLI.IDENTIFICADOR, CLI.NUM_CUENTA                              \n" +
                        "    HAVING SUM(CV.CANTIDAD) <> 0                                            \n" +
                        ") G ON S.NUM_CUENTA = G.NUM_CUENTA AND S.IDENTIFICADOR = G.IDENTIFICADOR   \n" +
                        "WHERE  S.NUM_CUENTA LIKE ?;";   // parámetro 2
        // --------------------------------------------------------------------------------------------

        try (PreparedStatement ps = connection.prepareStatement(combinedQuery)) {

            // Los dos parámetros LIKE ?
            ps.setString(1, rut + "%");
            ps.setString(2, rut + "%");

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {


                    BlotterMessage.SaldoCaja.Builder saldo = BlotterMessage.SaldoCaja.newBuilder();

                    if (rs.getString("IDENTIFICADOR") != null) {
                        saldo.setIdentificador(rs.getString("IDENTIFICADOR"));
                    }

                    if (rs.getString("ID_CUENTA") != null) {
                        saldo.setIdCuenta(rs.getString("ID_CUENTA"));
                    }

                    if (rs.getString("NUM_CUENTA") != null) {
                        saldo.setNumCuenta(rs.getString("NUM_CUENTA"));
                    }

                    if (rs.getString("DSC_CUENTA") != null) {
                        saldo.setDescripcionCuenta(rs.getString("DSC_CUENTA"));
                    }

                    Date fechaCierre = rs.getDate("FECHA_CIERRE");
                    if (fechaCierre != null) {
                        LocalDateTime ldt = fechaCierre.toLocalDate()
                                .atStartOfDay(MainApp.getZoneId())
                                .toLocalDateTime();
                        saldo.setFechaCierre(TimeGenerator.toProtoTimestampUTC(ldt, MainApp.getZoneId()));
                    }


                    if (rs.getString("EST_SALDO_CAJA") != null) {
                        saldo.setEstadoSaldoCaja(rs.getString("EST_SALDO_CAJA"));
                    }

                    if (rs.getString("TIPO_CAJA") != null) {
                        saldo.setTipoCaja(rs.getString("TIPO_CAJA"));
                    }

                    if (rs.getString("MONTO_MON_CAJA") != null) {
                        saldo.setMontoMonedaCaja(rs.getString("MONTO_MON_CAJA"));
                    }

                    if (rs.getString("MONTO_EN_PESOS") != null) {
                        saldo.setMontoEnPesos(rs.getString("MONTO_EN_PESOS"));
                    }

                    if (rs.getString("MONTO_X_COBRAR_MON_CTA") != null) {
                        saldo.setMontoPorCobrarMonedaCuenta(rs.getString("MONTO_X_COBRAR_MON_CTA"));
                    }

                    if (rs.getString("MONTO_X_PAGAR_MON_CTA") != null) {
                        saldo.setMontoPorPagarMonedaCuenta(rs.getString("MONTO_X_PAGAR_MON_CTA"));
                    }

                    if (rs.getString("MONTO_TRANSITO") != null) {
                        saldo.setMontoTransito(rs.getString("MONTO_TRANSITO"));
                    }


                    String gtaEfect = rs.getString("GTA_EFEC");
                    saldo.setGarantiaEfectivo(gtaEfect == null ? "0" : gtaEfect);


                    if (rs.getString("MONTO_SUMA_TOTAL_GENERAL") != null) {
                        saldo.setPrestamo(rs.getString("MONTO_SUMA_TOTAL_GENERAL"));
                    }


                    saldoList.add(saldo);
                }
            }

        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }

        return saldoList;
    }

    public static List<BlotterMessage.Prestamos.Builder> prestamos(String rut) {
        List<BlotterMessage.Prestamos.Builder> prestamosList = new ArrayList<>();

        final String sql =
                "WITH BasePCalc AS (                                                                 \n" +
                        "    SELECT DISTINCT                                                                  \n" +
                        "           I.NEMOTECNICO                  AS PRE_NEMOTECNICO,                         \n" +
                        "           CUS.CANTIDAD_VIGENTE,                                                       \n" +
                        "           P.FECHA_OPERACION              AS FECHA_INGRESO,                            \n" +
                        "           P.PLAZO                        AS PLAZO_PIMO,                               \n" +
                        "           P.FECHA_VENCIMIENTO            AS FECHA_VTO                                  \n" +
                        "    FROM dbo.PRESTAMO P                                                                  \n" +
                        "    JOIN dbo.INSTRUMENTO I                                                              \n" +
                        "      ON I.ID_INSTRUMENTO = P.ID_INSTRUMENTO                                            \n" +
                        "    JOIN dbo.VIEW_CUENTAS V                                                             \n" +
                        "      ON P.ID_CUENTA_PRESTATARIO = V.ID_CUENTA                                          \n" +
                        "     AND V.NUM_CUENTA = ?                                                               \n" + // <-- parámetro 1 (rut)
                        "    JOIN (                                                                              \n" +
                        "        SELECT  D.ID_PRESTAMO   AS IDPRESTAMO,                                          \n" +
                        "                V2.ID_CUENTA    AS CTA_PRESTATARIO,                                     \n" +
                        "                D.CANTIDAD_ASIGNADA                                                     \n" +
                        "        FROM dbo.PRESTAMO_DETALLE D                                                     \n" +
                        "        JOIN dbo.VIEW_CUENTAS V2                                                        \n" +
                        "          ON D.ID_CUENTA_PRESTAMISTA = V2.ID_CUENTA                                     \n" +
                        "    ) PD                                                                                \n" +
                        "      ON P.ID_PRESTAMO = PD.IDPRESTAMO                                                  \n" +
                        "    JOIN (                                                                              \n" +
                        "        SELECT  c.ID_CUENTA  AS PRESTATARIO2,                                           \n" +
                        "                c.NEMOTECNICO AS NEMO2,                                                 \n" +
                        "                c.ING_FECHA  AS ING_FECHA2,                                             \n" +
                        "                SUM(c.CANTIDAD) AS CANTIDAD_VIGENTE                                     \n" +
                        "        FROM dbo.Rcp_FN_Consulta_Cartera_OnLine(                                        \n" +
                        "                CONVERT(varchar, GETDATE(), 112), 5, NULL,'CLP','PRE') c                \n" +
                        "        GROUP BY c.ID_CUENTA, c.NEMOTECNICO, c.ING_ID_OPERACION_DETALLE, c.ING_FECHA    \n" +
                        "    ) CUS                                                                               \n" +
                        "      ON PD.CTA_PRESTATARIO = CUS.PRESTATARIO2                                          \n" +
                        "     AND I.NEMOTECNICO      = CUS.NEMO2                                                 \n" +
                        "     AND CAST(P.FECHA_OPERACION AS date) = CAST(CUS.ING_FECHA2 AS date)                 \n" +
                        "     AND PD.CANTIDAD_ASIGNADA  = CUS.CANTIDAD_VIGENTE                                   \n" +
                        "    WHERE P.ESTADO_PRESTAMO = 'VIG'                                                     \n" +
                        ")                                                                                       \n" +
                        "SELECT                                                                                   \n" +
                        "    B.PRE_NEMOTECNICO                              AS NEMOTECNICO,                       \n" +
                        "    SUM(B.CANTIDAD_VIGENTE)                        AS CANTIDAD_VIGENTE,                  \n" +
                        "    MAX(PX.PRECIO_AYER)                            AS PRECIO_AYER,                       \n" +
                        "    SUM(ISNULL(B.CANTIDAD_VIGENTE,0) * ISNULL(PX.PRECIO_AYER,0)) AS MONTO,               \n" +
                        "    CONVERT(date, MIN(B.FECHA_INGRESO))            AS FECHA_INGRESO,                     \n" +
                        "    MAX(B.PLAZO_PIMO)                              AS PLAZO_PIMO,                        \n" +
                        "    CONVERT(date, MAX(B.FECHA_VTO))                AS FECHA_VENCIMIENTO                  \n" +
                        "FROM BasePCalc B                                                                         \n" +
                        "OUTER APPLY (                                                                            \n" +
                        "    SELECT TOP 1 pr.PRECIO AS PRECIO_AYER                                               \n" +
                        "    FROM dbo.PUBLICADOR_PRECIO pr                                                        \n" +
                        "    WHERE pr.NEMOTECNICO = B.PRE_NEMOTECNICO                                             \n" +
                        "      AND pr.FECHA < CONVERT(date, GETDATE())                                            \n" +
                        "    ORDER BY pr.FECHA DESC                                                               \n" +
                        ") PX                                                                                     \n" +
                        "GROUP BY B.PRE_NEMOTECNICO                                                               \n" +
                        "ORDER BY MONTO DESC;";

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, rut); // @NumCuenta

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    BlotterMessage.Prestamos.Builder p = BlotterMessage.Prestamos.newBuilder();

                    p.setNemotecnico(rs.getString("NEMOTECNICO"));
                    p.setCantidadVigente(rs.getDouble("CANTIDAD_VIGENTE"));
                    p.setPrecioAyer(rs.getDouble("PRECIO_AYER"));
                    p.setMonto(rs.getDouble("MONTO"));
                    java.sql.Date fi = rs.getDate("FECHA_INGRESO");
                    if (fi != null) p.setFechaIngreso(fi.toLocalDate().toString());
                    java.sql.Date fv = rs.getDate("FECHA_VENCIMIENTO");
                    if (fv != null) p.setFechaVto(fv.toLocalDate().toString());
                    Object plazoObj = rs.getObject("PLAZO_PIMO");
                    if (plazoObj != null) p.setPlazoPimo(plazoObj.toString());

                    prestamosList.add(p);
                }
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }

        return prestamosList;
    }



    public static List<BlotterMessage.CierreCarteraResumida.Builder> carteraResumida(String rut) {

        List<BlotterMessage.CierreCarteraResumida.Builder> carteraList = new ArrayList<>();

        try {

            Statement statement = connection.createStatement();

            String query =
                    "SELECT V.ID_ASESOR, C.fecha_cierre, C.PRECIO_TASA_MERCADO, C.PRECIO_TASA_COMPRA, V.NUM_CUENTA, I.COD_MONEDA, " +
                            "       V.DSC_CUENTA AS NOMBRE_CLI, C.CANTIDAD AS LIBRE, C.GARANTIA, C.COMPRAS_PLAZO, C.VENTAS_PLAZO,  " +
                            "       CASE WHEN PRESTAMOS IS NULL THEN 0 WHEN PRESTAMOS IS NOT NULL THEN PRESTAMOS END AS PRESTAMOS_ACC, " +
                            "       V.IDENTIFICADOR, I.NEMOTECNICO, V.NOMBRE_ASESOR,  " +
                            "       CASE " +
                            "          WHEN COD_SUB_CLASE_INSTRUMENTO IN ('CFI', 'ACC') AND I.COD_MONEDA = 'CLP' THEN CANTIDAD * PRECIO_TASA_MERCADO " +
                            "          WHEN COD_SUB_CLASE_INSTRUMENTO IN ('CFI', 'ACC') AND I.COD_MONEDA <> 'CLP' THEN ((VALOR_MERCADO_CLP) / (CANTIDAD + GARANTIA) * cantidad) " +
                            "          WHEN COD_SUB_CLASE_INSTRUMENTO NOT IN ('CFI', 'ACC') THEN ((VALOR_MERCADO_CLP) / (CANTIDAD + GARANTIA) * cantidad)  " +
                            "       END AS LIBRE_CLP, " +
                            "       CASE " +
                            "          WHEN COD_SUB_CLASE_INSTRUMENTO IN ('CFI', 'ACC') AND I.COD_MONEDA = 'CLP' THEN GARANTIA * PRECIO_TASA_MERCADO " +
                            "          WHEN COD_SUB_CLASE_INSTRUMENTO IN ('CFI', 'ACC') AND I.COD_MONEDA <> 'CLP' THEN ((VALOR_MERCADO_CLP) / (CANTIDAD + GARANTIA) * GARANTIA) " +
                            "          WHEN COD_SUB_CLASE_INSTRUMENTO NOT IN ('CFI', 'ACC') THEN ((VALOR_MERCADO_CLP) / (CANTIDAD + GARANTIA) * GARANTIA)  " +
                            "       END AS GARANTIA_CLP, " +
                            "       C.COMPRAS_PLAZO * C.PRECIO_TASA_MERCADO AS SIM_COMPRA_CLP, " +
                            "       C.VENTAS_PLAZO * C.PRECIO_TASA_MERCADO AS SIM_VENTA_CLP, " +
                            "       C.VALOR_MERCADO_CLP,  " +
                            "       I.COD_SUB_CLASE_INSTRUMENTO " +
                            "FROM dbo.cierre_cartera_resumida AS C WITH (NOLOCK) " +
                            "INNER JOIN dbo.VIEW_CUENTAS AS V WITH (NOLOCK) ON C.ID_CUENTA = V.ID_CUENTA " +
                            "INNER JOIN dbo.INSTRUMENTO AS I WITH (NOLOCK) ON C.ID_INSTRUMENTO = I.ID_INSTRUMENTO " +
                            "WHERE FECHA_CIERRE = CAST(GETDATE() -1 AS date) " +
                            "  AND V.NUM_CUENTA LIKE ? " +
                            "  AND I.COD_SUB_CLASE_INSTRUMENTO <> 'FMV'";


            PreparedStatement preparedStatement = connection.prepareStatement(query);
            preparedStatement.setString(1, rut + "%");
            //preparedStatement.setString(1, rut);
            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {

                try {

                    BlotterMessage.CierreCarteraResumida.Builder cierre = BlotterMessage.CierreCarteraResumida.newBuilder();

                    if (resultSet.getString("ID_ASESOR") != null) {
                        cierre.setIdAsesor(resultSet.getString("ID_ASESOR"));
                    }

                    Date fechaCierre = resultSet.getDate("fecha_cierre");
                    if (fechaCierre != null) {
                        LocalDate localDate = fechaCierre.toLocalDate();
                        LocalDateTime localDateTime = localDate.atStartOfDay(MainApp.getZoneId()).toLocalDateTime();
                        cierre.setFechaCierre(TimeGenerator.toProtoTimestampUTC(localDateTime, MainApp.getZoneId()));
                    }

                    if (resultSet.getObject("PRECIO_TASA_MERCADO") != null) {
                        cierre.setPrecioTasaMercado(resultSet.getDouble("PRECIO_TASA_MERCADO"));
                    }

                    if (resultSet.getObject("PRECIO_TASA_COMPRA") != null) {
                        cierre.setPrecioTasaCompra(resultSet.getDouble("PRECIO_TASA_COMPRA"));
                    }

                    if (resultSet.getString("NUM_CUENTA") != null) {
                        cierre.setNumCuenta(resultSet.getString("NUM_CUENTA"));
                    }

                    if (resultSet.getString("COD_MONEDA") != null) {
                        cierre.setCodigoMoneda(resultSet.getString("COD_MONEDA"));
                    }

                    if (resultSet.getString("NOMBRE_CLI") != null) {
                        cierre.setNombreCliente(resultSet.getString("NOMBRE_CLI"));
                    }

                    if (resultSet.getObject("LIBRE") != null) {
                        cierre.setLibre(resultSet.getDouble("LIBRE"));
                    }

                    if (resultSet.getObject("GARANTIA") != null) {
                        cierre.setGarantia(resultSet.getDouble("GARANTIA"));
                    }

                    if (resultSet.getObject("COMPRAS_PLAZO") != null) {
                        cierre.setComprasPlazo(resultSet.getDouble("COMPRAS_PLAZO"));
                    }

                    if (resultSet.getObject("VENTAS_PLAZO") != null) {
                        cierre.setVentasPlazo(resultSet.getDouble("VENTAS_PLAZO"));
                    }

                    if (resultSet.getObject("PRESTAMOS_ACC") != null) {
                        cierre.setPrestamosAcc(resultSet.getDouble("PRESTAMOS_ACC"));
                    }

                    if (resultSet.getString("IDENTIFICADOR") != null) {
                        cierre.setIdentificador(resultSet.getString("IDENTIFICADOR"));
                    }

                    if (resultSet.getString("NEMOTECNICO") != null) {
                        cierre.setNemotecNico(resultSet.getString("NEMOTECNICO"));
                    }

                    if (resultSet.getString("NOMBRE_ASESOR") != null) {
                        cierre.setNombreAsesor(resultSet.getString("NOMBRE_ASESOR"));
                    }

                    if (resultSet.getObject("LIBRE_CLP") != null) {
                        cierre.setLibreClp(resultSet.getDouble("LIBRE_CLP"));
                    }

                    if (resultSet.getObject("GARANTIA_CLP") != null) {
                        cierre.setGarantiaClp(resultSet.getDouble("GARANTIA_CLP"));
                    }

                    if (resultSet.getObject("SIM_COMPRA_CLP") != null) {
                        cierre.setSimCompraClp(resultSet.getDouble("SIM_COMPRA_CLP"));
                    }

                    if (resultSet.getObject("SIM_VENTA_CLP") != null) {
                        cierre.setSimVentaClp(resultSet.getDouble("SIM_VENTA_CLP"));
                    }

                    if (resultSet.getObject("VALOR_MERCADO_CLP") != null) {
                        cierre.setValorMercadoClp(resultSet.getDouble("VALOR_MERCADO_CLP"));
                    }

                    if (resultSet.getString("COD_SUB_CLASE_INSTRUMENTO") != null) {
                        cierre.setCodSubClaseInstrumento(resultSet.getString("COD_SUB_CLASE_INSTRUMENTO"));
                    }

                    carteraList.add(cierre);

                } catch (SQLException e) {
                    log.error(e.getMessage(), e);
                }
            }

            resultSet.close();
            statement.close();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return carteraList;

    }

    public static List<BlotterMessage.Simultaneas> consultaSimultanea() {

        try {

            Statement statement = connection.createStatement();

            String query = "select ROW_NUMBER() OVER (ORDER BY S.IDENTIFICADOR) AS Detalle_Simultanea, " +
                    "     case when TIPO_SIMULTANEA = 'cirvs' then 'Simultánea Compra' when TIPO_SIMULTANEA = 'virvs' then 'Simultánea Venta' end  as Tipo_Simul , " +
                    "  S.IDENTIFICADOR Ident_Cliente, S.id_operacion AS ID, S.NUM_CUENTA AS Num_Cuenta," +
                    "    UPPER(LEFT( S.NOMBRE_CLIENTE, 1)) + LOWER(SUBSTRING( S.NOMBRE_CLIENTE, 2, LEN( S.NOMBRE_CLIENTE) - 1)) as Nombre_Cliente,  " +
                    "FECHA_TRANSACCION as Fecha_Operacion  ,FECHA_VCTO_SIM As Fecha_Vcto,               PLAZO as Plazo,             PLAZO_REMANENTE as Plazo_Rem     , " +
                    "ING_NEMO_INSTRUMENTO as Nemotecnico, ING_CANTIDAD as Cantidad, TASA_REAL as Tasa,     PRECIO_CONTADO as Precio_PH,        " +
                    "PRECIO_PLAZO AS Precio_Plazo,    PRECIO_TASA_MERCADO as Precio_Mercado ,\n" +
                    "MONTO_CONTADO  AS Monto_Contado , VALOR_PRESENTE as Monto_Presente ,               MONTO_PLAZO              AS  Monto_Plazo, " +
                    "round ((MONTO_CONTADO - MONTO_PLAZO) / plazo  ,0) as Costo_Diario2, " +
                    "UPPER(LEFT(  CORR_VENTA, 1)) + LOWER(SUBSTRING( CORR_VENTA, 2, LEN(  CORR_VENTA) - 1)) as Corredor_Venta, \n" +
                    "UPPER(LEFT(  CORR_COMP, 1)) + LOWER(SUBSTRING(CORR_COMP, 2, LEN(  CORR_COMP) - 1)) as Corredor_Compra, \n" +
                    "COD_SUB AS Cod_Inst, ING_CANTIDAD_ORIGEN AS Cantidad_Orig, " +
                    "FOLIO_CONTADO as Folio_Fact_PH,FOLIO_PLAZO AS Folio_Fact_TP " +
                    "from SIMULTANEAS_DIARIAS_MO S  with (nolock) LEFT JOIN  VIEW_CUENTAS C with (nolock)\n" +
                    "ON S.NUM_CUENTA=C.NUM_CUENTA";


            PreparedStatement preparedStatement = connection.prepareStatement(query);

            ResultSet resultSet = preparedStatement.executeQuery();
            List<BlotterMessage.Simultaneas> list = new ArrayList<>();

            while (resultSet.next()) {

                try {



                    BlotterMessage.Simultaneas.Builder simultaneasBuilder = BlotterMessage.Simultaneas.newBuilder();
                    ProtoDateProcessor.setDateProcesorIfMissing(simultaneasBuilder);

                    if (resultSet.getString("Tipo_Simul") != null) {
                        simultaneasBuilder.setTipoSimul(resultSet.getString("Tipo_Simul"));
                    }

                    if (resultSet.getString("Detalle_Simultanea") != null) {
                        simultaneasBuilder.setDetalleSimultanea(resultSet.getString("Detalle_Simultanea"));
                    }

                    if (resultSet.getString("Ident_Cliente") != null) {
                        simultaneasBuilder.setIdentCliente(resultSet.getString("Ident_Cliente"));
                    }

                    if (resultSet.getString("Num_Cuenta") != null) {
                        simultaneasBuilder.setNumCuenta(resultSet.getString("Num_Cuenta"));
                    }

                    if (resultSet.getString("Fecha_Operacion") != null) {
                        simultaneasBuilder.setFechaOperacion(resultSet.getString("Fecha_Operacion"));
                    }

                    if (resultSet.getString("Fecha_Vcto") != null) {
                        simultaneasBuilder.setFechaVcto(resultSet.getString("Fecha_Vcto"));
                    }

                    if (resultSet.getString("Nombre_Cliente") != null) {
                        simultaneasBuilder.setNombreCliente(resultSet.getString("Nombre_Cliente"));
                    }

                    if (resultSet.getString("Plazo") != null) {
                        simultaneasBuilder.setPlazo(resultSet.getString("Plazo"));
                    }

                    if (resultSet.getString("Plazo_Rem") != null) {
                        simultaneasBuilder.setPlazoRem(resultSet.getString("Plazo_Rem"));
                    }

                    if (resultSet.getString("Nemotecnico") != null) {
                        simultaneasBuilder.setNemotecnico(resultSet.getString("Nemotecnico"));
                    }

                    if (resultSet.getString("Cantidad") != null) {
                        simultaneasBuilder.setCantidad(resultSet.getString("Cantidad"));
                    }

                    if (resultSet.getString("Tasa") != null) {
                        simultaneasBuilder.setTasa(resultSet.getString("Tasa"));
                    }

                    if (resultSet.getString("Precio_PH") != null) {
                        simultaneasBuilder.setPrecioPH(resultSet.getString("Precio_PH"));
                    }

                    if (resultSet.getString("Precio_Plazo") != null) {
                        simultaneasBuilder.setPrecioPlazo(resultSet.getString("Precio_Plazo"));
                    }

                    if (resultSet.getString("Precio_Mercado") != null) {
                        simultaneasBuilder.setPrecioMercado(resultSet.getString("Precio_Mercado"));
                    }

                    if (resultSet.getString("Monto_Contado") != null) {
                        simultaneasBuilder.setMontoContado(resultSet.getString("Monto_Contado"));
                    }

                    if (resultSet.getString("Costo_Diario2") != null) {
                        simultaneasBuilder.setCostoDiario2(resultSet.getString("Costo_Diario2"));
                    }

                    if (resultSet.getString("Monto_Presente") != null) {
                        simultaneasBuilder.setMontoPresente(resultSet.getString("Monto_Presente"));
                    }

                    if (resultSet.getString("Monto_Plazo") != null) {
                        simultaneasBuilder.setMontoPlazo(resultSet.getString("Monto_Plazo"));
                    }

                    if (resultSet.getString("Cod_Inst") != null) {
                        simultaneasBuilder.setCodInst(resultSet.getString("Cod_Inst"));
                    }

                    if (resultSet.getString("Cantidad_Orig") != null) {
                        simultaneasBuilder.setCantidadOrig(resultSet.getString("Cantidad_Orig"));
                    }

                    if (resultSet.getString("Corredor_Venta") != null) {
                        simultaneasBuilder.setCorredorVenta(resultSet.getString("Corredor_Venta"));
                    }

                    if (resultSet.getString("Corredor_Compra") != null) {
                        simultaneasBuilder.setCorredorCompra(resultSet.getString("Corredor_Compra"));
                    }

                    if (resultSet.getString("Folio_Fact_PH") != null) {
                        simultaneasBuilder.setFolioFactPH(resultSet.getString("Folio_Fact_PH"));
                    }

                    if (resultSet.getString("Folio_Fact_TP") != null) {
                        simultaneasBuilder.setFolioFactTP(resultSet.getString("Folio_Fact_TP"));
                    }

                    if (resultSet.getString("id") != null) {
                        simultaneasBuilder.setId(resultSet.getString("id"));
                    }


                    list.add(simultaneasBuilder.build());

                } catch (SQLException e) {
                    log.error(e.getMessage(), e);

                }

            }

            resultSet.close();
            statement.close();


            return list;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return null;


    }


    public static void main(String[] args) {

        try {


            Properties properties = new Properties();
            properties.put("server.sql", "mi-sql-prd-002.5bf1e89c8cf2.database.windows.net");
            properties.put("database.sql", "capitaria");
            properties.put("username.sql", "victornazar");
            properties.put("password.sql", "y3IaTn11ih1Q*ivQ");
            SQLServerConnection.getConnection(properties);
            MainApp.setZoneId(ZoneId.of("America/Santiago"));
            List<BlotterMessage.CierreCarteraResumida.Builder> cc = SQLServerConnection.carteraResumida("16138017/0");
            System.out.println();


        } catch (Exception exc) {
            log.error("Error al leer parametros:", exc);
        }


    }

}
