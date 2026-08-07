package cl.vc.service.admin;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ManualSaldoOverride {
    private double caja;
    private double cuentaTransitorias;
    private double garantiaEfectivo;
    private double garantiasConstituidas;
    private double garantiasExigidas;
    private double garantiasReservadas;
    private double limiteFinanciero;
    private double garantiasDisponible;
    private double ordenesActivasCompras;
    private double ordenesActivasVentas;
    private double ordenesCalzadasCompras;
    private double ordenesCalzadasVentas;
    private double ordenesCestaCompras;
    private double ordenesCestaVentas;
    private double rendimiento;
    private double total;
}
