#!/usr/bin/env python3
"""
Compara lo que el agente de GraalVM observo en ejecucion contra lo que el pom.xml registra
para native-image, y lista lo que FALTA.

Por que existe: la app funciona en la JVM y falla en el nativo. La diferencia es siempre lo
mismo — algo que se accede por reflexion, un ResourceBundle o un recurso que no esta declarado.
El agente lo observa de verdad en vez de adivinarlo.

Uso:
    1. Corre la app en la JVM con el agente (ver instrucciones abajo)
    2. Ejercita lo que falla: doble click en Tendencia, boton de velas en Estadisticas, Configuracion
    3. Cierra la app (el agente escribe al salir)
    4. python3 tools/diff-native-config.py /tmp/ni-config
"""
import json, re, sys, pathlib

def cargar(p):
    try:
        return json.loads(pathlib.Path(p).read_text())
    except Exception:
        return None

def main():
    if len(sys.argv) < 2:
        print(__doc__); sys.exit(2)
    cfg = pathlib.Path(sys.argv[1])
    pom = pathlib.Path(__file__).resolve().parent.parent / 'pom.xml'
    texto_pom = pom.read_text()

    # Lo que el pom ya registra
    reg_clases = set(re.findall(r'<list>([^<]+)</list>', texto_pom))
    reg_bundles = set(re.findall(r'<bundle>([^<]+)</bundle>', texto_pom))

    print(f"pom.xml registra: {len(reg_clases)} clases, {len(reg_bundles)} bundles\n")

    # --- clases observadas por el agente ---
    obs = cargar(cfg / 'reflect-config.json') or []
    obs_clases = {e.get('name') for e in obs if e.get('name')}
    faltan = sorted(c for c in obs_clases - reg_clases if c)
    print(f"=== CLASES observadas por reflexion y NO registradas: {len(faltan)} ===")
    # Se agrupan por paquete: lo que importa es el patron, no la lista cruda
    grupos = {}
    for c in faltan:
        grupos.setdefault(c.rsplit('.', 1)[0], []).append(c)
    for paquete in sorted(grupos, key=lambda k: -len(grupos[k]))[:20]:
        print(f"  {len(grupos[paquete]):4d}  {paquete}")
        for c in grupos[paquete][:3]:
            print(f"          {c}")

    # --- bundles observados ---
    res = cargar(cfg / 'resource-config.json') or {}
    obs_bundles = set()
    for b in (res.get('bundles') or []):
        n = b.get('name')
        if n: obs_bundles.add(n)
    faltan_b = sorted(obs_bundles - reg_bundles)
    print(f"\n=== BUNDLES cargados y NO registrados: {len(faltan_b)} ===")
    for b in faltan_b:
        print(f"  <bundle>{b}</bundle>")
    if not faltan_b:
        print("  (ninguno: los bundles estan cubiertos)")

    # --- foco en los sospechosos de este caso ---
    print("\n=== Foco: gráficos y FXML ===")
    for patron in ('org.jfree', 'java.awt', 'javafx.scene.control', 'cl.vc.blotter.controller'):
        n = len([c for c in faltan if c.startswith(patron)])
        print(f"  {patron:28s} sin registrar: {n}")

if __name__ == '__main__':
    main()
