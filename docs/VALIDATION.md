# Validación del paquete público · 2026-11-22

Se ejecutó `tools/run_public_checks.py` sobre la copia seleccionada, no sobre
la instalación operativa ni sobre un índice del proyecto de desarrollo.

| Suite | Resultado |
|---|---|
| A | 49/49 |
| B | 30/30 |
| Motor determinista | 60/60; salida idéntica en dos reproducciones |
| Supervisor | 21/21 y circuito E2E aceptado |
| Servicios | 51/51 |
| C-host | 62/62; no es compilación MQL5 |
| Gestión shadow | 45/45 escenarios; cero peticiones broker |
| Backfill HTML | 3/3 |
| Dashboard | 10/10 comprobaciones; 82 filas sintéticas |

**321 pruebas y escenarios**, más dashboard y comprobaciones E2E. Cero fallos,
errores u omisiones. Los cinco cortes abruptos deliberados de B son ensayos
de recuperación, no fallos del paquete.

Los resultados y logs de tests publicables se encuentran en
[`evidence/publication/`](../evidence/publication/results.json).
Los resúmenes omiten rutas y observaciones incidentales del host. No se publican
las SQLite, credenciales efímeras, vaults ni logs operativos. Las entradas son
inventadas; la etiqueta `real` de algún fixture ejercita esquemas o rechazos,
no describe una cuenta observada.

## Controles de publicación

- Procedencia: 66 archivos seleccionados, hashes de origen y públicos verificados.
  Solo dos transformaciones de texto están declaradas en el registro.
- Inventario manual: fuentes, tests, fixtures, docs, metadatos y resúmenes;
  sin enlaces simbólicos, binarios, configuraciones de cuenta ni datos operativos.
- Gitleaks 8.30.1: distribución oficial y checksum verificados; sin excepciones
  ni reglas suprimidas. Directorio y nuevo historial raíz deben dar cero hallazgos
  antes del push; los recibos íntegros se conservan fuera del repositorio.
- Derechos: sin LICENSE/NOTICE de origen, sin nueva licencia añadida y sin
  dependencias vendorizadas. La publicación no equivale a licencia open-source.
- `FILES.txt` enumera el conjunto exacto; `SHA256SUMS` verifica todos sus archivos
  excepto el propio manifiesto y los metadatos `.git`.

Reproducir las pruebas con el README. Verificar integridad desde la raíz con
`sha256sum -c SHA256SUMS`. Las fechas y duraciones de los tests pueden variar;
las comprobaciones deterministas se refieren a datos normalizados, no al log.

## Lo que no se acredita

No hay compilación MetaEditor ni EX5 reproducible; no se consulta un terminal
real, no se gestionan posiciones ni se ejecutan órdenes. Tampoco se acreditan
entrega real de alertas, permisos del broker, cuentas de firmas, robustez de
un despliegue continuo ni motor autónomo. No se instala ni modifica ningún
servicio de OpenClaw, MT5 o SQX.
