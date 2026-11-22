# Dependencias y atribuciones

No se han encontrado archivos LICENSE/NOTICE ni cabeceras de licencia en el
proyecto de origen. No se ha añadido una licencia por inferencia ni se ha
vendorizado código de terceros. Se conservan las cabeceras de los fuentes.

La implementación seleccionada procede del proyecto del propietario y de su
paquete A/B/C revisado; los hashes y transformaciones constan en
[procedencia](docs/PROVENANCE.md).

Dependencias externas, **no incluidas** en esta distribución:

| Dependencia/herramienta | Uso y origen |
|---|---|
| Python y biblioteca estándar | Runtime y arneses; https://www.python.org/ |
| Compilador C++17 | Dobles host del EA; no compila MQL5 |
| RPyC | Importación opcional del cliente experimental; https://github.com/tomerfiliba-org/rpyc |
| MetaTrader 5 / API Python MetaTrader5 | API de un futuro puente externo; https://www.mql5.com/en/docs/python_metatrader5 |
| Gitleaks 8.30.1 | Auditoría local, no requerido para ejecutar los tests; https://github.com/gitleaks/gitleaks/releases/tag/v8.30.1 |

Cada dependencia conserva sus propios términos. El paquete no redistribuye
MT5, Wine, RPyC, servidores, Gitleaks ni sus binarios y no concede derechos
sobre ellos. La compatibilidad de un despliegue vivo sigue sin acreditarse.
