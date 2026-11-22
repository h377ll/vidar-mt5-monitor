# Observadores demo: fuentes experimentales

`ea/VidarObserverDemo.mq5` conserva el prototipo 0.10 como referencia A/B.
El candidato recomendado para **revisión**, no instalación automática, es
`ea/phase_c/VidarObserverDemoC.mq5` 0.11.

C exige `ExpectedLogin` y `ExpectedServer` locales (defaults inválidos),
conexión y cuenta demo antes/después de muestreos, spool por stream nuevo,
escape JSON y comprobaciones de I/O. La discrepancia bloquea la instancia y
descarta la cola RAM; no reutiliza eventos de otra identidad. Callback y deinit
no escriben el spool. No contiene primitivas de trading, red, DLL ni imports.

Límites: las lecturas de identidad no forman una transacción atómica de MT5.
Cola limitada, reinicialización y pérdidas requieren reconciliación histórica.
`complete` no prueba atomicidad ni cobertura de todos los eventos. El spool no
tiene límite de tamaño. Ningún EX5 se publica ni se acredita por este arnés.

Reproducción: `python3 -B tests/run_phase_c_host.py --output-dir /tmp/vidar-c-new`.
El arnés adapta únicamente propiedades y el buffer dinámico a C++, usando
dobles limitados de API. No sustituye una compilación de MetaEditor, conexión
demo ni ensayo del broker. La gestión real y la ejecución autónoma no están
validadas ni se autorizan por pasar estas pruebas.
