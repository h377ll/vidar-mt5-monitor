# Procedencia pública · v0.1.0-dev

Publicación autorizada por el propietario el 2026-11-22. Se seleccionaron fuentes
del proyecto de desarrollo, **sin copiar su historial Git** ni una instalación
operativa. El primer commit público tiene una raíz nueva.

El paquete base A/B/C es `MT5-Monitor-Fases-ABC-2026-10-20.zip`, SHA-256
`fcb291db261b1248d4c72d26e48790864f4dd103ce3ca9ea533f6893703ebc89`.
El registro previo de procedencia relaciona sus archivos con los módulos A/B,
tests y observador 0.10. Las adaptaciones de B posteriores están acreditadas
en el proyecto: imports de paquete, control source/stream y transacciones.
El observador C 0.11, motor y servicios son incrementos del mismo proyecto.

[source-files.json](provenance/source-files.json) registra por archivo los hashes
de origen y copia pública, su pertenencia al paquete base cuando existe, y si
era un archivo versionado o una incorporación del árbol de trabajo revisado.
Un hash acredita bytes, no una licencia. Los archivos con origen A/B conservan
la separación de familias; no se han unificado.

Transformación pública limitada: en los dos `journal.py` se sustituye un texto
obsoleto de cobertura de instalaciones por una advertencia genérica. No cambia
su lógica. El resto del código seleccionado conserva bytes de origen.

README, seguridad, documentación de publicación, ejecutor público y manifiesto
se preparan para esta versión. No se incluyen expedientes de despliegue, parches
históricos, referencias locales ni evidencias operativas. Los únicos resultados
publicados son resúmenes saneados de los ensayos sobre este paquete.

## Estado de derechos

No había LICENSE/NOTICE en el origen, ni se ha identificado código de terceros
vendorizado en el conjunto seleccionado. La autorización de publicación del
propietario se aplica al proyecto propio; no se interpreta como licencia
open-source ni como permiso para redistribuir dependencias externas.
Las cabeceras se mantienen; véase [THIRD_PARTY.md](../THIRD_PARTY.md).

## Exclusiones

Se excluyen `.git` de desarrollo, otras ramas, cachés, binarios, despliegues,
herramientas administrativas, configuraciones privadas, claves, SQLite,
logs, datos/históricos de cuentas y metodología propietaria. No se copia ninguna
instalación activa. La rama de desarrollo y sus cambios sin confirmar se preservan.
