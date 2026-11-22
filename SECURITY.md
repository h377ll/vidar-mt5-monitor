# Seguridad de la versión experimental

Solo código revisado y fixtures sintéticos. Se excluyen claves, tokens, `.env`,
configuraciones de cuenta, bases SQLite, sidecars, spool, logs operativos,
históricos privados, binarios y metodología propietaria.

El `.gitignore` es preventivo, no una auditoría. Antes de publicar cambios:

```bash
gitleaks dir . --redact --no-banner
gitleaks git . --redact --no-banner --log-opts=--all
```

Revisar además inventario, cuentas, rutas, fixtures y derechos: Gitleaks no
detecta toda información privada. No desactivar reglas para obtener un resultado
limpio. El manifiesto `SHA256SUMS` usa rutas relativas y excluye únicamente el
propio manifiesto y los metadatos Git.

No instalar el EA ni configurar un puente con cuentas reales desde esta versión.
Los gates observacionales, hashes y tests sintéticos no acreditan aislamiento
absoluto ni autorización comercial. El servidor RPyC clásico exige aislamiento
de red y privilegios; no se incluye ni se inicia aquí.

Para informar de una incidencia, no publicar valores de credenciales, cuentas,
históricos ni payloads privados en issues. Reproducir con datos inventados.
