#!/bin/bash
set -e

echo "=== 1. Escaneo de dependencias con pip-audit ==="
pip-audit || true

echo ""
echo "=== 2. Escaneo de código con Bandit ==="
bandit -r . || true

echo ""
echo "=== 3. Escaneo de código con Semgrep (reglas OWASP/CWE) ==="
semgrep --config p/owasp-top-ten --config p/cwe-top-25 || true

echo ""
echo "=== 4. Análisis con SonarQube local ==="
# Ajusta la ruta si tienes sonar-scanner en otra ubicación
sonar-scanner
