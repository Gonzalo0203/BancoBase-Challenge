# Ejercicio 2 - Propuesta de Arquitectura de Datos

## 1. Contexto del problema

El objetivo del ejercicio es proponer una arquitectura de datos para consolidar información proveniente de tres fuentes transaccionales que operan 24/7:

| Fuente | Tipo | Descripción |
|---|---|---|
| F1 | CRM propietario | Información de clientes, datos demográficos y datos de contacto |
| F2 | SQL Server | Transacciones de clientes sobre una parte de los productos |
| F3 | PostgreSQL | Transacciones de clientes sobre el resto de los productos |

La arquitectura debe cumplir con dos objetivos principales:

1. Habilitar al área operativa para realizar consultas SQL.
2. Permitir al equipo de ciencia de datos aplicar algoritmos de detección de patrones como clustering o búsquedas en grafos.