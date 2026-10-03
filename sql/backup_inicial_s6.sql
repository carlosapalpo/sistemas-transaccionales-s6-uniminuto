-- Datos iniciales reproducibles. Solo utiliza los esquemas NUEVOS de semana 6.
CREATE DATABASE IF NOT EXISTS sistemas_transaccionales_s6 CHARACTER SET utf8mb4;
CREATE DATABASE IF NOT EXISTS sistemas_transaccionales_s6_destino CHARACTER SET utf8mb4;
USE sistemas_transaccionales_s6;
CREATE TABLE IF NOT EXISTS cuentas(id BIGINT PRIMARY KEY, titular VARCHAR(100), saldo DECIMAL(12,2) NOT NULL CHECK(saldo>=0)) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS productos(id BIGINT PRIMARY KEY, nombre VARCHAR(100), existencias INT NOT NULL CHECK(existencias>=0)) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS eventos(id VARCHAR(100) PRIMARY KEY, ejercicio VARCHAR(30), detalle VARCHAR(255), fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS transacciones_tiempo_real(id VARCHAR(100) PRIMARY KEY, monto DECIMAL(12,2), estado VARCHAR(30), fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP) ENGINE=InnoDB;
INSERT IGNORE INTO cuentas VALUES(1,'Cuenta de ahorros Carlos',1000.00);
INSERT IGNORE INTO productos VALUES(1,'Computador portatil de prueba',10);
USE sistemas_transaccionales_s6_destino;
CREATE TABLE IF NOT EXISTS cuentas(id BIGINT PRIMARY KEY, titular VARCHAR(100), saldo DECIMAL(12,2) NOT NULL CHECK(saldo>=0)) ENGINE=InnoDB;
INSERT IGNORE INTO cuentas VALUES(2,'Cuenta destino Universidad',500.00);
-- INSERT IGNORE permite repetir el script sin reiniciar saldos ni borrar evidencias.
