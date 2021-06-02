-- MySQL Workbench Forward Engineering


-- -----------------------------------------------------
-- Database project1
-- -----------------------------------------------------
CREATE DATABASE IF NOT EXISTS project1 ;
USE project1 ;

-- -----------------------------------------------------
-- Table `project1`.`query1_result`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS project1.query1_result (
  data DATE NOT NULL,
  regione VARCHAR(30) NOT NULL,
  "vaccinazioni per centro" INT NULL DEFAULT NULL,
  PRIMARY KEY (data, regione));



-- -----------------------------------------------------
-- Table `project1`.`query2_result`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS project1.query2_result (
  data DATE NOT NULL,
  "fascia anagrafica" VARCHAR(32) NOT NULL,
  regione VARCHAR(32) NOT NULL,
  "vaccinazioni previste" INT NOT NULL ,
  PRIMARY KEY (data, "fascia anagrafica", regione));


-- -----------------------------------------------------
-- Table `project1`.`query3_benchmark`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS project1.query3_benchmark (
  algoritmo VARCHAR(32) NOT NULL,
  k INT NOT NULL,
  "costo addestramento (ms)" INT NULL DEFAULT NULL,
  wssse DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (algoritmo, k));



-- -----------------------------------------------------
-- Table `project1`.`query3_result`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS project1.query3_result (
  algoritmo VARCHAR(32) NOT NULL,
  k INT NOT NULL,
  regione VARCHAR(32) NOT NULL,
  "stima percentuale popolazione vaccinata" DOUBLE PRECISION NULL DEFAULT NULL,
  "stima numero vaccinazioni" INT NULL DEFAULT NULL,
  "cluster" VARCHAR(45) NULL DEFAULT NULL,
  PRIMARY KEY (algoritmo, k, regione));

