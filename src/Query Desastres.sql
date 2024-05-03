USE DESASTRES;
GO


SELECT *
FROM Clima
SELECT *
FROM desastres
SELECT *
FROM muertes

INSERT INTO DESASTRES_BDE.dbo.DESASTRES_FINAL(
	Cuatrenio,
	Temp_AVG,
	Oxi_AVG,
	T_Tsunamis,
	T_OlasCalor,
	T_Terremotos,
	T_Erupciones,
	T_Incendios,
	M_Jovenes_AVG,
	M_Adutos_AVG,
	M_Ancianos_AVG
)
SELECT	'2023 - 2026' AS Cuatrienio,
		(MAX(Temperatura) - MIN(Temperatura)) / 4 AS Prom_Temperatura,
		(MAX(Oxigeno) - MIN(Oxigeno)) / 4 AS Prom_Oxigeno,
		SUM(D.Tsunamis) AS Tsunamis,
		SUM(D.Olas_Calor) AS Olas_Calor,
		SUM(D.Terremotos) AS Terremotos,
		SUM(D.Erupciones) AS Erupciones,
		SUM(D.Incendios) AS Incendios,
		SUM(M.R_Menor15) + SUM(M.R_15_a_30) AS MuertesJovenes,
		SUM(M.R_30_a_45) + SUM(M.R_45_a_60) AS MuertesAdultos,
		SUM(M.R_M_a_60) AS MuertesAncianos
FROM clima C
	INNER JOIN desastres D ON C.año = D.año
	INNER JOIN muertes M ON M.año = C.año
WHERE C.Año >= 2023 AND C.Año <= 2026
UNION
SELECT	'2027 - 2030' AS Cuatrienio,
		(MAX(Temperatura) - MIN(Temperatura)) / 4 AS Prom_Temperatura,
		(MAX(Oxigeno) - MIN(Oxigeno)) / 4 AS Prom_Oxigeno,
		SUM(D.Tsunamis) AS Tsunamis,
		SUM(D.Olas_Calor) AS Olas_Calor,
		SUM(D.Terremotos) AS Terremotos,
		SUM(D.Erupciones) AS Erupciones,
		SUM(D.Incendios) AS Incendios,
		SUM(M.R_Menor15) + SUM(M.R_15_a_30) AS MuertesJovenes,
		SUM(M.R_30_a_45) + SUM(M.R_45_a_60) AS MuertesAdultos,
		SUM(M.R_M_a_60) AS MuertesAncianos
FROM clima C
	INNER JOIN desastres D ON C.año = D.año
	INNER JOIN muertes M ON M.año = C.año
WHERE C.Año >= 2027 AND C.Año <= 2030

