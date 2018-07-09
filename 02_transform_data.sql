
/*UNA VEZ QUE TRATAMOS Y EXPLORAMOS UN POCO LAS FUENTES DE DATOS A UTILIZAR, 
Y YA INDEXADAS EN MYSQL, SIGUE TRANSFORMARLAS UN POCO MÁS PARA DEJARLAS LISTAS PARA LOS
CALCULOS DEL MODELO*/

/*LLAMAMOS A LA BD QUE CONTIENE LOS DATOS A TRANSFORMAR*/
use proyecto_final;

/*EMPEZAMOS CON LA DATA DEL DENUE, LA CUAL YA SOLO TIENE DATOS DE LOS ESTABLECIMIENTOS
DEL SECTOR DE ACTIVIDAD ECONOMICA 713943 */

    /*VEMOS SI LAS COLUMNAS ESTÁN CORRECTAMENTE NOMBRADAS O HAY QUE MODIFICARLAS*/
SHOW COLUMNS FROM dnue_cdmx_713943;

    /*VAMOS A MODIFICAR EL NOMBRE DE LAS COLUMNAS, HOMOLOGANDO CON LAS DE LOS CENSOS 2014*/
ALTER TABLE dnue_cdmx_713943 
	CHANGE CP cp text,
	CHANGE Calle calle text,
    CHANGE Clase_actividad actividad_economica text,
    CHANGE Colonia colonia text,
    CHANGE Correo_e correo_e text,
    CHANGE Estrato estrato text,
    CHANGE Latitud  latitud text,
    CHANGE Longitud longitud text,
    CHANGE Nombre nombre text,
    CHANGE Num_Exterior num_exterior text,
    CHANGE Num_Interior num_interior text,
    CHANGE Razon_social razon_social text,
    CHANGE Sitio_internet sitio_internet text,
    CHANGE Telefono telefono text,
    CHANGE Tipo tipo text,
    CHANGE Tipo_vialidad tipo_vialidad text,
    CHANGE Ubicacion ubicacion text,
    CHANGE Entidad entidad_federativa text,
    CHANGE Municipio municipio text;



/*SEGUIMOS CON ce_cdmx_espo*/
	
    /*VEMOS SI LAS COLUMNAS ESTÁN CORRECTAMENTE NOMBRADAS O HAY QUE MODIFICARLAS*/
SHOW COLUMNS FROM ce_cdmx_espo;

	/*ELIMINAMOS LAS COLUMNAS QUE NO NOS INTERESAN*/
ALTER TABLE ce_cdmx_espo 
	DROP COLUMN entidad_federativa0,
    DROP COLUMN tamanio_ue,
    DROP COLUMN sector,
    DROP COLUMN subsector,
    DROP COLUMN rama;

	/*NOS HACE FALTA EL CAMPO DE clave_actividad_economica POR LO QUE HAY QUE TRANSFORMAR LA COLUMNA clase*/
ALTER TABLE ce_cdmx_espo
	ADD clave_actividad_economica TEXT;

SET SQL_SAFE_UPDATES = 0;
UPDATE ce_cdmx_espo SET clave_actividad_economica = TRIM(REPLACE(clase, 'Clase', '' ));

	/*ESTA DATA NO ESTÁ FILTRADA CON EL SECTOR DE INTERÉS, POR LO QUE HAY QUE QUEDARNOS
    SOLO CON LOS REGISTROS DEL SECTOR 713943*/

DROP TABLE IF EXISTS ce_cdmx_espo_713943;
CREATE TABLE ce_cdmx_espo_713943 AS
	SELECT 	anio_censal,
			clave_entidad,
			'CIUDAD DE MEXICO' AS entidad_federativa,
			clave_actividad_economica,
            estrato,
            UE AS ue,
			H001A, H000A, H010A, H020A, I000A, J000A, K000A, M000A, 
            A111A, A121A, A131A, A221A, P000C, O020A, Q000B
    FROM ce_cdmx_espo
		WHERE clave_actividad_economica = '713943';

	/*EL CAMPO estrato ES CLAVE PARA ESTE ANÁLISIS, POR LO QUE TIENE QUE ESTAR DEFINIDO EN LOS
    MISMOS RANGOS EN LA TABLA DEL DNUE COMO EN ce_cdmx_espo_713943*/
ALTER TABLE ce_cdmx_espo_713943
	ADD estrato_dnue TEXT;

SET SQL_SAFE_UPDATES = 0;
UPDATE ce_cdmx_espo_713943 SET estrato_dnue = '0 a 5 personas' WHERE estrato = 'Hasta 2  personas';
UPDATE ce_cdmx_espo_713943 SET estrato_dnue = '0 a 5 personas' WHERE estrato = 'De      3  a       5  personas';
UPDATE ce_cdmx_espo_713943 SET estrato_dnue = '6 a 10 personas' WHERE estrato = 'De      6  a     10  personas';
UPDATE ce_cdmx_espo_713943 SET estrato_dnue = '11 a 30 personas' WHERE estrato = 'De    11  a     15  personas';
UPDATE ce_cdmx_espo_713943 SET estrato_dnue = '11 a 30 personas' WHERE estrato = 'De    16  a     20  personas';
UPDATE ce_cdmx_espo_713943 SET estrato_dnue = '11 a 30 personas' WHERE estrato = 'De    21  a     30  personas';
UPDATE ce_cdmx_espo_713943 SET estrato_dnue = '31 a 50 personas' WHERE estrato = 'De    31  a     50  personas';
UPDATE ce_cdmx_espo_713943 SET estrato_dnue = '51 a 100 personas' WHERE estrato = 'De    51  a   100  personas';
UPDATE ce_cdmx_espo_713943 SET estrato_dnue = '101 a 250 personas' WHERE estrato = 'De  101  a   250  personas';
UPDATE ce_cdmx_espo_713943 SET estrato_dnue = 'total_clase_713943' WHERE estrato = 'Clase 713943 Centros de acondicionamiento físico del sector privado';

	/*CREAMOS OTRA TABLA AGRUPANDO EL ESTRATO*/
DROP TABLE IF EXISTS ce_cdmx_espo_713943_fin;
CREATE TABLE ce_cdmx_espo_713943_fin AS
	SELECT  anio_censal,
			clave_entidad,
			entidad_federativa,
			clave_actividad_economica,
            estrato_dnue,
            SUM(ue) AS ue,
			SUM(H001A) AS H001A, SUM(H000A) AS H000A, SUM(H010A) AS H010A,
            SUM(H020A) AS H020A, SUM(I000A) AS I000A, SUM(J000A) AS J000A,
            SUM(K000A) AS K000A, SUM(M000A) AS M000A, SUM(A111A) AS A111A, 
            SUM(A121A) AS A121A, SUM(A131A) AS A131A, SUM(A221A) AS A221A,
            SUM(P000C) AS P000C, SUM(O020A) AS O020A, SUM(Q000B) AS Q000B
	FROM ce_cdmx_espo_713943 GROUP BY estrato_dnue;



/*AHORA TOCA PROCESAR ce_cdmx_mpio_713943*/

	/*EN ESTA DATA TENEMOS INFORMACIÓN DE LOS 3 ÚLTIMOS CENSOS, PERO SOLO NECESITAMOS LA DATA PARA
    EL LEVANTADO EN 2014*/
DROP TABLE IF EXISTS ce_cdmx_mpio_713943_fin;
CREATE TABLE ce_cdmx_mpio_713943_fin AS
	SELECT	anio_censal,
			clave_entidad,
            REPLACE(
				REPLACE(
					REPLACE(
						REPLACE(
							REPLACE(UPPER(entidad_federativa)
								, 'Á', 'A')
							, 'É', 'E')
						, 'Í', 'I')
					, 'Ó', 'O')
				, 'Ú', 'U') 
			AS entidad_federativa,
            clave_municipio,
            REPLACE(
				REPLACE(
					REPLACE(
						REPLACE(
							REPLACE(UPPER(municipio)
								, 'Á', 'A')
							, 'É', 'E')
						, 'Í', 'I')
					, 'Ó', 'O')
				, 'Ú', 'U') 
			AS municipio,
            clave_actividad_economica,
            UE AS ue,
            H001A, 
			H001B,
			H001C,
			K000A,
			A700A,
			J000A, 
			M000A,
			A800A     
    FROM ce_cdmx_mpio_713943
    WHERE anio_censal = '2014';

