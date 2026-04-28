package lsi.ubu.solucion;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsi.ubu.servicios.GestionDonacionesSangreException;
import lsi.ubu.util.ExecuteScript;
import lsi.ubu.util.PoolDeConexiones;


/**
 * GestionDonacionesSangre:
 * Implementa la gestion de donaciones de sangre según el enunciado del ejercicio
 * 
 * @author <a href="mailto:jmaudes@ubu.es">Jesus Maudes</a>
 * @author <a href="mailto:rmartico@ubu.es">Raul Marticorena</a>
 * @author <a href="mailto:pgdiaz@ubu.es">Pablo Garcia</a>
 * @author <a href="mailto:srarribas@ubu.es">Sandra Rodriguez</a>
 * @version 1.5
 * @since 1.0 
 */
public class GestionDonacionesSangre {
	
	private static Logger logger = LoggerFactory.getLogger(GestionDonacionesSangre.class);

	private static final String script_path = "sql/";

	public static void main(String[] args) throws SQLException{		
		tests();

		System.out.println("FIN.............");
	}
	
	public static void realizar_donacion(String m_NIF, int m_ID_Hospital,
			float m_Cantidad,  Date m_Fecha_Donacion) throws SQLException {
		
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con=null;
		
		//Sentencias preparadas para las operaciones de sql
		PreparedStatement stDonante = null; //Buscaar donante por nif
		PreparedStatement stHospital = null; //Buscar reserva del hospital
		PreparedStatement stUltimaDonacion = null; //Comprobar donaciones recientes
		PreparedStatement stInsertDonacion = null; //Insertar nueva donacion
		PreparedStatement stUpdateReserva = null; //Actualizar reserva hospital
		
		//ResultSets para guardar los resultados de las consultas
		ResultSet rsDonante = null;
		ResultSet rsHospital = null;
		ResultSet rsUltimaDonacion = null;		

		try{
			con = pool.getConnection();
			
			//Comprobar que la cantidad está entre 0 y 0.45
			if(m_Cantidad <= 0 || m_Cantidad > 0.45) {
				throw new GestionDonacionesSangreException(GestionDonacionesSangreException.VALOR_CANTIDAD_DONACION_INCORRECTO);

			}
			
			//Comprobar que el donante existe y obtener su tipo de sangre para poder después actualizar la reserva del hospital
			stDonante = con.prepareStatement("select id_tipo_sangre from donante where nif = ?");
			stDonante.setString(1, m_NIF);
			rsDonante = stDonante.executeQuery();
		
			//Si no hay nignuna fila, el donante no existe
			if(!rsDonante.next()) {
				throw new GestionDonacionesSangreException(GestionDonacionesSangreException.DONANTE_NO_EXISTE);
			}
			//Guardamos el tipo de sangre del donante para usarlo después
			int idTipoSangre = rsDonante.getInt("id_tipo_sangre");
			
			//Comprobar que existe el hospital y tiene reserva de ese tipo de sangre
			stHospital = con.prepareStatement("select cantidad from reserva_hospital " +
					"where id_hospital = ? and id_tipo_sangre = ?");
			stHospital.setInt(1, m_ID_Hospital);
			stHospital.setInt(2,  idTipoSangre);
			rsHospital = stHospital.executeQuery();
			
			if(!rsHospital.next()) {
				throw new GestionDonacionesSangreException(GestionDonacionesSangreException.HOSPITAL_NO_EXISTE);
			
			}
			
			//Comprobar que la ultima donacion ha sido hace mas de 15 dias
			//Se cuentan cuantas donaciones tiene el donante en los ultimos 15 dias con respecto a la fecha pasada por parámetro
			stUltimaDonacion = con.prepareStatement("select count (*) from donacion where nif_donante = ?" +
					"and fecha_donacion > ? - 15");
			stUltimaDonacion.setString(1, m_NIF);
			stUltimaDonacion.setDate(2,  new java.sql.Date(m_Fecha_Donacion.getTime()));//Conversion de fecha a sql.date
			rsUltimaDonacion = stUltimaDonacion.executeQuery();
			rsUltimaDonacion.next();
			
			if(rsUltimaDonacion.getInt(1) > 0) { //Si ha donado en los ultimos 15 dias
				throw new GestionDonacionesSangreException(GestionDonacionesSangreException.DONANTE_EXCEDE);
			}
			
			//Insertar la donación
			//Usamos la secuencia seq_donacion para generar el id y lo insertamos junto con los demas campos
			stInsertDonacion = con.prepareStatement("insert into donacion values (seq_donacion.nextval, ?, ?, ?)");
			stInsertDonacion.setString(1, m_NIF);
			stInsertDonacion.setFloat(2,  m_Cantidad);
			stInsertDonacion.setDate(3,  new java.sql.Date(m_Fecha_Donacion.getTime()));
			stInsertDonacion.executeUpdate();
			
			//Actualizar la reserva del hospital
			//Incrementando la cantidad de sangre en el hospital a la reserva actual, filtrando por hospital
			//y tipo de sangre
			stUpdateReserva = con.prepareStatement("update reserva_hospital set cantidad = cantidad + ? " +
			"where id_hospital = ? and id_tipo_sangre = ?");
			stUpdateReserva.setFloat(1,  m_Cantidad);
			stUpdateReserva.setInt(2,  m_ID_Hospital);
			stUpdateReserva.setInt(3,  idTipoSangre);
			stUpdateReserva.executeUpdate();
			
			con.commit(); //Commit de los cambios si no ha habido excepciones
			
			//Excepción, se revierten los cambios y se relanza para que el método que hizo la llamada pueda tratarla
		} catch (GestionDonacionesSangreException e) {
			if (con != null) {
				con.rollback();
			}
			throw e;
		
			//Si salta excepción de SQL se revierten los cambios, se registra en el logger y se relanza
		} catch (SQLException e) {
			if (con != null) {
				con.rollback();
			}
			logger.error(e.getMessage());
			throw e;		

			//Bloque finally, que se ejecuta siempre
			//Cierre de todos los recursos
		} finally {
			if(stDonante != null) {
				stDonante.close();
			}
			if(stHospital != null) {
				stHospital.close();
			}
			if(stUltimaDonacion != null) {
				stUltimaDonacion.close();
			}
			if(stInsertDonacion != null) {
				stInsertDonacion.close();
			}
			if(stUpdateReserva != null) {
				stUpdateReserva.close();
			}
			if(rsDonante != null) {
				rsDonante.close();
			}
			if(rsHospital != null) {
				rsHospital.close();
			}			
			if(rsUltimaDonacion != null) {
				rsUltimaDonacion.close();
			}
			
			if(con != null) {
				con.close();
			}
		}	
	}
	
	public static void anular_traspaso(int m_ID_Tipo_Sangre, int m_ID_Hospital_Origen,int m_ID_Hospital_Destino,
			Date m_Fecha_Traspaso)
			throws SQLException {
		
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con=null;
		

		PreparedStatement stSelectTraspasos = null;
		PreparedStatement stUpdateOrigen = null;
		PreparedStatement stUpdateDestino = null;
		PreparedStatement stDeleteTraspasos = null;
		ResultSet rsTraspasos = null;
	
		try{
			con = pool.getConnection();
			//Buscamos todos los traspasos que coincidan con los parametros samos trunc para que no afecte la hora al comparar fechas
			stSelectTraspasos = con.prepareStatement(
		         "select id_traspaso, cantidad from traspaso " +
		         "where id_tipo_sangre = ? " +
		         "and id_hospital_origen = ? " +
		         "and id_hospital_destino = ? " +
		         "and trunc(fecha_traspaso) = trunc(?)");
		     stSelectTraspasos.setInt(1, m_ID_Tipo_Sangre);
		     stSelectTraspasos.setInt(2, m_ID_Hospital_Origen);
		     stSelectTraspasos.setInt(3, m_ID_Hospital_Destino);
		     stSelectTraspasos.setDate(4, new java.sql.Date(m_Fecha_Traspaso.getTime()));
		     rsTraspasos = stSelectTraspasos.executeQuery();
		        
		   //Preparamos las sentencias para actualizar las reservas de ambos hospitales
		     
		     stUpdateOrigen = con.prepareStatement(
		             "update reserva_hospital set cantidad = cantidad + ? " +
		             "where id_hospital = ? and id_tipo_sangre = ?");
		     
		     stUpdateDestino = con.prepareStatement(
		             "update reserva_hospital set cantidad = cantidad - ? " +
		             "where id_hospital = ? and id_tipo_sangre = ?");
		     
		   //Recorremos los traspasos encontrados y actualizamos las reservas
		     while (rsTraspasos.next()) {
		        float cantidad = rsTraspasos.getFloat("cantidad");
		        	
		    //Si la cantidad es negativa lanzamos excepcion
		        
		            if (cantidad < 0) {
		                throw new GestionDonacionesSangreException(
		                    GestionDonacionesSangreException.VALOR_CANTIDAD_TRASPASO_INCORRECTO);
		            }

		    //El origen recupera la sangre que habia enviado
		            
		      stUpdateOrigen.setFloat(1, cantidad);
		      stUpdateOrigen.setInt(2, m_ID_Hospital_Origen);
		      stUpdateOrigen.setInt(3, m_ID_Tipo_Sangre);
		      stUpdateOrigen.executeUpdate();

		    //El destino devuelve la sangre que habia recibido
		      stUpdateDestino.setFloat(1, cantidad);
		      stUpdateDestino.setInt(2, m_ID_Hospital_Destino);
		      stUpdateDestino.setInt(3, m_ID_Tipo_Sangre);
		      stUpdateDestino.executeUpdate();
		    }

		     
		   //Borramos los traspasos una vez actualizadas las reservas
		        stDeleteTraspasos = con.prepareStatement(
		            "delete from traspaso " +
		            "where id_tipo_sangre = ? " +
		            "and id_hospital_origen = ? " +
		            "and id_hospital_destino = ? " +
		            "and trunc(fecha_traspaso) = trunc(?)");
		        stDeleteTraspasos.setInt(1, m_ID_Tipo_Sangre);
		        stDeleteTraspasos.setInt(2, m_ID_Hospital_Origen);
		        stDeleteTraspasos.setInt(3, m_ID_Hospital_Destino);
		        stDeleteTraspasos.setDate(4, new java.sql.Date(m_Fecha_Traspaso.getTime()));
		        stDeleteTraspasos.executeUpdate();

		        con.commit();
		        
		} catch (GestionDonacionesSangreException e) {
			if (con != null) {
	            con.rollback();
	        }
	        throw e;
	        
		} catch (SQLException e) {
			//Completar por el alumno			
			if (con != null) {
	            con.rollback();
	        }
			logger.error(e.getMessage());
			throw e;		

		} finally {
			if (stSelectTraspasos != null) {
	            stSelectTraspasos.close();
	        }
	        if (stUpdateOrigen != null) {
	            stUpdateOrigen.close();
	        }
	        if (stUpdateDestino != null) {
	            stUpdateDestino.close();
	        }
	        if (stDeleteTraspasos != null) {
	            stDeleteTraspasos.close();
	        }
	        if (rsTraspasos != null) {
	            rsTraspasos.close();
	        }
	        if (con != null) {
	            con.close();
	        }
	}
	}

	/**
	 * consulta_traspasos:
	 * Muestra por logger todos los traspasos de un tipo de sangre concreto.
	 * Para cada traspaso imprime: hospital origen, hospital destino, cantidad y fecha.
	 * Si el tipo de sangre no existe lanza TIPO_SANGRE_NO_EXISTE.
	 */
	public static void consulta_traspasos(String m_Tipo_Sangre)
			throws SQLException {

		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con = null;

		PreparedStatement stTipoSangre = null; // Buscar el tipo de sangre por descripción
		PreparedStatement stTraspasos = null;  // Consultar traspasos de ese tipo de sangre
		ResultSet rsTipoSangre = null;
		ResultSet rsTraspasos = null;

		try {
			con = pool.getConnection();

			// Buscamos el id del tipo de sangre a partir de su descripción
			stTipoSangre = con.prepareStatement(
				"select id_tipo_sangre from tipo_sangre where descripcion = ?");
			stTipoSangre.setString(1, m_Tipo_Sangre);
			rsTipoSangre = stTipoSangre.executeQuery();

			// Si no existe el tipo de sangre, lanzamos excepción
			if (!rsTipoSangre.next()) {
				throw new GestionDonacionesSangreException(
					GestionDonacionesSangreException.TIPO_SANGRE_NO_EXISTE);
			}

			int idTipoSangre = rsTipoSangre.getInt("id_tipo_sangre");

			// Consultamos todos los traspasos del tipo de sangre dado,
			// haciendo join con hospital para obtener los nombres de origen y destino
			stTraspasos = con.prepareStatement(
				"select ho.nombre as hospital_origen, " +
				"       hd.nombre as hospital_destino, " +
				"       t.cantidad, " +
				"       t.fecha_traspaso " +
				"from traspaso t " +
				"join hospital ho on t.id_hospital_origen  = ho.id_hospital " +
				"join hospital hd on t.id_hospital_destino = hd.id_hospital " +
				"where t.id_tipo_sangre = ? " +
				"order by t.fecha_traspaso");
			stTraspasos.setInt(1, idTipoSangre);
			rsTraspasos = stTraspasos.executeQuery();

			// Recorremos e imprimimos cada traspaso encontrado
			logger.info("Traspasos del tipo de sangre: " + m_Tipo_Sangre);
			boolean hayTraspasos = false;
			while (rsTraspasos.next()) {
				hayTraspasos = true;
				String origen   = rsTraspasos.getString("hospital_origen");
				String destino  = rsTraspasos.getString("hospital_destino");
				float cantidad  = rsTraspasos.getFloat("cantidad");
				java.sql.Date fecha = rsTraspasos.getDate("fecha_traspaso");
				logger.info("  Origen: " + origen +
							" | Destino: " + destino +
							" | Cantidad: " + cantidad +
							" | Fecha: " + fecha);
			}

			// Si no hay traspasos para ese tipo, lo indicamos
			if (!hayTraspasos) {
				logger.info("  No hay traspasos registrados para este tipo de sangre.");
			}

		} catch (GestionDonacionesSangreException e) {
			// Error de negocio: tipo de sangre no existe
			if (con != null) {
				con.rollback();
			}
			throw e;

		} catch (SQLException e) {
			// Error de base de datos: rollback, log y relanzar
			if (con != null) {
				con.rollback();
			}
			logger.error(e.getMessage());
			throw e;

		} finally {
			// Cerramos todos los recursos en el finally
			if (rsTipoSangre != null) rsTipoSangre.close();
			if (rsTraspasos  != null) rsTraspasos.close();
			if (stTipoSangre != null) stTipoSangre.close();
			if (stTraspasos  != null) stTraspasos.close();
			if (con != null) con.close();
		}
	}
	
	static public void creaTablas() {
		ExecuteScript.run(script_path + "gestion_donaciones_sangre.sql");
	}

	static void tests() throws SQLException{
		creaTablas();
		
		PoolDeConexiones pool = PoolDeConexiones.getInstance();		
		
		//Relatar caso por caso utilizando el siguiente procedure para inicializar los datos
		
		CallableStatement cll_reinicia=null;
		Connection conn = null;
	
		try {
			//Reinicio filas
			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
		} catch (SQLException e) {				
			logger.error(e.getMessage());			
		} finally {
			if (cll_reinicia!=null) cll_reinicia.close();
			if (conn!=null) conn.close();
		
		}			
		
		//Test realizar donacion
		logger.info("Tests realizar_donacion");
		tests_realizar_donacion();
		
		//TEst anular traspaso
		logger.info("Tests anular_traspaso");
		tests_anular_traspaso();

		// Test consulta_traspasos
		logger.info("Tests consulta_traspasos");
		tests_consulta_traspasos();
		
	}
	
	
	private static void tests_realizar_donacion() throws SQLException{
		
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		
		//Fecha del día en que se ejecutan las pruebas
		Date hoy = new Date();
		//Fecha de hace 5 dias para simular una donacion reciente
		Date hace10dias = new Date(hoy.getTime() - 10L * 24 * 60 * 60 * 1000);
		
		/*Caso1. Cantidad incorrecta: valor negativo
		 * Se espera la excepcion VALOR_CANTIDAD_DONACION_INCORRECTO
		 */
		try {
			GestionDonacionesSangre.realizar_donacion("12345678A", 1, -0.1f, hoy);
			logger.info("Caso 1 mal: No lanza excepción con cantidad negativa");
		} catch(GestionDonacionesSangreException e) {
			if (e.getErrorCode() == GestionDonacionesSangreException.VALOR_CANTIDAD_DONACION_INCORRECTO) {
				logger.info("Caso 1 ok. Detecta cantidad negativa correctamente");
			} else {
				logger.info("Caso 1 mal. Laza excepción incorrecta, código: " + e.getErrorCode());
			}
		}
		
		/*Caso2. Cantidad incorrecta: Más del máximo de 0.45
		 * Se espera la excepción VALOR_CANTIDAD_DONACION_INCORRECTO
		 */
		try {
			GestionDonacionesSangre.realizar_donacion("12345678A", 1, 0.5f, hoy);
			logger.info("Caso 2 mal: No lanza excepción con cantidad > 0.45");
		} catch(GestionDonacionesSangreException e) {
			if (e.getErrorCode() == GestionDonacionesSangreException.VALOR_CANTIDAD_DONACION_INCORRECTO) {
				logger.info("Caso 2 ok. Detecta cantidad superior al máximo correctamente");
			} else {
				logger.info("Caso 2 mal: Lanza excepción incorrecta con código " + e.getErrorCode());
				
			}
		}
		
		/*Caso3. Donante inexistente
		 * Se utiliza un NIF que no existe en la tabla de donantes
		 * Se espera la excepción DONANTE_NO_EXISTE
		 */
		try {
			GestionDonacionesSangre.realizar_donacion(
				"00000000A", 1, 0.3f, hoy);
			logger.info("Caso 3 mal: No se lanza excepción con donante inexistente");
		} catch (GestionDonacionesSangreException e) {
			if (e.getErrorCode() == GestionDonacionesSangreException.DONANTE_NO_EXISTE) {
				logger.info("Caso 3 ok: Se detecta correctamente que el donante no existe");
			} else {
				logger.info("Caso 3 mal: Se lanza una excepcion incorrecta con código: "+ e.getErrorCode());
			}
		
		}
		
		/*Caso4. Hospital no existe
		 * Utiliza un donante válido pero un hospital que no existe
		 * Se esoera que salte la excepción HOSPITAL_NO_EXISTE
		 */
		try {
			GestionDonacionesSangre.realizar_donacion("12345678A", 1000, 0.3f, hoy);
			logger.info("Caso 4 mal: No lanza excepción con hospital inexistente");
		} catch (GestionDonacionesSangreException e) {
			if (e.getErrorCode() ==	GestionDonacionesSangreException.HOSPITAL_NO_EXISTE) {
				logger.info("Caso 4 ok: Se detecta hospital inexistente correctamente");
			} else {
				logger.info("Caso 4 mal: LAnza excepcion incorrecta, con código: "+ e.getErrorCode());
			}
		}
		
		/*Caso 5: Donante que ha donado hace menos de 15 dias
		 * El donante con DNI 12345678A tiene donaciones el 10/01 y 15/01
		 * usamos hace10dias para simular que intenta donar demasiado pronto
		 * Se espera la excepción DONANTE_EXCEDE
		 */
		try {
			GestionDonacionesSangre.realizar_donacion("12345678A", 1, 0.3f, hace10dias);
			logger.info("Caso 5 mal: No lanza excepción con donación hace menos de 15 días");
		} catch (GestionDonacionesSangreException e) {
			if (e.getErrorCode() == GestionDonacionesSangreException.DONANTE_EXCEDE) {
				logger.info("Caso 5 ok: Detecta donacion de hace menos de 15 dias correctamente");
			} else {
				logger.info("Caso 5 mal: Se lanza una excepción incorrecta con código: "+ e.getErrorCode());
			}
		}

		/*Caso 6: Donación correcta
		 * Donante 98989898C (tipo O) en el hospital 1 que tiene reserva de tipo O
		 * Comprobamos que se inserta la donación y se actualiza la reserva del hospital sumando
		 * la cantidad donada a la reserva existente
		*/
		try {
			// Guardamos la reserva actual antes de donar
			con = pool.getConnection();
			st = con.prepareStatement("select cantidad from reserva_hospital where id_hospital = 1 and id_tipo_sangre = "+
				"(select id_tipo_sangre from donante where nif = '98989898C')");
			rs = st.executeQuery();
			rs.next();
			float reservaAntes = rs.getFloat("cantidad");
			rs.close();
			st.close();
			con.close();

			//Realizamos la donacion
			GestionDonacionesSangre.realizar_donacion("98989898C", 1, 0.3f, hoy);

			// Comprobamos que la reserva ha aumentado 0.3
			con = pool.getConnection();
			st = con.prepareStatement("select cantidad from reserva_hospital where id_hospital = 1 and id_tipo_sangre = " +
				"(select id_tipo_sangre from donante where nif = '98989898C')");
			rs = st.executeQuery();
			rs.next();
			float reservaDespues = rs.getFloat("cantidad");

			//Comprobamos si se ha actualizado bien la cantidad en resserva
			if (Math.abs((reservaDespues - reservaAntes) - 0.3f) < 0.001f) { //Tenemos en cuenta el posible margen de error de float
				logger.info("Caso 6 ok: Donación correcta");
			} else {
				logger.info("Caso 6 mal: La reserva no se ha actualizado correctamente");
			}

		} catch (GestionDonacionesSangreException e) {
			logger.info("Caso 6 mal: Se lanza excepcion inesperada, codigo: "+ e.getErrorCode());
		} finally {
			if (rs != null) rs.close();
			if (st != null) st.close();
			if (con != null) con.close();
		}
	}
	
	/**
	 * Tests para anular_traspaso.
	 * Comprobamos los casos de traspaso correcto, traspaso inexistente
	 * y fecha sin traspasos registrados.
	 */
	
	private static void tests_anular_traspaso() throws SQLException {

	    PoolDeConexiones pool = PoolDeConexiones.getInstance();
	    Connection con = null;
	    PreparedStatement st = null;
	    ResultSet rs = null;

	    java.sql.Date fechaTraspaso = java.sql.Date.valueOf("2025-01-11");
	    java.sql.Date fechaTraspaso2 = java.sql.Date.valueOf("2025-01-16");

	    /*Caso 1: Anular traspaso correcto
	     * Tipo sangre 1, origen hospital 1, destino hospital 2, fecha 11/01/2025
	     * Comprobamos que las reservas se actualizan correctamente
	     */
	    try {
	        con = pool.getConnection();
	        st = con.prepareStatement(
	            "select cantidad from reserva_hospital " +
	            "where id_hospital = 1 and id_tipo_sangre = 1");
	        rs = st.executeQuery();
	        rs.next();
	        float reservaOrigenAntes = rs.getFloat("cantidad");
	        rs.close();
	        st.close();

	        st = con.prepareStatement(
	            "select cantidad from reserva_hospital " +
	            "where id_hospital = 2 and id_tipo_sangre = 1");
	        rs = st.executeQuery();
	        rs.next();
	        float reservaDestinoAntes = rs.getFloat("cantidad");
	        rs.close();
	        st.close();
	        con.close();

	        GestionDonacionesSangre.anular_traspaso(1, 1, 2, fechaTraspaso);

	        con = pool.getConnection();
	        st = con.prepareStatement(
	            "select cantidad from reserva_hospital " +
	            "where id_hospital = 1 and id_tipo_sangre = 1");
	        rs = st.executeQuery();
	        rs.next();
	        float reservaOrigenDespues = rs.getFloat("cantidad");
	        rs.close();
	        st.close();

	        st = con.prepareStatement(
	            "select cantidad from reserva_hospital " +
	            "where id_hospital = 2 and id_tipo_sangre = 1");
	        rs = st.executeQuery();
	        rs.next();
	        float reservaDestinoDespues = rs.getFloat("cantidad");

	        if (reservaOrigenDespues > reservaOrigenAntes &&
	                reservaDestinoDespues < reservaDestinoAntes) {
	            logger.info("Caso 1 ok: Traspaso anulado y reservas actualizadas correctamente");
	        } else {
	            logger.info("Caso 1 mal: Las reservas no se actualizaron correctamente");
	        }

	    } catch (GestionDonacionesSangreException e) {
	        logger.info("Caso 1 mal: Lanza excepcion inesperada, codigo: " + e.getErrorCode());
	    } finally {
	        if (rs != null) rs.close();
	        if (st != null) st.close();
	        if (con != null) con.close();
	    }

	    /*Caso 2: Anular traspaso que no existe
	     * Usamos parametros que no corresponden a ningun traspaso
	     * No debe lanzar excepcion pero tampoco modificar nada
	     */
	    try {
	        GestionDonacionesSangre.anular_traspaso(99, 99, 99, fechaTraspaso);
	        logger.info("Caso 2 ok: Anular traspaso inexistente no lanza excepcion");
	    } catch (GestionDonacionesSangreException e) {
	        logger.info("Caso 2 mal: Lanza excepcion inesperada, codigo: " + e.getErrorCode());
	    }

	    /*Caso 3: Anular traspaso de tipo sangre 2, origen hospital 1, destino hospital 2
	     * Existe en los datos de prueba con fecha 11/01/2025
	     */
	    try {
	        GestionDonacionesSangre.anular_traspaso(2, 1, 2, fechaTraspaso);
	        logger.info("Caso 3 ok: Traspaso de tipo sangre 2 anulado correctamente");
	    } catch (GestionDonacionesSangreException e) {
	        logger.info("Caso 3 mal: Lanza excepcion inesperada, codigo: " + e.getErrorCode());
	    }

	    /*Caso 4: Fecha que no coincide con ningun traspaso
	     * No debe lanzar excepcion pero tampoco modificar nada
	     */
	    try {
	        GestionDonacionesSangre.anular_traspaso(1, 1, 2,
	            java.sql.Date.valueOf("2024-01-01"));
	        logger.info("Caso 4 ok: Fecha sin traspasos no lanza excepcion");
	    } catch (GestionDonacionesSangreException e) {
	        logger.info("Caso 4 mal: Lanza excepcion inesperada, codigo: " + e.getErrorCode());
	    }

	    /*Caso 5: Anular traspaso origen hospital 3, destino hospital 2, fecha 16/01/2025
	     * Existe en los datos de prueba con cantidad 10
	     */
	    try {
	        GestionDonacionesSangre.anular_traspaso(2, 3, 2, fechaTraspaso2);
	        logger.info("Caso 5 ok: Traspaso del 16/01 anulado correctamente");
	    } catch (GestionDonacionesSangreException e) {
	        logger.info("Caso 5 mal: Lanza excepcion inesperada, codigo: " + e.getErrorCode());
	    }
	}
	
	
	
	/**
	 * Tests para consulta_traspasos.
	 * Comprobamos los casos de tipo de sangre inexistente y tipo de sangre con traspasos.
	 */
	private static void tests_consulta_traspasos() throws SQLException {

		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection conn = null;
		CallableStatement cll_reinicia = null;

		// Reiniciamos los datos antes de cada bloque de tests
		try {
			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} finally {
			if (cll_reinicia != null) cll_reinicia.close();
			if (conn != null) conn.close();
		}

		/*Caso 1: Tipo de sangre que no existe en la BD
		 * Se espera la excepción TIPO_SANGRE_NO_EXISTE
		 */
		try {
			GestionDonacionesSangre.consulta_traspasos("Tipo X.");
			logger.info("CT Caso 1 mal: No lanza excepción con tipo de sangre inexistente");
		} catch (GestionDonacionesSangreException e) {
			if (e.getErrorCode() == GestionDonacionesSangreException.TIPO_SANGRE_NO_EXISTE) {
				logger.info("CT Caso 1 ok: Detecta correctamente tipo de sangre inexistente");
			} else {
				logger.info("CT Caso 1 mal: Lanza excepción incorrecta, código: " + e.getErrorCode());
			}
		}

		/*Caso 2: Tipo de sangre que existe y tiene traspasos (Tipo A.)
		 * Según inicializa_test, hay dos traspasos de tipo A (id=1):
		 *   - Hospital 1 -> Hospital 2, cantidad 2, fecha 11/01/2025
		 *   - Hospital 2 -> Hospital 3, cantidad 1.2, fecha 16/01/2025
		 * Se espera que se muestren esos dos traspasos sin lanzar excepción
		 */
		try {
			GestionDonacionesSangre.consulta_traspasos("Tipo A.");
			logger.info("CT Caso 2 ok: consulta_traspasos ejecutada sin excepción para Tipo A.");
		} catch (GestionDonacionesSangreException e) {
			logger.info("CT Caso 2 mal: Lanza excepción inesperada, código: " + e.getErrorCode());
		}

		/*Caso 3: Tipo de sangre que existe pero NO tiene traspasos (Tipo O.)
		 * Según inicializa_test no hay traspasos de tipo O
		 * Se espera que se ejecute sin excepción y sin mostrar traspasos
		 */
		try {
			GestionDonacionesSangre.consulta_traspasos("Tipo O.");
			logger.info("CT Caso 3 ok: consulta_traspasos ejecutada sin excepción para Tipo O. (sin traspasos)");
		} catch (GestionDonacionesSangreException e) {
			logger.info("CT Caso 3 mal: Lanza excepción inesperada, código: " + e.getErrorCode());
		}

		/*Caso 4: Tipo de sangre que existe y tiene varios traspasos (Tipo B.)
		 * Según inicializa_test, hay dos traspasos de tipo B (id=2):
		 *   - Hospital 1 -> Hospital 2, cantidad 3, fecha 11/01/2025
		 *   - Hospital 3 -> Hospital 2, cantidad 10, fecha 16/01/2025
		 * Se espera que se muestren esos dos traspasos sin lanzar excepción
		 */
		try {
			GestionDonacionesSangre.consulta_traspasos("Tipo B.");
			logger.info("CT Caso 4 ok: consulta_traspasos ejecutada sin excepción para Tipo B.");
		} catch (GestionDonacionesSangreException e) {
			logger.info("CT Caso 4 mal: Lanza excepción inesperada, código: " + e.getErrorCode());
		}
	}
}
