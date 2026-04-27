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
				throw e;
			}
		
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
	public static void consulta_traspasos(String m_Tipo_Sangre)
			throws SQLException {

				
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con=null;

	
		try{
			con = pool.getConnection();
			//Completar por el alumno
			
		} catch (SQLException e) {
			//Completar por el alumno			
			
			logger.error(e.getMessage());
			throw e;		

		} finally {
			/*A rellenar por el alumno*/
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
		
	}
}
