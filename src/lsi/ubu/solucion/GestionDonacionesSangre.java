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
		PreparedStatement stDonante = null;
		PreparedStatement stHospital = null;
		PreparedStatement stUltimaDonacion = null;
		PreparedStatement stInsertDonacion = null;
		PreparedStatement stUpdateReserva = null;
		ResultSet rsDonante = null;
		ResultSet rsHospital = null;
		ResultSet rsUltimaDonacion = null;		

	
		try{
			con = pool.getConnection();
			//Comprobar que la cantidad está entre los valores correctos
			if(m_Cantidad <= 0 || m_Cantidad > 0.45) {
				throw new GestionDonacionesSangreException(GestionDonacionesSangreException.VALOR_CANTIDAD_DONACION_INCORRECTO);

			}
			
			//Comprobar que el donante existe y se obtiene su tipo de sangre
			stDonante = con.prepareStatement("select id_tipo_sangre from donante where nif = ?");
			stDonante.setString(1, m_NIF);
			rsDonante = stDonante.executeQuery();
			if(!rsDonante.next()) {
				throw new GestionDonacionesSangreException(GestionDonacionesSangreException.DONANTE_NO_EXISTE);
			}
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
			
			
		} catch (SQLException e) {
			//Completar por el alumno			
			
			logger.error(e.getMessage());
			throw e;		

		} finally {
			/*A rellenar por el alumno*/
		}
		
		
	}
	
	public static void anular_traspaso(int m_ID_Tipo_Sangre, int m_ID_Hospital_Origen,int m_ID_Hospital_Destino,
			Date m_Fecha_Traspaso)
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
