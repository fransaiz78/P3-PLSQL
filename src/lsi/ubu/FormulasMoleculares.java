package lsi.ubu;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsi.ubu.util.*;

/**
 * Clase en el que se realizan las transaciones.
 * 
 * @author Francisco Saiz Güemes - Mario Santamaria Arias.
 *
 */
public class FormulasMoleculares {

	/**
	 * Pool de conexiones.
	 */
	private static PoolDeConexiones pool;

	/**
	 * logger.
	 */
	private static Logger logger;

	/**
	 * Main donde se realizaran las inicializaciones y la bateria de pruebas.
	 * 
	 * @param args
	 *            argumentos
	 */
	public static void main(String[] args) {

		try {
			inicializaciones();
		} catch (NamingException | SQLException | IOException e1) {
			e1.printStackTrace();
		}

		bateriaPruebas();
		
		
		System.out.println("\n-----------------------------------------------------------------");
		System.out.println("    El tratamiento de pruebas se ha realizado correctamente.     \n");
	}

	// Nº 1
	/**
	 * Transacion nº1. Insertar molecula.
	 * 
	 * @param nombre
	 *            String que representa el nombre
	 * @param simbolos
	 *            Array de String que contiene los simbolos
	 * @param nros
	 *            Array de int que contiene los nros
	 * @throws ChemistryException
	 *             Excepcion
	 */
	public static void insertarMolecula(String nombre, String[] simbolos, int[] nros) throws ChemistryException {

		Connection con = null;

		PreparedStatement pst = null;
		PreparedStatement pst2 = null;
		PreparedStatement pst3 = null;
		PreparedStatement pstExc = null;
		PreparedStatement pstExc2 = null;

		ResultSet rs = null;
		ResultSet rsExc = null;
		ResultSet rsExc2 = null;

		String form = "";
		int pesoTotal = 0;
		int insertados = 0;

		try {
			con = pool.getConnection();

			// Comprobamos si ambos arrays son del mismo tamaño.
			if (simbolos.length != nros.length) {
				pool.undo(con);
				throw (new ChemistryException(ChemistryError.TAMAÑOS_INADECUADOS));
			}

			// Comprobamos si el nombre de la molecula a insertar ya existe.
			pstExc = con.prepareStatement("SELECT * FROM Moleculas WHERE nombre=?");
			pstExc.setString(1, nombre);
			rsExc = pstExc.executeQuery();

			if (rsExc.next()) {
				pool.undo(con);
				throw (new ChemistryException(ChemistryError.NOMBRE_DE_MOLECULA_YA_EXISTENTE));
			}

			// Calcular formula y sacar pesos.
			pst = con.prepareStatement("SELECT pesoAtomico FROM Elementos WHERE simbolo=?");

			for (int i = 0; i < simbolos.length; i++) {
				form = form.concat(simbolos[i]);
				if (nros[i] > 1) {
					form = form.concat("" + nros[i]);
				}

				// Sacamos el peso de ese simbolo y nos creamos un array
				// auxiliar de los pesos.
				pst.setString(1, simbolos[i]);
				rs = pst.executeQuery();

				if (rs.next()) {
					pesoTotal += rs.getInt(1) * nros[i];
				} else {
					pool.undo(con);
					pool.close(con);
					pool.close(pst);
					pool.close(pst2);
					pool.close(rs);
					throw (new ChemistryException(ChemistryError.NO_EXISTE_ATOMO));
				}

			}

			// Comprobar si la formula ya existe.
			pstExc2 = con.prepareStatement("Select * FROM Moleculas Where formula=?");
			pstExc2.setString(1, form);
			rsExc2 = pstExc2.executeQuery();
			if (rsExc2.next()) {
				pool.undo(con);
				throw (new ChemistryException(ChemistryError.FORMULA_YA_EXISTENTE));
			}
			// Insertar molecula en Moleculas:
			pst2 = con.prepareStatement(
					"INSERT INTO Moleculas(id, nombre, pesoMolecular, formula) values(seq_molId.nextval, ?, ?, ?)");
			pst2.setString(1, nombre);
			pst2.setInt(2, pesoTotal);
			pst2.setString(3, form);
			insertados = pst2.executeUpdate();

			pst3 = con.prepareStatement(
					"INSERT INTO Composicion(simbolo, idMolecula, nroAtomos) values(?, seq_molId.currval, ?)");
			// Relleno Composicion
			for (int i = 0; i < simbolos.length; i++) {

				pst3.setString(1, simbolos[i]);
				pst3.setInt(2, nros[i]);
				insertados += pst3.executeUpdate();
			}

			if (insertados == 3) {
				logger.info("La transacion ha ido bien.");
				con.commit();
			} else {
			}

		} catch (SQLException e) {
			logger.error("La transacion hay que deshacerla.");
			pool.undo(con);
			logger.error(e.getLocalizedMessage());
			e.printStackTrace();

		} finally {
			logger.debug("Cerrando recursos");
			pool.close(con);
			pool.close(pst);
			pool.close(pst2);
			pool.close(pstExc);
			pool.close(pstExc2);
			pool.close(rs);
			pool.close(rsExc);
			pool.close(rsExc2);
		}
	}

	/**
	 * Transacion nº2. Borrar molecula mediante el nombre.
	 * 
	 * @param nombreMol
	 *            String que representa el nombre
	 * @throws ChemistryException
	 *             Excepcion
	 */
	public static void borrarMolecula(String nombreMol) throws ChemistryException {
		Connection con = null;
		CallableStatement cst = null;
		
		try {
			con = pool.getConnection();
			cst = con.prepareCall("{ call borrarMolecula(?) }");
			cst.setString(1, nombreMol);
			
			cst.execute();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Transacion nº3. Actualiza molecula mediante el nombre.
	 * 
	 * @param nombreMol
	 *            String que representa el nombre de la molecula
	 * @param simbolo
	 *            String que representa el simbolo
	 * @param nro
	 *            Entero que representa el nro
	 * @throws ChemistryException
	 *             Excepcion
	 */
	public static void actualizarMolecula(String nombreMol, String simbolo, int nro) throws ChemistryException {
		Connection con = null;

		PreparedStatement pstId = null;
		ResultSet rsId = null;

		int idMol = 0;

		try {
			con = pool.getConnection();
			// Obtenemos el id correspondiente a la molecula de ese nombre.
			pstId = con.prepareStatement("SELECT id FROM MOLECULAS WHERE nombre=?");
			pstId.setString(1, nombreMol);
			rsId = pstId.executeQuery();
			if (rsId.next()) {
				idMol = rsId.getInt(1);
				actualizarMolecula(idMol, simbolo, nro);
			} else {
				pool.undo(con);
				throw (new ChemistryException(ChemistryError.NO_EXISTE_MOLECULA));
			}

		} catch (SQLException e) {
			logger.error("La transacion hay que deshacerla.");
			pool.undo(con);
			logger.error(e.getLocalizedMessage());
			e.printStackTrace();

		} finally {
			logger.debug("Cerrando recursos");
			pool.close(con);
			pool.close(pstId);
			pool.close(rsId);
		}
	}

	/**
	 * Transacion nº4. Borrar molecula mediante el id.
	 * 
	 * @param id
	 *            Entero que representa el id
	 * @throws ChemistryException
	 *             Excepcion
	 */
	public static void borrarMolecula(int id) throws ChemistryException {
		Connection con = null;
		CallableStatement cst = null;
		
		try {
			con = pool.getConnection();
			cst = con.prepareCall("{ call borrarMolecula(?) }");
			cst.setInt(1, id);
			
			cst.execute();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Transacion nº5. Actualizar molecula mediante id.
	 * 
	 * @param id
	 *            Entero que representa el id
	 * @param simbolo
	 *            String que representa el simbolo
	 * @param nro
	 *            Entero que representa el nro
	 * 
	 * @throws ChemistryException
	 *             Excepcion
	 */
	public static void actualizarMolecula(int id, String simbolo, int nro) throws ChemistryException {

		Connection con = null;
		CallableStatement cst = null;
		
		try {
			con = pool.getConnection();
			cst = con.prepareCall("{ call actualizarMolecula(?,?,?) }");
			cst.setInt(1, id);
			cst.setString(2, simbolo);
			cst.setInt(3, nro);
			
			cst.execute();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Metodo en el que se realiza el tratamiento de las excepciones. Se recogen
	 * aqui y comprobamos si el error es el que corresponde.
	 * 
	 * Realizaremos las comprobaciones en el siguiente orde: Insertar //
	 * Actualizar // Borrar
	 */
	public static void bateriaPruebas() {

		System.out.println("->  Script cargado con la molecula: H2O.\n\n");
		

		System.out.println("\n->  Molecula existente: H2O\n\n");

		Connection con = null;

		Statement st = null;
		PreparedStatement pst = null;

		ResultSet rs = null;
		ResultSet rs1 = null;
		
		
		System.out.println("\n->  Moleculas existentes: H2O - H2O2 \n\n");
		
		System.out.println("\n-----------------------------------------------------------------");
		System.out.println("         - Bateria de pruebas para el caso de ACTUALIZAR -         ");
		System.out.println("-----------------------------------------------------------------\n");
		
		try {
			actualizarMolecula(1, "H", 4);

			con = pool.getConnection();
			st = con.createStatement();
			rs = st.executeQuery("SELECT * FROM Composicion where simbolo='H' AND idMolecula=1 AND nroAtomos=4");
			if (rs.next()) {
				System.out.println("ActualizarMolecula mediante Id se ha realizado con éxito.");
			} else {
				System.out.println("ActualizarMolecula mediante Id ·NO· se ha realizado con éxito.");
			}

		} catch (ChemistryException e) {
			if (e.getError() == ChemistryError.NO_EXISTE_MOLECULA) {
				System.out.println("ActualizarMolecula mediante Id. OK. ");
			} else {
				System.out.println("ActualizarMolecula mediante Id. MAL. ");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			pool.close(st);
			pool.close(rs);
			pool.close(con);
		}

		System.out.println("\n-----------------------------------------------------------------");
		System.out.println("           - Bateria de pruebas para el caso de BORRAR -           ");
		System.out.println("-----------------------------------------------------------------\n");

		
//		try {
//			borrarMolecula(1);
//
//			con = pool.getConnection();
//			st = con.createStatement();
//			rs = st.executeQuery("SELECT * FROM Moleculas where id=1");
//			if (!rs.next()) {
//				System.out.println("Borrar molecula mediante Id se ha realizado con éxito.");
//			} else {
//				System.out.println("Borrar molecula mediante Id ·NO· se ha realizado con éxito.");
//
//			}
//
//		} catch (ChemistryException e) {
//			if (e.getError() == ChemistryError.NO_EXISTE_MOLECULA) {
//				System.out.println("Borrar molecula mediante Id si no existe. OK. ");
//			} else {
//				System.out.println("Borrar molecula mediante Id si no existe. MAL.		 ");
//			}
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} finally {
//			pool.close(st);
//			pool.close(rs);
//			pool.close(con);
//		}
		
		
//		try {
//			borrarMolecula("Agua");
//
//			con = pool.getConnection();
//			st = con.createStatement();
//			rs = st.executeQuery("SELECT * FROM Moleculas where nombre='Agua'");
//			if (!rs.next()) {
//				System.out.println("Borrar molecula mediante Nombre se ha realizado con éxito.");
//			} else {
//				System.out.println("Borrar molecula mediante Nombre ·NO· se ha realizado con éxito.");
//			}
//
//		} catch (ChemistryException e) {
//			if (e.getError() == ChemistryError.NO_EXISTE_MOLECULA) {
//				System.out.println("Borrar molecula mediante Nombre si no existe. OK. ");
//			} else {
//				System.out.println("Borrar molecula mediante Nombre si no existe. MAL. ");
//			}
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} finally {
//			pool.close(st);
//			pool.close(rs);
//			pool.close(con);
//		}
		
	}

	/**
	 * Metodo estatico llamado inicializaciones. Aqui se recoge la instancia del
	 * pool de conexiones Obtenemos el logger y lanzamos la ejecucion de nuestro
	 * script sql.
	 * 
	 * @throws NamingException
	 *             Excepcion
	 * @throws SQLException
	 *             Excepcion
	 * @throws IOException
	 *             Excepcion
	 */
	public static void inicializaciones() throws NamingException, SQLException, IOException {
		// Obtenemos una instancia del Pool
		pool = PoolDeConexiones.getInstance();

		// Obtenemos el Logger
		logger = LoggerFactory.getLogger(FormulasMoleculares.class);

		logger.info("Comienzo Ejecución");

		//Cargamos el script.
		System.out.println("Cargando de nuevo el Script...");
		ExecuteScript.run("./sql/sp-formulas.sql");
	}

}
