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
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;

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
		CallableStatement cst = null;

		try {
			con = pool.getConnection();

			ArrayDescriptor des = ArrayDescriptor.createDescriptor("NESTED_TYPE", con);
			ARRAY array_to_pass_simbolos = new ARRAY(des, con, simbolos);
			ARRAY array_to_pass_nros = new ARRAY(des, con, nros);

			cst = con.prepareCall("{ call insertarMolecula(?,?,?) }");

			cst.setString(1, nombre);
			cst.setArray(2, array_to_pass_simbolos);
			cst.setArray(3, array_to_pass_nros);

			cst.execute();

		} catch (SQLException e) {
			System.out.println("------------------------------");

		} finally {
			pool.close(cst);
			pool.close(con);
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
	public static void borrarMoleculaNombre(String nombreMol) throws ChemistryException {
		Connection con = null;
		CallableStatement cst = null;

		try {
			con = pool.getConnection();
			cst = con.prepareCall("{ call borrarMoleculaNombre(?) }");
			cst.setString(1, nombreMol);

			cst.execute();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			pool.close(cst);
			pool.close(con);
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
	public static void actualizarMoleculaNombre(String nombreMol, String simbolo, int nro) throws ChemistryException {
		Connection con = null;
		CallableStatement cst = null;

		try {
			con = pool.getConnection();
			cst = con.prepareCall("{ call actualizarMoleculaNombre(?,?,?) }");
			cst.setString(1, nombreMol);
			cst.setString(2, simbolo);
			cst.setInt(3, nro);

			cst.execute();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			pool.close(cst);
			pool.close(con);
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
	public static void borrarMoleculaId(int id) throws ChemistryException {
		Connection con = null;
		CallableStatement cst = null;

		try {
			con = pool.getConnection();
			cst = con.prepareCall("{ call borrarMoleculaId(?) }");
			cst.setInt(1, id);

			cst.execute();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			pool.close(cst);
			pool.close(con);
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
	public static void actualizarMoleculaId(int id, String simbolo, int nro) throws ChemistryException {

		Connection con = null;
		CallableStatement cst = null;

		try {
			con = pool.getConnection();
			cst = con.prepareCall("{ call actualizarMoleculaId(?,?,?) }");
			cst.setInt(1, id);
			cst.setString(2, simbolo);
			cst.setInt(3, nro);

			cst.execute();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			pool.close(cst);
			pool.close(con);
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
		System.out.println("          - Bateria de pruebas para el caso de INSERTAR -          ");
		System.out.println("-----------------------------------------------------------------\n");

		try {
			String[] simbolos = {"H", "O"};
			int[] nros = {2, 2};
			insertarMolecula("AguaOxigenada", simbolos, nros);

			con = pool.getConnection();
			st = con.createStatement();
			rs = st.executeQuery(
					"SELECT * FROM Moleculas where nombre='AguaOxigenada' AND pesoMolecular=38 AND formula='H2O2'");
			if (rs.next()) {
				System.out.println(
						"Insertar molecula AguaOxigenada con formula H2O2(ordenada alfabeticamente) se ha realizado con éxito.");
			} else {
				System.out.println(
						"Insertar molecula AguaOxigenada con formula H2O2(ordenada alfabeticamente) ·NO· se ha realizado con éxito");
			}

		} catch (ChemistryException e) {
			if (e.getError() == ChemistryError.FORMULA_YA_EXISTENTE) {
				System.out.println("Insertar molecula con formula existente. OK. ");
			} else {
				System.out.println("Insertar molecula con formula existente. MAL. ");
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
		System.out.println("         - Bateria de pruebas para el caso de ACTUALIZAR -         ");
		System.out.println("-----------------------------------------------------------------\n");

//		try {
//			actualizarMoleculaId(1, "H", 4);
//
//			con = pool.getConnection();
//			st = con.createStatement();
//			rs = st.executeQuery("SELECT * FROM Composicion where simbolo='H' AND idMolecula=1 AND nroAtomos=4");
//			if (rs.next()) {
//				System.out.println("ActualizarMolecula mediante Id se ha realizado con éxito.");
//			} else {
//				System.out.println("ActualizarMolecula mediante Id ·NO· se ha realizado con éxito.");
//			}
//
//		} catch (ChemistryException e) {
//			if (e.getError() == ChemistryError.NO_EXISTE_MOLECULA) {
//				System.out.println("ActualizarMolecula mediante Id. OK. ");
//			} else {
//				System.out.println("ActualizarMolecula mediante Id. MAL. ");
//			}
//		} catch (SQLException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} finally {
//			pool.close(st);
//			pool.close(rs);
//			pool.close(con);
//		}

		System.out.println("\n-----------------------------------------------------------------");
		System.out.println("           - Bateria de pruebas para el caso de BORRAR -           ");
		System.out.println("-----------------------------------------------------------------\n");

//		try {
//			borrarMoleculaId(1);
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
//				System.out.println("Borrar molecula mediante Id si no existe. MAL. ");
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
//			borrarMoleculaNombre("Agua");
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

		// Cargamos el script.
		System.out.println("Cargando de nuevo el Script...");
		ExecuteScript.run("./sql/sp_formulas.sql");
	}

}
