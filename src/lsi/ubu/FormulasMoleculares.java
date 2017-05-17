package lsi.ubu;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
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
 * Clase en el que se realizan los metodos java que llaman a los procedimientos del sql. 
 * Incluye tambien la bateria de pruebas.
 * 
 * @author Francisco Saiz Güemes.
 * @author Mario Santamaria Arias
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

	/***
	 * ruta donde se encuentra el archivo .sql.
	 */
	private static final String rutaArchivo = "./sql/sp_formulas.sql"; 
	
	/**
	 * Main donde se realizaran las inicializaciones y la llamada correspondiente a los metodos de la bateria de pruebas.
	 * 
	 * @param args
	 *            argumentos
	 */	
	public static void main(String[] args) {

		try {
			inicializaciones();

			System.out.println("->  Script cargado correctamente.\n");

			Connection con = null;
			Statement st = null;
			ResultSet rs = null;
			
			//Comenzamos las pruebas de insertar sin moleculas en la BD.
			System.out.println("\n->  Moleculas existentes: \n\t·\n");

			System.out.println("\n-----------------------------------------------------------------");
			System.out.println("          - Bateria de pruebas para el caso de INSERTAR -          ");
			System.out.println("-----------------------------------------------------------------\n");

			pruebasInsertar(con, st, rs);
			
			//Comenzamos las pruebas de actualizar con las moleculas:
			// 1 - Agua - H2O
			// 2 - AguaOxigenada - H2O2
			System.out.println("\n->  Moleculas existentes: \n\t·1 - Agua - H2O\n\t·2 - AguaOxigenada - H2O2 \n");

			System.out.println("\n-----------------------------------------------------------------");
			System.out.println("         - Bateria de pruebas para el caso de ACTUALIZAR -         ");
			System.out.println("-----------------------------------------------------------------\n");

			pruebasActualizar(con, st, rs);
			
			//Comenzamos las pruebas de borrar con las moleculas:
			// 1 - Agua - H2O
			// 2 - AguaOxigenada - H2O2
			System.out.println("\n->  Moleculas existentes: \n\t·1 - Agua - H2O\n\t·2 - AguaOxigenada - H2O2 \n");
			
			System.out.println("\n-----------------------------------------------------------------");
			System.out.println("          - Bateria de pruebas para el caso de BORRAR -            ");
			System.out.println("-----------------------------------------------------------------\n");
			
			pruebasBorrar(con, st, rs);
			
			System.out.println("\n-----------------------------------------------------------------");
			System.out.println("    El tratamiento de pruebas se ha realizado correctamente.     \n");

		} catch (NamingException | SQLException | IOException e1) {
			e1.printStackTrace();
		}
	}

	/**
	 * Transacion nº1. Insertar molecula.
	 * 
	 * @param nombre
	 *            String que representa el nombre
	 * @param simbolos
	 *            Array de String que contiene los simbolos
	 * @param nros
	 *            Array de int que contiene los nros
	 * @throws SQLException
	 *             Excepcion
	 */
	public static void insertarMolecula(String nombre, String[] simbolos, int[] nros) throws SQLException {

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
	 * @throws SQLException
	 *             Excepcion
	 */
	public static void borrarMoleculaNombre(String nombreMol) throws SQLException {
		Connection con = null;
		CallableStatement cst = null;

		try {
			con = pool.getConnection();
			cst = con.prepareCall("{ call borrarMoleculaNombre(?) }");
			cst.setString(1, nombreMol);

			cst.execute();

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
	 * @throws SQLException
	 *             Excepcion
	 */
	public static void actualizarMoleculaNombre(String nombreMol, String simbolo, int nro) throws SQLException {
		Connection con = null;
		CallableStatement cst = null;

		try {
			con = pool.getConnection();
			cst = con.prepareCall("{ call actualizarMoleculaNombre(?,?,?) }");
			cst.setString(1, nombreMol);
			cst.setString(2, simbolo);
			cst.setInt(3, nro);

			cst.execute();

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
	 * @throws SQLException
	 *             Excepcion
	 */
	public static void borrarMoleculaId(int id) throws SQLException {
		Connection con = null;
		CallableStatement cst = null;

		try {
			con = pool.getConnection();
			cst = con.prepareCall("{ call borrarMoleculaId(?) }");
			cst.setInt(1, id);

			cst.execute();

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
	 * @throws SQLException
	 *             Excepcion
	 */
	public static void actualizarMoleculaId(int id, String simbolo, int nro) throws SQLException {

		Connection con = null;
		CallableStatement cst = null;

		try {
			con = pool.getConnection();
			cst = con.prepareCall("{ call actualizarMoleculaId(?,?,?) }");
			cst.setInt(1, id);
			cst.setString(2, simbolo);
			cst.setInt(3, nro);

			cst.execute();

		} finally {
			pool.close(cst);
			pool.close(con);
		}
	}

	/**
	 * Metodo donde se realizan las pruebas correspondientes al caso insertar.
	 * 
	 * @param con Conexion
	 * @param st Statement
	 * @param rs ResultSet
	 * @throws SQLException Excepcion
	 * 
	 */
	public static void pruebasInsertar(Connection con, Statement st, ResultSet rs) throws SQLException {

		try {
			String[] simbolos = { "O", "H" };
			int[] nros = { 1, 2 };
			insertarMolecula("Agua", simbolos, nros);

			con = pool.getConnection();
			st = con.createStatement();
			rs = st.executeQuery(
					"SELECT * FROM Moleculas where nombre='Agua' AND pesoMolecular=20 AND formula='H2O'");
			if (rs.next()) {
				System.out.println(
						"Insertar molecula Agua (teniendo que ordenar los arrays) con formula H2O se ha realizado con éxito.");
			} else {
				System.out.println(
						"Insertar molecula Agua (teniendo que ordenar los arrays) con formula H2O ·NO· se ha realizado con éxito");
			}

		} catch (SQLException e) {
			System.out.println("Insertar molecula lanza excepcion cuando no deberia.");

		} finally {
			pool.close(st);
			pool.close(rs);
			pool.close(con);
		}
		
		try {
			String[] simbolos = { "H", "O" };
			int[] nros = { 2, 2 };
			insertarMolecula("AguaOxigenada", simbolos, nros);

			con = pool.getConnection();
			st = con.createStatement();
			rs = st.executeQuery(
					"SELECT * FROM Moleculas where nombre='AguaOxigenada' AND pesoMolecular=38 AND formula='H2O2'");
			if (rs.next()) {
				System.out.println(
						"Insertar molecula AguaOxigenada (sin tener que ordenar los arrays) con formula H2O2 se ha realizado con éxito.");
			} else {
				System.out.println(
						"Insertar molecula AguaOxigenada (sin tener que ordenar los arrays) con formula H2O2 ·NO· se ha realizado con éxito");
			}

		} catch (SQLException e) {
			System.out.println("Insertar molecula lanza excepcion cuando no deberia.");

		} finally {
			pool.close(st);
			pool.close(rs);
			pool.close(con);
		}

		try {
			String[] simbolos = { "H", "O" };
			int[] nros = { 2, 1 };
			insertarMolecula("AguaForRep", simbolos, nros);

		} catch (SQLException e) {

			if (e.getErrorCode() == 20003)
				System.out.println("Insertar molecula con formula ya existente. OK.");
			else
				System.out.println("Insertar molecula con formula ya existente. MAL.");

		} finally {
			pool.close(st);
			pool.close(rs);
			pool.close(con);
		}

		try {
			String[] simbolos = { "H", "O" };
			int[] nros = { 2, 2, 3 };
			insertarMolecula("AguaMal", simbolos, nros);

		} catch (SQLException e) {

			if (e.getErrorCode() == 20000)
				System.out.println("Insertar molecula con arrays de tamaños inadecuados. OK.");
			else
				System.out.println("Insertar molecula con arrays de tamaños inadecuados. MAL.");

		} finally {
			pool.close(st);
			pool.close(rs);
			pool.close(con);
		}

		try {
			String[] simbolos = { "C", "H" };
			int[] nros = { 1, 4 };
			insertarMolecula("Metano", simbolos, nros);
			System.out.println("Insertar molecula se ha realizado con éxito.");

		} catch (SQLException e) {

			if (e.getErrorCode() == 20002)
				System.out.println("Insertar molecula con elemento inexistente. OK.");
			else
				System.out.println("Insertar molecula con elemento inexistente. MAL.");

		} finally {
			pool.close(st);
			pool.close(rs);
			pool.close(con);
		}

		try {
			String[] simbolos = { "H", "O" };
			int[] nros = { 3, 1 };
			insertarMolecula("Agua", simbolos, nros);

		} catch (SQLException e) {

			if (e.getErrorCode() == 20003)
				System.out.println("Insertar molecula con nombre ya existente. OK.");
			else
				System.out.println("Insertar molecula con nombre ya existente. MAL.");

		} finally {
			pool.close(st);
			pool.close(rs);
			pool.close(con);
		}
	}

	
	/**
	 * Metodo donde se realizan las pruebas correspondientes al caso de actualizar tanto para id como nombre.
	 * 
	 * @param con Conexion
	 * @param st Statement
	 * @param rs ResultSet
	 * @throws SQLException Excepcion
	 * 
	 */
	public static void pruebasActualizar(Connection con, Statement st, ResultSet rs) throws SQLException {
		
		try{
			actualizarMoleculaId(1, "V", 4);
			System.out.println("ActualizarMolecula mediante Id con simbolo inexistente se ha realizado con éxito.");
			
		} catch (SQLException e) {
			if (e.getErrorCode() == 20001)
				System.out.println("Actualizar molecula mediante Id con simbolo inexistente. OK.");
			else
				System.out.println("Actualizar molecula mediante Id con simbolo inexistente. MAL.");

		} finally {
			pool.close(st);
			pool.close(rs);
			pool.close(con);
		}
		
		try{
			actualizarMoleculaId(1, "H", 2);
			System.out.println("ActualizarMolecula mediante Id con simbolo inexistente se ha realizado con éxito.");
			
		} catch (SQLException e) {
			if (e.getErrorCode() == 20000)
				System.out.println("Actualizar molecula mediante Id con formula ya existente. OK.");
			else
				System.out.println("Actualizar molecula mediante Id con formula ya existente. MAL.");

		} finally {
			pool.close(st);
			pool.close(rs);
			pool.close(con);
		}
		
		try{
			actualizarMoleculaId(22222, "H", 2);
			System.out.println("ActualizarMolecula mediante Id con id inexistente se ha realizado con éxito.");
			
		} catch (SQLException e) {
			if (e.getErrorCode() == 20002)
				System.out.println("Actualizar molecula mediante id con id inexistente. OK.");
			else
				System.out.println("Actualizar molecula mediante id con id inexistente. MAL.");

		} finally {
			pool.close(st);
			pool.close(rs);
			pool.close(con);
		}
		
		try{
			actualizarMoleculaId(1, "H", 4);

			con = pool.getConnection();
			st = con.createStatement();
			rs = st.executeQuery("SELECT * FROM Composicion where simbolo='H' AND idMolecula=1 AND nroAtomos=4");
			if (rs.next()) {
				System.out.println("Actualizar molecula mediante Id se ha realizado con éxito.");
			} else {
				System.out.println("Actualizar molecula mediante Id ·NO· se ha realizado con éxito.");
			}
			
		} catch (SQLException e) {
			System.out.println("Actualizar molecula mediante id lanza excepcion cuando no deberia.");

		} finally {
			pool.close(st);
			pool.close(rs);
			pool.close(con);
		}


		System.out.println("\n");

		try{
			actualizarMoleculaNombre("Agua", "V", 4);
			System.out.println("Actualizar molecula mediante nombre con simbolo inexistente se ha realizado con éxito.");
			
		} catch (SQLException e) {
			if (e.getErrorCode() == 20001)
				System.out.println("Actualizar molecula mediante nombre con simbolo inexistente. OK.");
			else
				System.out.println("Actualizar molecula mediante nombre con simbolo inexistente. MAL.");

		} finally {
			pool.close(st);
			pool.close(rs);
			pool.close(con);
		}
		
		//Ahora tenemos 1 - Agua - H4O
		try{
			actualizarMoleculaNombre("Agua", "H", 4);
			System.out.println("Actualizar molecula mediante Nombre con formula ya existente se ha realizado con éxito.");
			
		} catch (SQLException e) {
			if (e.getErrorCode() == 20000)
				System.out.println("Actualizar molecula mediante nombre con formula ya existente. OK.");
			else
				System.out.println("Actualizar molecula mediante nombre con formula ya existente. MAL.");

		} finally {
			pool.close(st);
			pool.close(rs);
			pool.close(con);
		}
		
		try{
			actualizarMoleculaNombre("aaaaaaa", "H", 2);
			System.out.println("ActualizarMolecula mediante nombre con nombre inexistente se ha realizado con éxito.");
			
		} catch (SQLException e) {
			if (e.getErrorCode() == 20002)
				System.out.println("Actualizar molecula mediante nombre con nombre inexistente. OK.");
			else
				System.out.println("Actualizar molecula mediante nombre con nombre inexistente. MAL.");

		} finally {
			pool.close(st);
			pool.close(rs);
			pool.close(con);
		}
		
		//Dejamos la molecula Agua con su formula correspondiente.
		try{
			actualizarMoleculaNombre("Agua", "H", 2);

			con = pool.getConnection();
			st = con.createStatement();
			rs = st.executeQuery("SELECT * FROM Composicion where simbolo='H' AND idMolecula=1 AND nroAtomos=2");
			if (rs.next()) {
				System.out.println("ActualizarMolecula mediante nombre se ha realizado con éxito.");
			} else {
				System.out.println("ActualizarMolecula mediante nombre ·NO· se ha realizado con éxito.");
			}
			
		} catch (SQLException e) {
			System.out.println("Actualizar molecula mediante nombre lanza excepcion cuando no deberia.");

		} finally {
			pool.close(st);
			pool.close(rs);
			pool.close(con);
		}
	}
	
	/**
	 * Metodo donde se realizan las pruebas correspondientes al caso borrar tanto por id como nombre.
	 * 
	 * @param con Conexion
	 * @param st Statement
	 * @param rs ResultSet
	 * @throws SQLException Excepcion
	 * 
	 */
	public static void pruebasBorrar(Connection con, Statement st, ResultSet rs) throws SQLException {
		try{
			borrarMoleculaId(1);
			
			con = pool.getConnection();
			st = con.createStatement();
			rs = st.executeQuery("SELECT * FROM Moleculas where id=1");
			if (!rs.next()) {
				System.out.println("Borrar molecula mediante Id se ha realizado con éxito.");
			} else {
				System.out.println("Borrar molecula mediante Id ·NO· se ha realizado con éxito.");

			}
		} catch (SQLException e) {
			System.out.println("Borrar molecula lanza excepcion cuando no deberia.");

		} finally {
			pool.close(st);
			pool.close(rs);
			pool.close(con);
		}
		
		try{
			borrarMoleculaId(1);
			System.out.println("Borrar molecula por Id se ha realizado correctamente.");

		} catch (SQLException e) {
			if (e.getErrorCode() == 20000)
				System.out.println("Borrar molecula con Id inexistente. OK.");
			else
				System.out.println("Borrar molecula con Id inexistente. MAL.");

		} finally {
			pool.close(st);
			pool.close(rs);
			pool.close(con);
		}

		try{
			borrarMoleculaNombre("AguaOxigenada");
			
			con = pool.getConnection();
			st = con.createStatement();
			rs = st.executeQuery("SELECT * FROM Moleculas where nombre='Agua'");
			if (!rs.next()) {
				System.out.println("Borrar molecula mediante Nombre se ha realizado con éxito.");
			} else {
				System.out.println("Borrar molecula mediante Nombre ·NO· se ha realizado con éxito.");
			}
			
		} catch (SQLException e) {
			System.out.println("Borrar molecula lanza excepcion cuando no deberia.");

		} finally {
			pool.close(st);
			pool.close(rs);
			pool.close(con);
		}

		try{
			borrarMoleculaNombre("AguaOxigenada");
			
		} catch (SQLException e) {
			
			if (e.getErrorCode() == 20000)
				System.out.println("Borrar molecula con Nombre inexistente. OK.");
			else
				System.out.println("Borrar molecula con Nombre inexistente. MAL.");

		} finally {
			pool.close(st);
			pool.close(rs);
			pool.close(con);
		}

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
		ExecuteScript.run(rutaArchivo);
	}

}
