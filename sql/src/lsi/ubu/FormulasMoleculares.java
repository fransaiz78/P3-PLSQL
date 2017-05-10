package lsi.ubu;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsi.ubu.util.ExecuteScript;

public class FormulasMoleculares {
	// Metodos con las transacciones y main
	private static PoolDeConexiones pool;
	private static Logger logger;

	@SuppressWarnings({ "static-access", "unused" })
	public static void main(String[] args) {
		Connection con = null;
		ExecuteScript executeScript = new ExecuteScript();

		try {
			inicializaciones();
			
			/*----------------------------insertarMolecula(nombre, simbolos, nros)----------------------------*/

			/* Prueba insertaMolécula correctamente */
			String[] simbolos = { "H", "O" };
			int[] nros = { 2, 1 };
			//executeScript.main(args);
			//System.out.println("Prueba insertar molécula: " + insertaMolecula("Agua", simbolos, nros) + "=true");

			/* Prueba insertaMolécula que deberia fallar, los simbolos no estan en la tabla de moleculas */
			String[] simbolos2 = { "P", "Q" };
			int[] nros2 = { 2, 1 };
			//executeScript.main(args);
			//System.out.println("Prueba insertar molécula: " + insertaMolecula("Agua", simbolos2, nros2) + "=false");

			/* Prueba insertaMolécula que deberia fallar, tamaños de arrays inadecuados */
			String[] simbolos3 = { "P", "Q" };
			int[] nros3 = { 2, 1, 5, 6 };
			//executeScript.main(args);
			//System.out.println("Prueba insertar molécula: " + insertaMolecula("Agua", simbolos3, nros3) + "=false");

			/* Prueba insertaMolécula que deberia fallar, tamaños de arrays inadecuados*/
			String[] simbolos4 = { "P", "Q", "R", "S" };
			int[] nros4 = { 2, 1 };
			//executeScript.main(args);
			//System.out.println("Prueba insertar molécula: " + insertaMolecula("Agua", simbolos4, nros4) + "=false");

			/* Prueba insertaMolécula que deberia fallar */
			//executeScript.main(args);
			//insertaMolecula("Agua", simbolos, nros);
			//System.out.println("Prueba insertar molécula: " + insertaMolecula("Agua", simbolos, nros) + "=false");
			
			/*----------------------------borrarMolecula(nombreMol)----------------------------*/
			
			/* Insertamos una molecula para despues borrarla */
			//executeScript.main(args);
			//System.out.println("Prueba insertar molécula: " + insertaMolecula("Agua", simbolos, nros) + "=true");
			//System.out.println("Prueba borrar molecula: " + borrarMolecula("Agua") + "=true");
			
			/* Intentamos borrar una molecula que no existe */
			//executeScript.main(args);
			//System.out.println("Prueba borrar molecula: " + borrarMolecula("Agua") + "=false");
			
			/*----------------------------borrarMolecula(id)----------------------------*/
	
			/* Insertamos una molecula para despues borrarla */
			//executeScript.main(args);
			//System.out.println("Prueba insertar molécula: " + insertaMolecula("Agua", simbolos, nros) + "=true");
			//System.out.println("Prueba borrar molecula: " + borrarMolecula(1) + "=true");
			
			/* Intentamos borrar una molecula que no existe */
			//executeScript.main(args);
			//System.out.println("Prueba borrar molecula: " + borrarMolecula(200) + "=false");
			

			/*----------------------------actualizarMolecula(id,simbolo,nro)----------------------------*/
			
			/*Actualizamos una molecula de forma correcta*/
			//executeScript.main(args);
			//System.out.println("Prueba insertar molécula: " + insertaMolecula("Agua", simbolos, nros) + "=true");
			//System.out.println("Prueba actualizar molecula: " + actualizarMolecula(1, "O", 4) + "=true");
			
			/*Actualizamos una molecula que no existe */
			//executeScript.main(args);
			//System.out.println("Prueba actualizar molecula: " + actualizarMolecula(1, "O", 4) + "=false");
			
			/*----------------------------actualizarMolecula(nombreMolecula,simbolo,nro)----------------------------*/
			
			/*Actualizamos una molecula de forma correcta*/
			//executeScript.main(args);
			//System.out.println("Prueba insertar molécula: " + insertaMolecula("Agua", simbolos, nros) + "=true");
			//System.out.println("Prueba actualizar molecula: " + actualizarMolecula("Agua", "O", 4) + "=true");
			
			/*Actualizamos una molecula que no existe */
			//executeScript.main(args);
			//System.out.println("Prueba actualizar molecula: " + actualizarMolecula("Agua", "O", 4) + "=false");
			

		} catch (Exception e) {
			System.err.println(e.getMessage());
			logger.error(e.getLocalizedMessage());
			pool.undo(con);
		}
	}

	public static boolean insertaMolecula(String nombre, String[] simbolos, int[] nros) {

		boolean retorno = true;
		Connection con = null;
		PreparedStatement statement = null;
		ResultSet rs = null;

		try {
			//Comprobamos que los arrays de simbolos y nros tengan el mismo tamaño
			if (simbolos.length == nros.length) {
				logger.info("Comiezo insertaMolecula.");
				con = pool.getConnection();

				// Calculamos el peso molecular
				int[] pesosAtomicos = new int[simbolos.length];
				logger.info("Obteniendo pesos atomicos.");
				for (int i = 0; i < simbolos.length; i++) {
					statement = con.prepareStatement("SELECT pesoAtomico FROM elementos WHERE SIMBOLO=?");
					statement.setString(1, simbolos[i]);
					rs = statement.executeQuery();
					if (rs.next()) {
						logger.info("Guardando pesos atomicos.");
						pesosAtomicos[i] = rs.getInt(1);
					} else {
						// Deberia saltar una excepcion ya que el elemento no esta  en la tabla elementos
						logger.error("No se ha podido obtener el peso atomico.");
						throw(new ChemistryException(ChemistryError.MOLECULA_NO_CONTIENE_SIMBOLO));
					}
				}
				logger.info("Calculando pesoMolecular.");
				int pesoMolecular = 0;
				for (int i = 0; i < pesosAtomicos.length; i++) {
					pesoMolecular += (pesosAtomicos[i] * nros[i]);
				}

				// Obtenemos la formula formula
				logger.info("Calculando la formula.");
				String formula = "";
				for (int i = 0; i < simbolos.length; i++) {
					if (nros[i] < 1) {
						formula += simbolos[i];
					} else {
						formula += simbolos[i] + nros[i];
					}
				}

				// Insertamos la molecula en la tabla moleculas
				statement = con.prepareStatement("INSERT INTO moleculas VALUES(moleculas_id.nextval,?,?,?)");
				statement.setString(1, nombre); // nombre
				statement.setInt(2, pesoMolecular);
				statement.setString(3, formula);
				if (statement.executeUpdate() != 1) {
					// Si no se inserta correctamente
					throw(new ChemistryException(ChemistryError.SQL_ERROR));
				}
				logger.info("Realizado insert de las moleculas en la tabla moleculas.");

				// Insertamos los elementos en la tabla composicion
				for (int i = 0; i < simbolos.length; i++) {
					statement = con.prepareStatement("INSERT INTO composicion VALUES(?, moleculas_id.currval, ?)");
					statement.setString(1, simbolos[i]); // simbolo
					statement.setInt(2, nros[i]); // nroAtomos
					if (statement.executeUpdate() != 1) {
						// Si no se inserta correctamente
						throw(new ChemistryException(ChemistryError.SQL_ERROR));
					}
					logger.info("Realizado insert del elemento " + simbolos[i] + " en la tabla composicion.");
				}

				if (retorno) {
					logger.info("Realizando commit.");
					con.commit();
				}
			}else{
				throw(new ChemistryException(ChemistryError.TAMAÑOS_INADECUADOS));
			}
		} catch (SQLException e) {
			logger.error("La transacción hay que deshacerla.");
			logger.error(e.getLocalizedMessage());
			pool.undo(con);
			retorno = false;
		} catch (ChemistryException e){
			logger.error("La transacción hay que deshacerla.");
			logger.error(e.getLocalizedMessage());
			pool.undo(con);
			retorno = false;
		} finally {
			logger.info("Transaccion finalizada. Cerrando recursos.");
			pool.close(rs);
			pool.close(statement);
			pool.close(con);
		}
		return retorno;
	}
	
	public static boolean borrarMolecula(int id){
		boolean retorno = true;
		Connection con = null;
		PreparedStatement statement = null;
		ResultSet rs = null;
		
		try{
			logger.info("Comiezo borrarMolecula.");
			con = pool.getConnection();
												
			//Buscamos la molecula en la tabla de moleculas y guardamos el id
			statement = con.prepareStatement("SELECT * FROM moleculas WHERE id=?");
			statement.setInt(1, id);
			rs = statement.executeQuery();
			
			if(rs.next()){
				logger.info("Borrando la molecula...");
				
				//Borramos los elementos de la tabla composicion con el idMolecula obtenido anteriormente
				statement = con.prepareStatement("DELETE FROM composicion WHERE idMolecula=?");
				statement.setInt(1, id);
				//TODO comprobar si se ha borrado correctamente
				statement.executeUpdate();
				logger.info("Borrados los elementos de la tabla composicion.");
				
				//Borramos la molecula de la tabla moleculas
				statement = con.prepareStatement("DELETE FROM moleculas WHERE id=?");
				statement.setInt(1, id);
				//TODO comprobar si se ha borrado correctamente
				statement.executeUpdate();
				logger.info("Borrada la molecula de la tabla moleculas.");
							
				if (retorno) {
					logger.info("Realizando commit.");
					con.commit();
				}
			}else{
				// Deberia saltar una excepcion ya que la molecula no existe
				logger.error("No se ha podido obtener el id de la molecula.");
				throw(new ChemistryException(ChemistryError.NO_EXISTE_MOLECULA));
			}
			
		} catch (SQLException e) {
			logger.error("La transacción hay que deshacerla.");
			logger.error(e.getLocalizedMessage());
			pool.undo(con);
			retorno = false;
		} catch (ChemistryException e) {
			logger.error("La transacción hay que deshacerla.");
			logger.error(e.getLocalizedMessage());
			pool.undo(con);
			retorno = false;
		} finally {
			logger.info("Transaccion finalizada. Cerrando recursos.");
			pool.close(rs);
			pool.close(statement);
			pool.close(con);
		}
		return retorno;
	}
	
	public static boolean borrarMolecula(String nombreMol){
		boolean retorno = true;
		Connection con = null;
		PreparedStatement statement = null;
		ResultSet rs = null;
		
		try{
			logger.info("Comiezo borrarMolecula.");
			con = pool.getConnection();
			
			//Buscamos la molecula en la tabla de moleculas y guardamos el id
			statement = con.prepareStatement("SELECT id FROM moleculas WHERE nombre=?");
			statement.setString(1, nombreMol);
			rs = statement.executeQuery();
			
			if(rs.next()){
				logger.info("Borrando la molecula...");
				int id = rs.getInt(1);
				
				//Borramos los elementos de la tabla composicion con el idMolecula obtenido anteriormente
				statement = con.prepareStatement("DELETE FROM composicion WHERE idMolecula=?");
				statement.setInt(1, id);
				//TODO comprobar si se ha borrado correctamente
				statement.executeUpdate();
				logger.info("Borrados los elementos de la tabla composicion.");
				
				//Borramos la molecula de la tabla moleculas
				statement = con.prepareStatement("DELETE FROM moleculas WHERE id=?");
				statement.setInt(1, id);
				//TODO comprobar si se ha borrado correctamente
				statement.executeUpdate();
				logger.info("Borrada la molecula de la tabla moleculas.");
							
				if (retorno) {
					logger.info("Realizando commit.");
					con.commit();
				}
			}else{
				// Deberia saltar una excepcion ya que la molecula no existe
				logger.error("No se ha podido obtener el id de la molecula.");
				throw(new ChemistryException(ChemistryError.NO_EXISTE_MOLECULA));
			}
			
			
		} catch (SQLException e) {
			logger.error("La transacción hay que deshacerla.");
			logger.error(e.getLocalizedMessage());
			pool.undo(con);
			retorno = false;
		} catch (ChemistryException e) {
			logger.error("La transacción hay que deshacerla.");
			logger.error(e.getLocalizedMessage());
			pool.undo(con);
			retorno = false;
		} finally {
			logger.info("Transaccion finalizada. Cerrando recursos.");
			pool.close(rs);
			pool.close(statement);
			pool.close(con);
		}
		return retorno;
	}
	
	public static boolean actualizarMolecula(int id,String simbolo, int nro) {
		boolean retorno = true;
		Connection con = null;
		PreparedStatement statement = null;
		ResultSet rs = null;
		
		try{
			logger.info("Comiezo actualizarMolecula con id.");
			con = pool.getConnection();
			
			//Buscamos la molecula en la tabla de moleculas y guardamos el id
			statement = con.prepareStatement("SELECT * FROM moleculas WHERE id=?");
			statement.setInt(1, id);
			rs = statement.executeQuery();
			
			if(rs.next()){
				logger.info("Llamando a actualizarMolecula con la id.");
				
				logger.info("Actualizando la tabla de composicion.");
				statement = con.prepareStatement("UPDATE composicion SET nroAtomos=? WHERE simbolo=? AND idMolecula=?");
				statement.setInt(1, nro);
				statement.setString(2, simbolo);
				statement.setInt(3, id);
				if (statement.executeUpdate() != 1) {
					// Si no se actualiza correctamente
					throw(new ChemistryException(ChemistryError.SQL_ERROR));
				}
					
				//Recalculamos la formula y el peso molecular
				logger.info("Recalculando la formula y el peso molecular.");
				statement = con.prepareStatement("SELECT composicion.simbolo, composicion.nroAtomos, elementos.pesoAtomico"
												+ " FROM composicion JOIN elementos"
												+ " ON composicion.simbolo=elementos.simbolo"
												+ " WHERE idMolecula=?");
				statement.setInt(1, id);
				rs = statement.executeQuery();
				int pesoMolecular = 0;
				String formula = "";
				while(rs.next()){
					pesoMolecular += (rs.getInt(2)*rs.getInt(3));
					if (rs.getInt(2) < 1) {
						formula += rs.getString(1);
					} else {
						formula += rs.getString(1) + rs.getInt(2);
					}
				}
				
				logger.info("Actualizando la tabla de moleculas con la nueva formula y peso.");
				statement = con.prepareStatement("UPDATE moleculas SET pesoMolecular=?, formula=? WHERE id=?");
				statement.setInt(1, pesoMolecular);
				statement.setString(2, formula);
				statement.setInt(3, id);
				if (statement.executeUpdate() == 1) {
					logger.info("Realizando commit.");
					con.commit();
				}else{
					// Si no se actualiza correctamente
					throw(new ChemistryException(ChemistryError.SQL_ERROR));
				}
			}else{
				// Deberia saltar una excepcion ya que la molecula no existe
				logger.error("No se ha podido obtener el id de la molecula.");
				throw(new ChemistryException(ChemistryError.NO_EXISTE_MOLECULA));
			}
			
		} catch (SQLException e) {
			logger.error("La transacción hay que deshacerla.");
			logger.error(e.getLocalizedMessage());
			pool.undo(con);
			retorno = false;
		} catch (ChemistryException e) {
			logger.error("La transacción hay que deshacerla.");
			logger.error(e.getLocalizedMessage());
			pool.undo(con);
			retorno = false;
		} finally {
			logger.info("Transaccion finalizada. Cerrando recursos.");
			pool.close(rs);
			pool.close(statement);
			pool.close(con);
		}
		return retorno;
	}
	
	public static boolean actualizarMolecula(String nombreMol,String simbolo, int nro) {
		boolean retorno = true;
		Connection con = null;
		PreparedStatement statement = null;
		ResultSet rs = null;
		
		try{
			logger.info("Comiezo actualizarMolecula con nombreMolecula.");
			con = pool.getConnection();
			
			//Buscamos la molecula en la tabla de moleculas y guardamos el id
			statement = con.prepareStatement("SELECT id FROM moleculas WHERE nombre=?");
			statement.setString(1, nombreMol);
			rs = statement.executeQuery();
			
			if(rs.next()){
				logger.info("Llamando a actualizarMolecula con la id.");
				int id = rs.getInt(1);
				
				logger.info("Actualizando la tabla de composicion.");
				statement = con.prepareStatement("UPDATE composicion SET nroAtomos=? WHERE simbolo=? AND idMolecula=?");
				statement.setInt(1, nro);
				statement.setString(2, simbolo);
				statement.setInt(3, id);
				if (statement.executeUpdate() != 1) {
					// Si no se actualiza correctamente
					throw(new ChemistryException(ChemistryError.SQL_ERROR));
				}
					
				//Recalculamos la formula y el peso molecular
				logger.info("Recalculando la formula y el peso molecular.");
				statement = con.prepareStatement("SELECT composicion.simbolo, composicion.nroAtomos, elementos.pesoAtomico"
												+ " FROM composicion JOIN elementos"
												+ " ON composicion.simbolo=elementos.simbolo"
												+ " WHERE idMolecula=?");
				statement.setInt(1, id);
				rs = statement.executeQuery();
				int pesoMolecular = 0;
				String formula = "";
				while(rs.next()){
					pesoMolecular += (rs.getInt(2)*rs.getInt(3));
					if (rs.getInt(2) < 1) {
						formula += rs.getString(1);
					} else {
						formula += rs.getString(1) + rs.getInt(2);
					}
				}
				
				logger.info("Actualizando la tabla de moleculas con la nueva formula y peso.");
				statement = con.prepareStatement("UPDATE moleculas SET pesoMolecular=?, formula=? WHERE id=?");
				statement.setInt(1, pesoMolecular);
				statement.setString(2, formula);
				statement.setInt(3, id);
				if (statement.executeUpdate() == 1) {
					logger.info("Realizando commit.");
					con.commit();
				}else{
					// Si no se actualiza correctamente
					throw(new ChemistryException(ChemistryError.SQL_ERROR));
				}
			}else{
				// Deberia saltar una excepcion ya que la molecula no existe
				logger.error("No se ha podido obtener el id de la molecula.");
				throw(new ChemistryException(ChemistryError.NO_EXISTE_MOLECULA));
			}
			
			
		} catch (SQLException e) {
			logger.error("La transacción hay que deshacerla.");
			logger.error(e.getLocalizedMessage());
			pool.undo(con);
			retorno = false;
		} catch (ChemistryException e) {
			logger.error("La transacción hay que deshacerla.");
			logger.error(e.getLocalizedMessage());
			pool.undo(con);
			retorno = false;
		} finally {
			logger.info("Transaccion finalizada. Cerrando recursos.");
			pool.close(rs);
			pool.close(statement);
			pool.close(con);
		}
		return retorno;
	}
	
	public static void inicializaciones() throws NamingException, SQLException, IOException {
		// Inicializacion de Pool
		pool = PoolDeConexiones.getInstance();

		// Inicializacion del Logger
		logger = LoggerFactory.getLogger(FormulasMoleculares.class);
		logger.info("Comienzo Ejecución.");
	}
}
