package lsi.ubu.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilidad para ejecutar el script sql de borrado y creación de datos. Permite
 * la ejecución de los "tests" con independencia.
 * 
 * @author <a href="mailto:jmaudes@ubu.es">Jesús Maudes</a>
 * @author <a href="mailto:rmartico@ubu.es">Raúl Marticorena</a>
 */
public class ExecuteScript {
	/** Logger. */
	private static Logger l = LoggerFactory.getLogger(ExecuteScript.class);;

	/**
	 * Principal.
	 * 
	 * @param args
	 *            el primer argumento incluye el nombre del script a ejecutar
	 */
	public static void main(String[] args) {
		run(args[0]);
	}

	/**
	 * Ejecuta el script sql.
	 * 
	 * @param file_name
	 *            nombre del script .sql a ejecutar
	 */
	public static void run(String file_name) {
		try {
			String line;
			// No te olvides de hacer que el script acabe con "exit;"
			Process p = Runtime.getRuntime().exec("sqlplus hr/hr @" + file_name);

			BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
			while ((line = input.readLine()) != null) {
				l.debug(line);
			}
			input.close();
		} catch (Exception err) {
			err.printStackTrace();
		}
	}

}
