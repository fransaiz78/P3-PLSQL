package lsi.ubu;

/**
 * Errores codificados para la transacción de formulas moleculares.
 * 
 * @author <a href="mailto:jmaudes@ubu.es">Jesús Maudes</a>
 * @author <a href="mailto:rmartico@ubu.es">Raúl Marticorena</a>
 * @version 1.0
 * @since 1.0
 */
public enum ChemistryError {
	TAMAÑOS_INADECUADOS("La lista de símbolos y números no coincide en tamaño"),
	NOMBRE_DE_MOLECULA_YA_EXISTENTE("El nombre de la molécula ya existe"),
	NO_EXISTE_ATOMO("Alguno de los atomos de la molécula no existe"),
	NO_EXISTE_MOLECULA("No existe dicha molécula"),
	MOLECULA_NO_CONTIENE_SIMBOLO("La molécula especificada no tiene ese símbolo"),
	MOLECULA_YA_EXISTENTE("Existe una molécula con la misma fórmula"),	
	SQL_ERROR("Error SQL no determinado "),
	
	//Creada por los alumnos para un mejor funcionamiento.
	FORMULA_YA_EXISTENTE("Ya existe una molécula con esa fórmula");
	
	/** Texto. */
	private String text;
	
	/** 
	 * Constructor.
	 * 
	 * @param texto texto con el mensaje de error generado
	 */
	private ChemistryError(String text) {
		this.text = text;
	}	
	
	/**
	 * Consulta el texto.
	 * 
	 * @return texto
	 */
	public String getText(){
		return text;
	}
}
