package lsi.ubu;

/**
 * Tipo error en transacción de formulas moleculares.
 * 
 * @author <a href="mailto:jmaudes@ubu.es">Jesús Maudes</a>
 * @author <a href="mailto:rmartico@ubu.es">Raúl Marticorena</a>
 */
@SuppressWarnings("serial")
public class ChemistryException extends Exception {
	
	/** Error. */
	private ChemistryError error;

	/**
	 * Constructor.
	 * 
	 * @param error
	 */
	public ChemistryException(ChemistryError error) {
		this.error = error;
	}
	
	/**
	 * Constructor.
	 * 
	 * @param error error
	 * @param ex si la excepción ha sido originada por otra excepción
	 */
	public ChemistryException(ChemistryError error, Exception ex) {
		super(ex);
		this.error = error;
	}
	
	/**
	 * Obtiene el tipo de error que genera la excepción.
	 * 
	 * @return error
	 */
	public ChemistryError getError() {
		return error;
	}
	
	/**
	 * Obtiene el texto del error generado.
	 * 
	 * @return texto texto descriptivo del error
	 */
	@Override
	public String getMessage() {
		return error.getText();
	}
}
