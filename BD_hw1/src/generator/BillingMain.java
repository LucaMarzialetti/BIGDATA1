package generator;

import java.io.IOException;

public class BillingMain {

	public static void main(String[] args) throws IOException {

		// Comando da usare per generare il dataset
		// prima bisogna configurare il generatore di scontrini
		/* al costruttore va passato un file che contiene in chiaro la lista di
		 * cibi da cui pescare
		 * 
		 * tale file è nella cartella billing
		 * il file food può tranquillamente essere editato aggiungendo nuovi
		 * cibi (uno per riga)
		 */

		ItemBillingGenerator IB = new ItemBillingGenerator("food");

		/* quindi bisogna richiamare la funzione generate in cui bisogna passare:
		 * - il nome del file in cui generare il dataset
		 * - il numero di righe del file (nell'esempio 10)
		 * - il numero massimo di cibi per scontrino (nell'esempio 5)
		 * - la data viene generata in modo randomico nel formato yyyy-mm-dd
		 */
		IB.generate("dataset.txt", 1000000, 10);
	}
}
