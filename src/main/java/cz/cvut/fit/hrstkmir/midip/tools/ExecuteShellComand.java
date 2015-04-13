
package cz.cvut.fit.hrstkmir.midip.tools;
import java.io.BufferedReader;
import java.io.InputStreamReader;
 
public class ExecuteShellComand {
 
	public static void ExecutePATOHProgram(int partition){
		ExecuteShellComand obj = new ExecuteShellComand();

		String command = "/home/mira/Documents/DIP/PATOH/patoh ./patohGraph.txt 4 RA=6 UM=U";
 
		String output = obj.executeCommand(command);
 
		System.out.println(output);
 
	}
 
	private String executeCommand(String command) {
 
		StringBuffer output = new StringBuffer();
 
		Process p;
		try {
			p = Runtime.getRuntime().exec(command);
			p.waitFor();
			BufferedReader reader = 
                            new BufferedReader(new InputStreamReader(p.getInputStream()));
 
                        String line = "";			
			while ((line = reader.readLine())!= null) {
				output.append(line + "\n");
			}
 
		} catch (Exception e) {
			e.printStackTrace();
		}
 
		return output.toString();
 
	}
 
}
