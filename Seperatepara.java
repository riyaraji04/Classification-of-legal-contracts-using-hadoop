import java.io.*;

public class Seperatepara{
	public static void main(String[] args)
		throws IOException
	{
		//D:\\Desktop\\BDA\\Legal Agreement Form.txt
		File file = new File("/home/hadoopusr/Legal_Agreement_Form.txt");
		FileInputStream fileInputStream = new FileInputStream(file);
		InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

		String line;
        int i=1;
        try {
 	     //D:\\Desktop\\BDA\\test1.csv	       
            PrintWriter pw= new PrintWriter(new File("/home/hadoopusr/test3.csv"));
            StringBuilder sb=new StringBuilder();
            while ((line = bufferedReader.readLine()) != null) {
                
                if (line.isEmpty())
                    continue;
                else{
                    sb.append(i);
                    sb.append(",");
                    sb.append(line);
                    sb.append("\r\n");
                    i++;
                }    
		    }
            pw.write(sb.toString());
            pw.close();
            System.out.println("finished");
        } catch (Exception e) {
                // TODO: handle exception
        }
        File file1 = new File("test3.csv");
        BufferedReader br = new BufferedReader(new FileReader(file1)); 
        String line1;
        while ((line1 = br.readLine()) != null) {
                System.out.println(line1);
                }
        
		
	}
}
