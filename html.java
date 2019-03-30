import java.io.*;

class html {

    public static void htmlBuilder(String color,String data,File f) throws Exception {

    	String add="<font color="+color+">"+data+"<br></font>";
        BufferedWriter bw = new BufferedWriter(new FileWriter(f,true));
        bw.write(add);
        

        bw.close();

    }
  
}