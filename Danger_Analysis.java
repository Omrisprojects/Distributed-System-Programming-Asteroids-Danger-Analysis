
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;



public class Danger_Analysis {


		
	public static JSONObject inputRead(String jsonText){
		JSONObject jsonObject = null;
		//Json read//
		JSONParser parser = new JSONParser();
		try{
			Object obj = parser.parse(jsonText);

			 jsonObject = (JSONObject) obj;
		
		}catch (ParseException e) {
			e.printStackTrace();
		}
		return  jsonObject;

	}
		
	public static File outputWrite(String start,String end,double kilPrSec,double estDiamMax,double astr,String localAppName,int[] statistics ){

		JSONParser parser2 = new JSONParser();
		File f = null;
		try {
			String json = readJsonFromUrl("https://api.nasa.gov/neo/rest/v1/feed?start_date="+start+"&end_date="+end+"&api_key=NEMIlgFVuTLM1CjYTetlwb9KyeLR84gRGpF5HIOr");//&end_date=2015-09-08&speed_threshold=....&diameter_thershold=.........
			//if we if the arguments from the input?every arguments can be even there are not writen in the assig?to look at the furom
			Object obj = parser2.parse(json);
			JSONObject jsonObject = (JSONObject) obj;


			String date_to_take=start;
			String end_next=date.Get_Next_Date(end);
			String color="";
			f = new File("days_"+localAppName+start+"_"+end+".html");
			BufferedWriter bw = new BufferedWriter(new FileWriter(f,true));
			bw.write(" ");
			bw.close();
			while((date_to_take).equals(end_next)!=true){
				JSONObject start_Date3 = (JSONObject) jsonObject.get("near_earth_objects");
				JSONArray Days_astro=(JSONArray) start_Date3.get(date_to_take);

				for(int i=0;i<Days_astro.size();i++){
					JSONObject  astro= (JSONObject) Days_astro.get(i);
					statistics[0]++;
					statistics[2]++;
					if((boolean)astro.get("is_potentially_hazardous_asteroid")==true){
						color="black";
						statistics[2]--;
						JSONArray  speed_step1= (JSONArray) astro.get("close_approach_data");
						JSONObject  speed_step2= (JSONObject) speed_step1.get(0);
						JSONObject  speed_step3= (JSONObject) speed_step2.get("relative_velocity");
						String  speed_string= (String) speed_step3.get("kilometers_per_second");//3
						JSONObject  dis_step1= (JSONObject) astro.get("estimated_diameter");
						JSONObject  dis_step2= (JSONObject) dis_step1.get("meters");
						Double  estimated_diameter_max= (Double) dis_step2.get("estimated_diameter_max");//5
						double kilometers_per_second;
						kilometers_per_second= Double.parseDouble(speed_string);
						Double  estimated_diameter_min= (Double) dis_step2.get("estimated_diameter_min");//4
						JSONArray  miss_step1= (JSONArray) astro.get("close_approach_data");
						JSONObject  miss_step2= (JSONObject) miss_step1.get(0);
						JSONObject  miss_step3= (JSONObject) miss_step2.get("miss_distance");
						String  miss_string= (String) miss_step3.get("astronomical");
						double astronomical;
						astronomical= Double.parseDouble(miss_string);
						if(kilometers_per_second>=kilPrSec){
							statistics[1]++;
							color="green";
							if(estimated_diameter_min>=estDiamMax){
								color="yellow";
								if(astronomical>=astr){
									color="red";
								}
							}
						}
						if (color!=""){
							String name=(String)astro.get("name");
							String close_approach_data=(String)speed_step2.get("close_approach_date");
							String kilometers=(String)miss_step3.get("kilometers");
							String data_to_send=("name:"+name+", close_approach_data:"+close_approach_data+" , relative_velocity:"+speed_string +" , stimated_diameter_min:"+String.valueOf(estimated_diameter_min)+" ,estimated_diameter_max: "+String.valueOf(estimated_diameter_max)+" , miss_distance:"+kilometers  ); 
							try{
								html.htmlBuilder(color, data_to_send,f);
							}catch (Exception e) {
								e.printStackTrace();
							}
							color="";
						}
					}
				}
				date_to_take=date.Get_Next_Date(date_to_take);
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();

		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return f;
	}

	public static String readAll(Reader rd) throws IOException {
		StringBuilder sb = new StringBuilder();
		int cp;
		while ((cp = rd.read()) != -1) {
			sb.append((char) cp);
		}
		return sb.toString();
	}

	public static String readJsonFromUrl(String url) throws IOException {
	
		InputStream is = new URL(url).openStream();
		try {
			BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
			String jsonText = readAll(rd);
			return jsonText;
		} finally {
			is.close();
		}
	}
}