import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class date {

	public static String Get_Next_Date(String date){
		String ans,temp;

		String year=date.substring(0, 4);
		int yearInt=Integer.parseInt(year);

		String month=date.substring(5, 7);
		int monthInt=Integer.parseInt(month);

		String day=date.substring(8, 10);
		int dayInt=Integer.parseInt(day);		

		dayInt++;

		if (dayInt > 28){


			if (monthInt==2)
			{
				if (dayInt==30){
					dayInt=1;
					monthInt++;
				}else if ((yearInt%4)!=0){	
					dayInt=1;
					monthInt++;
				}		
			}else if (dayInt > 30 && (monthInt==4 || monthInt==6 || monthInt==9 || monthInt==11 )){
				dayInt=1;
				monthInt++;
			}else if (dayInt > 31 && (monthInt==1 || monthInt==3 || monthInt==5 || monthInt==7|| monthInt==8|| monthInt==10)){
				dayInt=1;
				monthInt++;
			}else if (dayInt > 31 && monthInt==12){
				dayInt=1;
				monthInt=1;
				yearInt++;
			}

		}


		ans = String.valueOf(yearInt);
		ans=ans+"-";

		temp=String.valueOf(monthInt);
		if (monthInt<10){
			ans=ans+"0";
		}
		ans=ans+temp;
		ans=ans+"-";

		temp=String.valueOf(dayInt);
		if (dayInt<10){
			ans=ans+"0";
		}
		ans=ans+temp;

		return 	ans;
	}
	
	public static String daysHandler(String date,String end_date, int n)throws ParseException {
		String dt = date;  // Start date
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Calendar c = Calendar.getInstance();
		c.setTime(sdf.parse(dt));
		c.add(Calendar.DATE,n);  // number of days to add
		Calendar end = Calendar.getInstance();
		end.setTime(sdf.parse(end_date));
		
		if(c.after(end)){
			return end_date;
		}
		dt = sdf.format(c.getTime());  
		return dt;
	}
}
