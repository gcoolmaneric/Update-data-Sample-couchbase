import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Date;

// Json
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.internal.LinkedTreeMap;

// Couchbase 
import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.*;

public class syncUser {

	public class User 
	{
		String userId;
		int level;
		int region;
		int gold;
		int credit;
		long lastRecover;
		long logintime;
		int maxLevel;
		int lives;
		int maxLives;
		int locker;
		
		String name;
		String pic;
		Object levelData;
		Object loginRecord;
		
		/// New field
		int superUser;
		int totalRank;
		int dailyRank;
		int eventRank;
		Object soundStatus;
		Object others;
		
		
	   public void setOthers(Object data)
	   {
		  this.others = data;
	   }
	   
	   public Object getOthers()
	   {
		  return this.others;
	   }
	}

	public class OthersData {
		
		Object helps;
		Object boss;
		Object gift;
		Object bag;
		Date gmt_create;
		
		int comment;
		Object unlockedComments;
	
	   public void setComment(int data)
	   {
		  this.comment = data;
	   }
	   
	   public void setUnlockedComment(Object data)
	   {
		  this.unlockedComments = data;
	   }
	}
	
	public static void main(String[] args) {
    
		// Config log file
		configure();
		
		System.out.println("--------------------------------------------------------------------------");
		System.out.println("\tCouchbase - Update User Data");
		System.out.println("--------------------------------------------------------------------------");

		List<URI> uris = new LinkedList<URI>();
		uris.add(URI.create("http://172.30.1.167:8091/pools"));

		CouchbaseClient cb = null;
		try {
			
			cb = new CouchbaseClient(uris, "default", "");
			
			System.out.println("--------------------------------------------------------------------------");
			View view = cb.getView("User", "UserData");
			
			Query query = new Query();
			int docsPerPage = 10000;
			int userCount = 0;
			
			Paginator paginatedQuery = cb.paginatedQuery(view, query, docsPerPage);
			int pageCount = 0;
			
			Runtime r = Runtime.getRuntime() ;

			while(paginatedQuery.hasNext()) 
			{
				System.out.println("Total JVM Memory:" + r.totalMemory()) ;
        			System.out.println("Before Memory = " + r.freeMemory()) ;

				pageCount++;
				System.out.println(" -- Page "+ pageCount +" -- ");
				ViewResponse response = paginatedQuery.next();
				
				for (ViewRow row : response) 
				{
					//System.out.println(row.getKey() + " getId : " + row.getId());
					//System.out.println(row.getValue() + " getValue : " + row.getValue());
					
					if(row.getValue().length() > 0 )
					{
						// 1. Serialize array from json data 
						Gson json = new Gson();	
						User[] userData = json.fromJson(row.getValue(), User[].class);
					
						String originId = row.getId();
						String userId = originId.substring(originId.indexOf("user")+5, originId.length());
						
						String name = userData[0].name;
						//System.out.println("userId:"+ userId +" name: " + name );
						
						//if( name.length() > 0 && userId.equals("4137347"))
						
						if( name.length() > 0)
						{
							//System.out.println(">>>>> Start update userId: "+  userId );		
							//System.out.println("###### Before soundStatus: "+  userData[0].soundStatus );
							//System.out.println("###### Before others: "+  userData[0].others );
							
							// 2. Update user data 
							userData[0].userId = userId;
							userData[0].superUser = 0;
							userData[0].totalRank = 0;
							userData[0].dailyRank = 0;
							userData[0].eventRank = 0;
							
							// update sound status 
							LinkedTreeMap<String, Integer> sound=new LinkedTreeMap<String, Integer>();
							sound.put("bgm", 20);
							sound.put("se", 50);
							sound.put("volumn", 100);
							userData[0].soundStatus = (Object)sound;
							//System.out.println("###### After soundStatus: "+  userData[0].soundStatus );
							
							// update others 
							int[] comments = {1};
							LinkedTreeMap result=(LinkedTreeMap)userData[0].others;
							result.put("comment", 1);
							result.put("unlockedComments", comments);
							userData[0].others = (Object)result;						
							//System.out.println("###### After 2 others: "+  json.toJson(userData[0]) );
	
							// 3. Save 
							Map newData = new HashMap();
							newData.put("udata", userData[0]);
							cb.replace(row.getId(), json.toJson(newData));

							userCount++;
							
							System.out.println(">>>>> userCount: "+ userCount +"  Update OK userId: "+  userId );
							//System.exit(0);
			
						}
						
						
					}// end if 
					
					
				}
			 	
				  
				System.out.println("After Memory = " + r.freeMemory()) ;
        		// Release memory 
				r.gc() ;
        		System.out.println("After memory GC Memory = " + r.freeMemory()) ;

				System.out.println(" -- -- -- ");
			}
			
			System.out.println("\n\n");
			cb.shutdown(10, TimeUnit.SECONDS);
			
		} catch (Exception e) {
			System.err.println("Error connecting to Couchbase: " + e.getMessage());
		}
	
  }
  
  private static void configure() {

		for(Handler h : Logger.getLogger("com.couchbase.client").getParent().getHandlers()) {
			if(h instanceof ConsoleHandler) {
				h.setLevel(Level.OFF);
			}
		}
		Properties systemProperties = System.getProperties();
		systemProperties.put("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.SunLogger");
		System.setProperties(systemProperties);

		Logger logger = Logger.getLogger("com.couchbase.client");
		logger.setLevel(Level.OFF);
		for(Handler h : logger.getParent().getHandlers()) {
			if(h instanceof ConsoleHandler){
				h.setLevel(Level.OFF);
			}
		}
	}
}
