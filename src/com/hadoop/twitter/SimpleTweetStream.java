package com.hadoop.twitter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;

//import com.microsoft.azure.storage.*;
//import com.microsoft.azure.storage.blob.*;





public class SimpleTweetStream {
	
	 private static final String COMMA_DELIMITER = ",";
	 private static final String NEW_LINE_SEPERATOR = "\n";
	 
	 private static final String FILE_HEADER = "DateTime,TwitterUserName,ProfileLocation,TweetContent";
	
	public static final String storageConnectionString =
			"DefaultEndpointsProtocol=http;" +
   "AccountName=wpctweet;" +
					"AccountKey=zSvwbB1RigHM3JKiOXIXSxpMIdBIBKMKtZ1PtsVwFvdBtbCyCafayrifYyaJIYe4Omtt2nA2eC+0v+st+2ecZA==";
	
	public static void main(String args[])   
	{
		 ConfigurationBuilder cb = new ConfigurationBuilder();
	        cb.setDebugEnabled(true);
	        cb.setOAuthConsumerKey("Esx0hYVHYAF8IdAA6x3FwvbFL");
	        cb.setOAuthConsumerSecret("rpWVkmiATXv2NoHWX5m9DMSfvQ4P712bHqsXqJPE0w9sKnckxY");
	        cb.setOAuthAccessToken("133490927-DGKoJARgLOKUvuZGY6ZwDgSUmssH0HWGCqMYAowC");
	        cb.setOAuthAccessTokenSecret("MppAlTnX4vSa6EJWOLAROPr3Wfk1HQEHaKbjjEyduc72r");

	        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

	        StatusListener listener = new StatusListener() {

	            @Override
	            public void onException(Exception arg0) {
	                // TODO Auto-generated method stub

	            }

	            @Override
	            public void onDeletionNotice(StatusDeletionNotice arg0) {
	                // TODO Auto-generated method stub

	            }

	            @Override
	            public void onScrubGeo(long arg0, long arg1) {
	                // TODO Auto-generated method stub

	            }

	            @Override
	            public void onStatus(Status status) {
	                User user = status.getUser();
	                
	                // gets Username
	                String username = status.getUser().getScreenName();
	           //     System.out.println(username);
	                String profilelocation = user.getLocation();
	               System.out.println(profilelocation);
	               long tweetId = status.getId(); 
	                String TweetID = Long.toString(tweetId);
	            //    System.out.println(tweetId);
	           //    String content = status.getText();
	             //    GeoLocation geolocation = status.getGeoLocation();
	            //    double tweetLatitude = geolocation.getLatitude();
	           //    String tweetLat = Double.toString(tweetLatitude);
	          //     System.out.println(tweetLat);
	          //      double tweetLongitude = geolocation.getLatitude();
	        //        String tweetLon = Double.toString(tweetLongitude);
	        //        System.out.println(tweetLon);
	           //     String application = status.getSource();
	           //     System.out.println(application);
	                
	            //    Place placementJSON = status.getPlace();
	           //     System.out.println(placementJSON);
	           //     String placement = placementJSON.getFullName();
	          //      System.out.println(placement);
	                Date time = status.getCreatedAt();
	           //     System.out.println(time);
	                DateFormat df = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
	                String dt = df.format(time);
	                
	           //     System.out.println(tweetLatitude + "," + tweetLongitude +"\n");
	                
	             //   System.out.println(content +"\n");
	                String TweetContent = dt+','+ username+','+ profilelocation +','+ TweetID ;
	               System.out.println(TweetContent);
	                        
	                               
	               
	               
	                
                  try
                  {
                	  
	                File file = new File("D:\\OneDrive\\Sensors\\WPC-Tweets\\WPCTweet.csv");
  	              //    File file = new File("C:\\ASA\\IoTTweetsAzure.csv");
                	PrintWriter out = new PrintWriter (new FileWriter(file,true));
	                BufferedWriter bufferWritter = new BufferedWriter(out);
	              	
	                if(!file.exists()){
	                	file.createNewFile();
	                	bufferWritter.append(FILE_HEADER.toString());
	                	bufferWritter.newLine(); 
                	 	bufferWritter.write(TweetContent);
	                }
	                else
	                {
	                	bufferWritter.newLine();
	                	bufferWritter.write(TweetContent);
	                }
	                
	                 
	               
	                
	               
	              
	                
	             
	                	    
	                
	                bufferWritter.close();
	                
	                
	                //System.out.println("Done");
	                
	                
	            }
                  catch(IOException ex)
                  {
                	  ex.printStackTrace();
                  } 
	             
	          /*    
	             try
	             {
	            	CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
	            	
	            	CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
	            	
	            	CloudBlobContainer container = blobClient.getContainerReference("tweetsblob");
	            	
	            	//container.createIfNotExists();
	            	
	            	BlobContainerPermissions permissions = new BlobContainerPermissions();
	            	
	               permissions.setPublicAccess(BlobContainerPublicAccessType.CONTAINER);
	               
	           //    container.uploadPermissions(permissions);
	            	
	            	final String filePath = "D:\\OneDrive\\Sensors\\WPC-Tweets\\WPCTweet.csv";
	            	
	            	CloudBlockBlob blob = container.getBlockBlobReference(filePath);
	            	
	            //	File source = new File(filePath);
	            	
	            //	blob.upload(new FileInputStream(source), source.length());
	            	
	            	
	            	blob.uploadFromFile(filePath);
	             } 
	             catch(Exception e)
	             {
	            	 e.printStackTrace();
	             } 
	              */  		
	            }
       
	            @Override
	            public void onTrackLimitationNotice(int arg0) {
	                // TODO Auto-generated method stub

	            }

				@Override
				public void onStallWarning(StallWarning arg0) {
					// TODO Auto-generated method stub
					
				}

	        };
	        FilterQuery fq = new FilterQuery();
	    
	        String keywords[] = {"@Azure,#Keynote,#Azure,#Partner,#WPC15,@WPC,#msPartner,#WPCNO15,#WPC2015"};

	        fq.track(keywords);

	        twitterStream.addListener(listener);
	        twitterStream.filter(fq);  

	    }

}
