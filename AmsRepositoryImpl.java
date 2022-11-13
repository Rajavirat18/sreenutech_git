package com.reposiritory.ga;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.group;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.limit;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.match;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.skip;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.unwind;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.project;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.skip;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import com.ams.AdnetHourlyReport;
import com.ams.AmsDynamicData;
import com.ams.AmsRequestVO;
import com.ams.AmsServerLog;
import com.ams.AppUserStats;
import com.ams.DashboardAllTrendzCollection;
import com.ams.TopTenVodChannelReport;
import com.config.AmsConstants;
import com.ams.ReportCollection;
import com.ams.RequestProcessor;
import com.ams.ServiceAssetEventsSummary;
import com.ams.HourlyTrafficAssetWise;
import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.mongodb.WriteResult;
import com.reposiritory.videoutilization.AmsRepository;
import org.springframework.data.mongodb.core.query.Query;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import com.ams.AmsRequest;
@Repository
public class AmsRepositoryImpl implements AmsRepository {

	@Autowired
	private MongoOperations mongoOperations;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private Environment env;
	
	private final static Logger LOGGER= Logger.getLogger(AmsRepositoryImpl.class);
	
	private final static org.slf4j.Logger AmsError =  LoggerFactory.getLogger("amsErrorLog");
	
	public static Map<String, Long> aliveMap = new HashMap<String, Long>();
	
	public static Map<String, String> playStopMap = new HashMap<String, String>();
	
//	public static Multimap<String, Object> multiMap = ArrayListMultimap.create();
	
	public static Map<String, AmsRequest> hMap = new HashMap<String, AmsRequest>();
	public static long playStartTimeStamp = System.currentTimeMillis(); 
	public Set<String> set = null;
	
	@Override
	public void saveAmsRequest(AmsRequest amsRequest) {
		

		
			

			//System.out.println(query1);
//			mongoOperations.remove(query1, AmsRequest.class);
//		} else if(amsRequest.getAction().equalsIgnoreCase("PLAYSTART")){
			/*delete the playStart trap if it received and playstop trap by comparing 
			 the 1.seqNo 2.ipAddress 3.stb*/		
			
//			Query query1 = new Query();
//			query1.addCriteria(Criteria.where("action").is("PLAYSTART").
//					andOperator(Criteria.where("stb").is(amsRequest.getStb())
					/*,Criteria.where("ipAddress").is(amsRequest.getIpAddress())
					,Criteria.where("seqNo").is(amsRequest.getSeqNo())
					,Criteria.where("subject").is(amsRequest.getSubject())*/
//							));
			//System.out.println(query1);
//			mongoOperations.remove(query1, AmsRequest.class);
//		}else if(amsRequest.getAction().equalsIgnoreCase("ALIVE")){
//			/*delete all old ALIVE traps for current STB and insert new ALIVE trap for current STB*/			
//			Query query1 = new Query();
//			query1.addCriteria(Criteria.where("action").is("ALIVE").
//					andOperator(Criteria.where("stb").is(amsRequest.getStb())
//							));			
//			mongoOperations.remove(query1, AmsRequest.class);
//		}
//		else if(amsRequest.getAction().equalsIgnoreCase("BOOTUP")){
//			/*delete all old BOOTUP traps for current STB and insert new BOOTUP trap for current STB*/			
//			Query query1 = new Query();
//			query1.addCriteria(Criteria.where("action").is("BOOTUP").
//					andOperator(Criteria.where("stb").is(amsRequest.getStb())
//							));			
//			mongoOperations.remove(query1, AmsRequest.class);
//		}
		long startTime=System.currentTimeMillis();
		
		Query query1 = new Query();
		
		switch(amsRequest.getAction().toUpperCase()){
		case "PLAYSTOP":
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH:");
			
			/*if endDateTime is less than startDateTime or
			 endDateTime - startDateTime is greater than 24 hours then we are ignoring traps(not processing)*/
			
			long abc = amsRequest.getEndDateTime()-amsRequest.getStartDateTime();
			/*if Command is ADNET then we are not filtering the junk like less than 50 secs or greater than 24 hrs or same stb same 
			asset multiple time skip in an hour will not apply to ADNET command*/
			if(!amsRequest.getCommand().toUpperCase().equalsIgnoreCase("ADNET") && !amsRequest.getCommand().toUpperCase().equalsIgnoreCase("VIDEO_ADNET")){
				if(abc < 50000 || abc > 86400000) {
					playStopMap.put(amsRequest.getStb(), sdf.format(amsRequest.getCreatedDate())+amsRequest.getSubject());
					AmsServerLog.error(new StringBuilder("Skipped Trap ").append(amsRequest.toString()).toString(),null);
					//System.out.println(new StringBuilder("Skipped Trap ").append(amsRequest.toString()).toString());
					return;
				}
				else{
					if(!playStopMap.containsKey(amsRequest.getStb())){
						playStopMap.put(amsRequest.getStb(), sdf.format(amsRequest.getCreatedDate()));
						//System.out.println("put:"+amsRequest.getStb()+":"+playStopMap.get(amsRequest.getStb()));
					}
						//if same stb sent playstop traps with same subject id continuously in same hour then discards such PLAYSTOP Traps. else 
						if(playStopMap.get(amsRequest.getStb()).equalsIgnoreCase(sdf.format(amsRequest.getCreatedDate())+amsRequest.getSubject())){
							AmsServerLog.error(new StringBuilder("Skipped Junk PlayStop Trap ").append(amsRequest.toString()).toString(),null);
							//System.out.println(":"+sdf.format(amsRequest.getCreatedDate())+amsRequest.getSubject()+"Skipped Junk PlayStop Trap "+amsRequest.toString());
							return;
						} else{
							
							//below statement should be last statement in else block
							playStopMap.put(amsRequest.getStb(), sdf.format(amsRequest.getCreatedDate())+amsRequest.getSubject());
							//System.out.println("processing:"+amsRequest.getStb()+":"+playStopMap.get(amsRequest.getStb())+":"+amsRequest.toString());
						}
				}
			}
//			sdf.format(amsRequest.getCreatedDate());
//			playStopMap.put(amsRequest.getStb(), sdf.format(amsRequest.getCreatedDate())+amsRequest.getSubject());
			
			
			//COMMENTING THE CODE TO REDUCE BURDEN ON SERVER WHILE SAVING PLAYSTOP//
			
			/*delete the playStart trap if it received and playstop trap by comparing 
			 the 1.seqNo 2.ipAddress 3.stb*/		
			
			/*query1.addCriteria(Criteria.where("action").is("PLAYSTART").
					andOperator(Criteria.where("stb").is(amsRequest.getStb())
					,Criteria.where("ipAddress").is(amsRequest.getIpAddress())
					,Criteria.where("seqNo").is(amsRequest.getSeqNo())
					,Criteria.where("subject").is(amsRequest.getSubject())
							));
			//System.out.println(query1);
			mongoOperations.remove(query1, AmsRequest.class);*/
			
			break;
		case "DUMMYPLAYSTART":
			if(RequestProcessor.IGNORE_ALIVE_PLAYSTART){
				return;
			}			
			/*delete the playStart trap if it received and playstop trap by comparing 
			 the 1.seqNo 2.ipAddress 3.stb*/
			//commenting already we are setting startDateTime before polling.
			//amsRequest.setStartDateTime(System.currentTimeMillis());
//			long milliSec = System.currentTimeMillis() -amsRequest.getStartDateTime();
//			hMap.put(amsRequest.getStb(), amsRequest);			
			/*System.out.println("hMap count :"+hMap.size());
			System.out.println("System.currentTimeMillis() - playStartTimeStamp count"+(System.currentTimeMillis() - playStartTimeStamp));*/
			if(System.currentTimeMillis() - playStartTimeStamp > 30000){
				set = new HashSet<String>();
				Iterable<AmsRequest> matches = Iterables.filter(hMap.values(), new Predicate<AmsRequest>() {
					@Override
					public boolean apply(AmsRequest input) {
//						System.out.println("System.currentTimeMillis() -input.getStartDateTime() "+(System.currentTimeMillis() -input.getStartDateTime()));
						if ((System.currentTimeMillis() -input.getStartDateTime()) >= 60000) {
							set.add(input.getStb());
							return true;
						} else {
							return false;
						}
					}
				});
				for(AmsRequest ar : matches){
					query1 = new Query();
					query1.addCriteria(Criteria.where("action").is("PLAYSTART").
							andOperator(Criteria.where("stb").is(ar.getStb())
							/*,Criteria.where("ipAddress").is(amsRequest.getIpAddress())
							,Criteria.where("seqNo").is(amsRequest.getSeqNo())
							,Criteria.where("subject").is(amsRequest.getSubject())*/
									));
					//System.out.println(query1);
					mongoOperations.remove(query1, "amsDynamicData");
					ar.setAliveUpdatedTime(System.currentTimeMillis());
					saveAmsRequestDetails(ar, mongoOperations, startTime);	
					startTime = System.currentTimeMillis();// Resetting the start time for each request
				}
				AmsServerLog.error("Play start set size "+set.size(),null);
				playStartTimeStamp = System.currentTimeMillis();//Resetting the play start time stamp
				hMap.keySet().removeAll(set);// Clearing the hash map whose entries are processed(greater than 60 sec)
				AmsServerLog.error("Play start hMap count after deleteing :"+hMap.size(),null);
				hMap.put(amsRequest.getStb(), amsRequest);
				return;
			} else {
				hMap.put(amsRequest.getStb(), amsRequest);
				return;
			}
//			break;// Code not reachable As we are returning before
		case "PLAYSTART":
			if(RequestProcessor.IGNORE_ALIVE_PLAYSTART || (System.currentTimeMillis()-amsRequest.getCreatedDate())/AmsConstants.MINUTE>=30){
				AmsServerLog.error(new StringBuilder("Skipped Old PLAYSTART Trap ").append(amsRequest.toString()).toString(),null);
				return;
			}
			query1 = new Query();
			query1.addCriteria(Criteria.where("action").is("PLAYSTART").
					andOperator(Criteria.where("stb").is(amsRequest.getStb())));
			//System.out.println(query1);
			mongoOperations.remove(query1, "amsDynamicData");
			amsRequest.setAliveUpdatedTime(System.currentTimeMillis());
			//saveAmsRequestDetails(amsRequest, mongoOperations, startTime);	
			break;
		case "ALIVE":
			if(RequestProcessor.IGNORE_ALIVE_PLAYSTART || (System.currentTimeMillis()-amsRequest.getCreatedDate())/AmsConstants.MINUTE>=30){
				AmsServerLog.error(new StringBuilder("Skipped Old ALIVE Trap ").append(amsRequest.toString()).toString(),null);
				return;
			}
			
			 /*Adding stb along with current timestamp to the map.
			   Alive Traps frequency should be greater than 1 minute. If alive traps received frequency is less than
			   1 minute then we will discard those STB related alive traps for next 1 minute */
			if(!aliveMap.containsKey(amsRequest.getStb()) || (System.currentTimeMillis() - aliveMap.get(amsRequest.getStb()) >AmsConstants.MINUTE*10)){
				
				aliveMap.put(amsRequest.getStb(), System.currentTimeMillis());
				//commenting already we are setting endDateTime before polling.
				//amsRequest.setEndDateTime(System.currentTimeMillis());
				/*delete all old ALIVE traps for current STB and insert new ALIVE trap for current STB*/			
				query1.addCriteria(Criteria.where("action").is("ALIVE").
						andOperator(Criteria.where("stb").is(amsRequest.getStb())
								));			
				mongoOperations.remove(query1, "amsDynamicData");
				
				try{
				Query query=new Query();				
				//Updating the PLAYSTART "aliveUpdatedTime" related to this STB.
				query.addCriteria(Criteria.where("action").is("PLAYSTART").andOperator(Criteria.where("stb").is(amsRequest.getStb())));				
				AmsDynamicData ams=mongoOperations.findOne(query, AmsDynamicData.class);
				if(ams!=null){			
					//setting trap received time rather than current time.
					ams.setAliveUpdatedTime(amsRequest.getCreatedDate());
					mongoOperations.save(ams,"amsDynamicData");
				}
				}
				catch (Exception e) {
					AmsError.error("Error in Method: saveAmsRequest"+e.getMessage(), e);					
				}
			} else {
				//Alive traps will be discarded here as they are received with frequency less than 1 minute.
				return;
			}
			
			break;
		case "BOOTUP":
			/*delete all old BOOTUP traps for current STB and insert new BOOTUP trap for current STB*/
			query1.addCriteria(Criteria.where("action").is("BOOTUP").
					andOperator(Criteria.where("stb").is(amsRequest.getStb())
							));			
			mongoOperations.remove(query1, "amsDynamicData");
			
			break;
		case "STANDBY":
			/*Things to be done after receiving standby trap
			1.Convert device playstart trap to playstop.
			2.Remove device ALive trap
			3.Delete and Insert STANDBY Traps*/
			
			//System.out.println("Trap-->"+amsRequest.getAction());
			//1.Convert device playstart trap to playstop
			Query query=new Query();				
			query.addCriteria(Criteria.where("action").is("PLAYSTART").andOperator(Criteria.where("stb").is(amsRequest.getStb())));				
			AmsDynamicData ams=mongoOperations.findOne(query, AmsDynamicData.class);
			//System.out.println("PLAYSTART-->"+ams.getStb());
			if(ams!=null){			
				ams.setAction("PLAYSTOP");			
				ams.setEndDateTime(System.currentTimeMillis());
				mongoOperations.save(ams,"amsTestData");
				mongoOperations.remove(ams, "amsDynamicData");
			}
			
			
			//2.Remove device ALive trap
			query1.addCriteria(Criteria.where("action").is("ALIVE").
					andOperator(Criteria.where("stb").is(amsRequest.getStb())
							));			
			mongoOperations.remove(query1, "amsDynamicData");
			Query query2=new Query();
			//3.Delete and Insert STANDBY Traps
			query2.addCriteria(Criteria.where("action").is("STANDBY").
					andOperator(Criteria.where("stb").is(amsRequest.getStb())
							));			
			mongoOperations.remove(query2, "amsDynamicData");
		break;
		case "INSTALLED":			
			query1.addCriteria(Criteria.where("action").is("UNINSTALLED")
						.andOperator(Criteria.where("appId").is(amsRequest.getAppId()),Criteria.where("stb").is(amsRequest.getStb())
				));
				AppUserStats ap1=mongoOperations.findOne(query1, AppUserStats.class);
				if(ap1!=null){
					ap1.setAction("INSTALLED");
					ap1.setDateTime(System.currentTimeMillis());
					mongoOperations.save(ap1,"AppstoreAmsData");
					mongoOperations.remove(query1,"AppstoreAmsData");					
				}
				else{					
					Query query4=new Query();
					query4.addCriteria(Criteria.where("action").is("INSTALLED")
							.andOperator(Criteria.where("appId").is(amsRequest.getAppId()),Criteria.where("stb").is(amsRequest.getStb())
					));
					AppUserStats ap4=mongoOperations.findOne(query4, AppUserStats.class);
					if(ap4==null){
					mongoOperations.save(amsRequest,"AppstoreAmsData");
					}
				}
			break;				
		case "UNINSTALLED":			
			query1.addCriteria(Criteria.where("action").is("INSTALLED")
					.andOperator(Criteria.where("appId").is(amsRequest.getAppId()),Criteria.where("stb").is(amsRequest.getStb())
			));
			AppUserStats ap2=mongoOperations.findOne(query1, AppUserStats.class);
				if(ap2!=null){
					ap2.setAction("UNINSTALLED");
					ap2.setDateTime(System.currentTimeMillis());
					mongoOperations.save(ap2,"AppstoreAmsData");
					mongoOperations.remove(query1,"AppstoreAmsData");					
				}
				else{
					Query query3=new Query();
					query3.addCriteria(Criteria.where("action").is("UNINSTALLED")
							.andOperator(Criteria.where("appId").is(amsRequest.getAppId()),Criteria.where("stb").is(amsRequest.getStb())
					));
					AppUserStats ap3=mongoOperations.findOne(query3, AppUserStats.class);
					if(ap3==null){
					mongoOperations.save(amsRequest,"AppstoreAmsData");
					}
				}
			break;					
		case "LAUNCHED":
			if(amsRequest.getCommand().equalsIgnoreCase("APP_STORE")){			
				mongoOperations.save(amsRequest,"AppstoreAmsData");
			}
			break;
		}		
		if(amsRequest.getAppCode()==null)
		saveAmsRequestDetails(amsRequest, mongoOperations, startTime);		
	}

	public void saveAmsRequestDetails(AmsRequest amsRequest, MongoOperations mongoOperations2, long startTime) {
		// TODO Auto-generated method stub
		long endTime=System.currentTimeMillis();
		AmsServerLog.error(new StringBuilder("Total Time Took for Case Statement ").append(amsRequest.getAction()).append(":").append(endTime-startTime).toString(),null);
		long startTime1=System.currentTimeMillis();
		int result=0;
		if(StringUtils.isNotBlank(amsRequest.getInfo())){
		String version1=amsRequest.getInfo().split("v")[1];
		String version2=env.getProperty("ams.app_version");
		if (version1.split("\\.").length < version2.split("\\.").length){
			int diff=(version2.split("\\.").length-version1.split("\\.").length);
			for(int i=0;i<diff;i++){
				version1=new StringBuilder(version1).append(".").append("00").toString();	
			}
		}
		if (version1.split("\\.").length > version2.split("\\.").length){
			int diff=(version1.split("\\.").length-version2.split("\\.").length);
			for(int i=0;i<diff;i++){
				version2=new StringBuilder(version2).append(".").append("00").toString();				
			}
			
		}
		//System.out.println("comparing "+version1+" with "+version2);
		String[] v1 = version1.split("\\.");
		String[] v2 = version2.split("\\.");
		
		
		for (int pos = 0; pos < v1.length; pos++) {	    
		    if (Integer.parseInt(v1[pos]) > Integer.parseInt(v2[pos])) {
		    	result=1;
		        break;
		        
		    } else if (Integer.parseInt(v1[pos]) == Integer.parseInt(v2[pos])) {	    	
		    	result=0;
		    }
		    else if (Integer.parseInt(v1[pos]) < Integer.parseInt(v2[pos])) {	    	
		    	result=-1;
		    	break;
		    }
		}
			if(result>=0){
				switch(amsRequest.getAction().toUpperCase()){
				case "PLAYSTART":
				case "BOOTUP":
				case "ALIVE":
				case "STANDBY":
					//System.out.println("SAVEMETHOD:"+amsRequest.getAction().toUpperCase());
					mongoOperations.save(amsRequest,"amsDynamicData");
					break;
				default://could be PLAYSTOP,RESULTS,GENERIC,STANDBY and any other aciton type
					mongoOperations.save(amsRequest);
				}
				/*
				if(amsRequest.getAction().equalsIgnoreCase("PLAYSTOP")){
					mongoOperations.save(amsRequest);			
				}
				else{
					mongoOperations.save(amsRequest,"amsDynamicData");					
				}*/
			}
			else{
				mongoOperations.save(amsRequest,"OldAppsAmsData");
			}
		}
		else{
			mongoOperations.save(amsRequest,"OldAppsAmsData");
		}
		long endTime1=System.currentTimeMillis();
		AmsServerLog.error("Total Time Took for Saving to MongoDb"+(endTime1-startTime1),null);
	}

	@Override
	public List getAmsDataByAction(String actionType, String assetType, String fromDate, String toDate,
			Integer subjectId, Integer offset, Integer max,List<Integer> franchiseIdsLst) {
		Criteria criteria = new Criteria();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		try {
			AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "startDateTime");
			Aggregation aggregation=null;
			Date fromDate1 = null;
			Date toDate1 = null;
			Calendar cal1 = Calendar.getInstance();
			Calendar cal2 = Calendar.getInstance();
			if (StringUtils.isNotBlank(fromDate) && StringUtils.isNotBlank(toDate)) {
				fromDate1 = sdf.parse(fromDate);
				toDate1 = sdf.parse(toDate);
				cal1.setTime(fromDate1);
				cal1.set(Calendar.HOUR_OF_DAY,0);
				cal1.set(Calendar.MINUTE,0);
				cal1.set(Calendar.SECOND,0);
				cal2.setTime(toDate1);
				cal2.set(Calendar.HOUR_OF_DAY,23);
				cal2.set(Calendar.MINUTE,59);
				cal2.set(Calendar.SECOND,59);				
				//System.out.println("from-->" + cal1.getTime() + "to-->" + cal2.getTime());
			}
			if (StringUtils.isNotBlank(actionType) && actionType.equalsIgnoreCase("PLAYSTOP")) {
				List<Criteria> criteriaLst = new ArrayList<Criteria>();
				criteriaLst.add(new Criteria().where("startDateTime").exists(true));
				criteriaLst.add(new Criteria().andOperator(criteria.where("endDateTime").exists(true)));
				criteriaLst.add(new Criteria().where("action").is(actionType));
				criteriaLst.add(new Criteria().where("command").is(assetType));
				criteriaLst.add(new Criteria().where("franchiseId").in(franchiseIdsLst));
				criteriaLst.add(new Criteria().where("createdDate").gte(cal1.getTimeInMillis()).lte(cal2.getTimeInMillis()));
				
				if (subjectId > 0) {
					criteriaLst.add(new Criteria().where("subject").is(subjectId.toString()));					
				}
				criteria.andOperator(criteriaLst.toArray(new Criteria[criteriaLst.size()]));
				aggregation = newAggregation(match(criteria),sort,skip(offset),limit(max)).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
			}
			if (StringUtils.isNotBlank(actionType) && actionType.equalsIgnoreCase("PLAYSTART")) {
				List<Criteria> criteriaLst = new ArrayList<Criteria>();
				criteriaLst.add(new Criteria().where("startDateTime").exists(true));
				criteriaLst.add(new Criteria().where("action").is(actionType));
				criteriaLst.add(new Criteria().where("command").is(assetType));
				criteriaLst.add(new Criteria().where("franchiseId").in(franchiseIdsLst));
				criteriaLst.add(new Criteria().where("createdDate").gte(cal1.getTimeInMillis()).lte(cal2.getTimeInMillis()));
				
				if (subjectId > 0) {
					criteriaLst.add(new Criteria().where("subject").is(subjectId.toString()));
				}
				criteria.andOperator(criteriaLst.toArray(new Criteria[criteriaLst.size()]));
				aggregation = newAggregation(match(criteria),
						sort,skip(offset),limit(max)).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
			}
			List<AmsRequestVO> results=new ArrayList<AmsRequestVO>();
			//System.out.println(aggregation);
			if(StringUtils.isNotBlank(actionType) && actionType.equalsIgnoreCase("PLAYSTART")){
			AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, "amsDynamicData",
					AmsRequestVO.class);
			results = groupResults.getMappedResults();
			}
			else{
				AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, AmsRequest.class,
						AmsRequestVO.class);
				results = groupResults.getMappedResults();
			}
			
			return results;
		} catch (Exception e) {
			//e.printStackTrace();
			AmsError.error("Error in Method: getAmsDataByAction"+e.getMessage(), e);
			return null;
		}
	}

	@Override
	public <S extends AmsRequest> List<S> save(Iterable<S> entites) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<AmsRequest> findAll() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<AmsRequest> findAll(Sort sort) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <S extends AmsRequest> S insert(S entity) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <S extends AmsRequest> List<S> insert(Iterable<S> entities) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Page<AmsRequest> findAll(Pageable pageable) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <S extends AmsRequest> S save(S entity) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AmsRequest findOne(String id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean exists(String id) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Iterable<AmsRequest> findAll(Iterable<String> ids) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long count() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void delete(String id) {
		// TODO Auto-generated method stub

	}

	@Override
	public void delete(AmsRequest entity) {
		// TODO Auto-generated method stub

	}

	@Override
	public void delete(Iterable<? extends AmsRequest> entities) {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteAll() {
		// TODO Auto-generated method stub

	}

	@Override
	public List getAmsDetailsBySubjectTypeandIdentifier(String commandType, String identifier, String fromDate,
			String toDate, Integer offset, Integer max) {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");			
			Date fromDate1 = null;
			Date toDate1 = null;			
			Calendar cal1 = Calendar.getInstance();
			Calendar cal2 = Calendar.getInstance();
			if (StringUtils.isNotBlank(fromDate) && StringUtils.isNotBlank(toDate)) {
				fromDate1 = sdf.parse(fromDate);
				toDate1 = sdf.parse(toDate);
				cal1.setTime(fromDate1);
				cal1.set(Calendar.HOUR_OF_DAY,0);
				cal1.set(Calendar.MINUTE,0);
				cal1.set(Calendar.SECOND,0);
				cal2.setTime(toDate1);
				cal2.set(Calendar.HOUR_OF_DAY,23);
				cal2.set(Calendar.MINUTE,59);
				cal2.set(Calendar.SECOND,59);				
				//System.out.println("from-->" + cal1.getTime() + "to-->" + cal2.getTime());
			}	
			Criteria criteria = new Criteria();
			if (StringUtils.isNotBlank(commandType)) {
				criteria = criteria.where("command").is(commandType)
						.andOperator(criteria.where("createdDate").gte(cal1.getTimeInMillis()).lte(cal2.getTimeInMillis()));
			}
			if (StringUtils.isNotBlank(identifier)) {
				criteria = criteria.where("command").is(commandType).andOperator(criteria.where("stb").regex(identifier,"i")
						.andOperator(criteria.where("createdDate").gte(cal1.getTimeInMillis()).lte(cal2.getTimeInMillis())));
			}
			if (StringUtils.isBlank(identifier) && StringUtils.isBlank(commandType)) {
				criteria = criteria.where("createdDate").gte(cal1.getTimeInMillis()).lte(cal2.getTimeInMillis());
			}
			if (StringUtils.isBlank(commandType) && StringUtils.isNotBlank(identifier)) {
				criteria = criteria.where("stb").regex(identifier,"i")
						.andOperator(criteria.where("createdDate").gte(cal1.getTimeInMillis()).lte(cal2.getTimeInMillis()));
			}
			
			AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "createdDate");
			Aggregation aggregation = newAggregation(match(criteria),sort,skip(offset), limit(max)).
					withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
			AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, AmsRequest.class,
					AmsRequestVO.class);
			List<AmsRequestVO> results = groupResults.getMappedResults();
			//System.out.println(aggregation);
			return results;
		} catch (Exception e) {
			//e.printStackTrace();
			AmsError.error("Error in Method: getAmsDetailsBySubjectTypeandIdentifier"+e.getMessage(), e);
			return null;
		}
	}

	@Override
	public long getAmsDetailsCountByCommandType(String commandType, String identifier, String fromDate, String toDate) {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date fromDate1 = null;
			Date toDate1 = null;
			Calendar cal1 = Calendar.getInstance();
			Calendar cal2 = Calendar.getInstance();
			if (StringUtils.isNotBlank(fromDate) && StringUtils.isNotBlank(toDate)) {
				fromDate1 = sdf.parse(fromDate);
				toDate1 = sdf.parse(toDate);
				cal1.setTime(fromDate1);
				cal1.set(Calendar.HOUR_OF_DAY,0);
				cal1.set(Calendar.MINUTE,0);
				cal1.set(Calendar.SECOND,0);
				cal2.setTime(toDate1);
				cal2.set(Calendar.HOUR_OF_DAY,23);
				cal2.set(Calendar.MINUTE,59);
				cal2.set(Calendar.SECOND,59);				
				//System.out.println("from-->" + cal1.getTime() + "to-->" + cal2.getTime());
			}	
			Criteria criteria = new Criteria();
			if (StringUtils.isNotBlank(commandType)) {
				criteria = criteria.where("command").is(commandType)
						.andOperator(criteria.where("createdDate").gte(cal1.getTimeInMillis()).lte(cal2.getTimeInMillis()));
			}
			if (StringUtils.isNotBlank(identifier)) {
				criteria = criteria.where("command").is(commandType).andOperator(criteria.where("stb").regex(identifier,"i")
						.andOperator(criteria.where("createdDate").gte(cal1.getTimeInMillis()).lte(cal2.getTimeInMillis())));
			}
			if (StringUtils.isBlank(identifier) && StringUtils.isBlank(commandType)) {
				criteria = criteria.where("createdDate").gte(cal1.getTimeInMillis()).lte(cal2.getTimeInMillis());
			}
			if (StringUtils.isBlank(commandType) && StringUtils.isNotBlank(identifier)) {
				criteria = criteria.where("stb").regex(identifier,"i")
						.andOperator(criteria.where("createdDate").gte(cal1.getTimeInMillis()).lte(cal2.getTimeInMillis()));
			}
			Query query = new Query();
			query.addCriteria(criteria);
			long totalcount = mongoOperations.count(query, AmsRequest.class);
			//System.out.println(query);
			//AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "createdDate");					
			/*Aggregation aggregation = newAggregation(match(criteria),group("createdDate")).withOptions((Aggregation.newAggregationOptions().allowDiskUse(true).build()));
			
			AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, AmsRequest.class,
					AmsRequestVO.class);*/
			
			//List<AmsRequestVO> results = groupResults.getMappedResults();
			//System.out.println("size"+totalcount);
			return totalcount;
		} catch (Exception e) {
			//e.printStackTrace();
			AmsError.error("Error in Method: getAmsDetailsCountByCommandType"+e.getMessage(), e);
			return 0l;
		}
	}

	@Override
	public long getAmsDataCountByAction(String actionType, String assetType, String fromDate, String toDate,
			Integer subjectId,List<Integer>franchiseIdsLst) {
		long totalcount=0l;
		Criteria criteria = new Criteria();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		try {
			Date fromDate1 = null;
			Date toDate1 = null;
			Calendar cal1 = Calendar.getInstance();
			Calendar cal2 = Calendar.getInstance();
			if (StringUtils.isNotBlank(fromDate) && StringUtils.isNotBlank(toDate)) {
				fromDate1 = sdf.parse(fromDate);
				toDate1 = sdf.parse(toDate);
				cal1.setTime(fromDate1);
				cal1.set(Calendar.HOUR_OF_DAY,0);
				cal1.set(Calendar.MINUTE,0);
				cal1.set(Calendar.SECOND,0);
				cal2.setTime(toDate1);
				cal2.set(Calendar.HOUR_OF_DAY,23);
				cal2.set(Calendar.MINUTE,59);
				cal2.set(Calendar.SECOND,59);				
				//System.out.println("from-->" + cal1.getTime() + "to-->" + cal2.getTime());
			}					
			if (StringUtils.isNotBlank(actionType) && actionType.equalsIgnoreCase("PLAYSTOP")) {
				List<Criteria> criteriaLst = new ArrayList<Criteria>();
				criteriaLst.add(new Criteria().where("startDateTime").exists(true));
				criteriaLst.add(new Criteria().andOperator(criteria.where("endDateTime").exists(true)));
				criteriaLst.add(new Criteria().where("action").is(actionType));
				criteriaLst.add(new Criteria().where("command").is(assetType));
				criteriaLst.add(new Criteria().where("franchiseId").in(franchiseIdsLst));
				criteriaLst.add(new Criteria().where("createdDate").gte(cal1.getTimeInMillis()).lte(cal2.getTimeInMillis()));
				
				if (subjectId > 0) {
					criteriaLst.add(new Criteria().where("subject").is(subjectId.toString()));					
				}
				criteria.andOperator(criteriaLst.toArray(new Criteria[criteriaLst.size()]));
				Query query = new Query();
				query.addCriteria(criteria);
				totalcount = mongoOperations.count(query, AmsRequest.class);
				
			}
			if (StringUtils.isNotBlank(actionType) && actionType.equalsIgnoreCase("PLAYSTART")) {
				List<Criteria> criteriaLst = new ArrayList<Criteria>();
				criteriaLst.add(new Criteria().where("startDateTime").exists(true));
				criteriaLst.add(new Criteria().where("action").is(actionType));
				criteriaLst.add(new Criteria().where("command").is(assetType));
				criteriaLst.add(new Criteria().where("franchiseId").in(franchiseIdsLst));
				criteriaLst.add(new Criteria().where("createdDate").gte(cal1.getTimeInMillis()).lte(cal2.getTimeInMillis()));
				
				if (subjectId > 0) {
					criteriaLst.add(new Criteria().where("subject").is(subjectId.toString()));
				}
				criteria.andOperator(criteriaLst.toArray(new Criteria[criteriaLst.size()]));
				Query query = new Query();
				query.addCriteria(criteria);
				totalcount = mongoOperations.count(query, "amsDynamicData");
			}
			return totalcount;
		} catch (Exception e) {
			//e.printStackTrace();
			AmsError.error("Error in Method: getAmsDataCountByAction"+e.getMessage(), e);
			return 0l;
		}
		
	}

	@Override
	public List getAmsAliveDataByAction(String actionType, String status, String identifier, String deviceType,
			long noOfSecondsToConsider, Integer offset, Integer max,List<Integer> franchiseIdsLst) {
		Criteria criteria = new Criteria();
		Criteria criteria1 = new Criteria();

		
		if (StringUtils.isNotBlank(actionType) && StringUtils.isNotBlank(status)) {
			if (status.equalsIgnoreCase("online")) {
				criteria = criteria.where("action").is(actionType).andOperator(
						criteria.where("command").is("SYSTEM"),criteria.where("franchiseId").in(franchiseIdsLst)
						,criteria.where("stb").regex(identifier, "i").
						andOperator(criteria.where("subDeviceType").regex(deviceType, "i")),
						criteria.where("endDateTime").gte(noOfSecondsToConsider));
				//criteria1 = criteria1.where("endDateTime").gte(noOfSecondsToConsider);
			}
			if (status.equalsIgnoreCase("offline")) {
				criteria = criteria.where("action").is(actionType).andOperator(
						  criteria.where("command").is("SYSTEM"),criteria.where("franchiseId").in(franchiseIdsLst)
						  ,criteria.where("stb")
						.regex(identifier, "i").andOperator(criteria.where("subDeviceType").regex(deviceType, "i")),
						criteria.where("endDateTime").lte(noOfSecondsToConsider));
				//criteria1 = criteria1.where("endDateTime").lte(noOfSecondsToConsider);
			}
		}

		AggregationOperation sort = Aggregation.sort(Sort.Direction.ASC, "createdDate");
		Aggregation aggregation = newAggregation(match(criteria),
				/*group("stb").last("stb").as("stb").last("seqNo").as("seqNo").last("action").as("action").last("command")
						.as("command").last("ipAddress").as("ipAddress").last("sessionId").as("sessionId")
						.last("subDeviceType").as("subDeviceType").last("subDeviceOS").as("subDeviceOS").last("subject")
						.as("subject").last("startDateTime").as("startDateTime").last("endDateTime").as("endDateTime").
						last("createdDate").as("createdDate"),*/
						sort,skip(offset),limit(max)).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
		//System.out.println(aggregation);
		AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, "amsDynamicData",
				AmsRequestVO.class);
		
		List<AmsRequestVO> results = groupResults.getMappedResults();
		return results;
	}

	@Override
	public long getAmsAliveDataCountByAction(String actionType, String status, String identifier, String deviceType,
			long noOfSecondsToConsider,List<Integer> franchiseIdsLst) {
		Criteria criteria = new Criteria();
		Criteria criteria1 = new Criteria();
		long totalcount=0l;
		if (StringUtils.isNotBlank(actionType) && StringUtils.isNotBlank(status)) {
			if (status.equalsIgnoreCase("online")) {
				criteria = criteria.where("action").is(actionType).andOperator(criteria.where("franchiseId").in(franchiseIdsLst),
						criteria.where("stb")
						.regex(identifier, "i").andOperator(criteria.where("subDeviceType").regex(deviceType, "i")),
						criteria.where("endDateTime").gte(noOfSecondsToConsider));
				//criteria1 = criteria1.where("endDateTime").gte(noOfSecondsToConsider);
			}
			if (status.equalsIgnoreCase("offline")) {
				criteria = criteria.where("action").is(actionType).andOperator(criteria.where("franchiseId").in(franchiseIdsLst),
						criteria.where("stb")
						.regex(identifier, "i").andOperator(criteria.where("subDeviceType").regex(deviceType, "i")),
						criteria.where("endDateTime").lte(noOfSecondsToConsider));
				//criteria1 = criteria1.where("endDateTime").lte(noOfSecondsToConsider);
			}
		}

		Query query = new Query();
		query.addCriteria(criteria);
		totalcount = mongoOperations.count(query, "amsDynamicData");
		
		/*AggregationOperation sort = Aggregation.sort(Sort.Direction.ASC, "createdDate");
		Aggregation aggregation = newAggregation(match(criteria),
				group("stb")).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
		AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, AmsRequest.class,
				AmsRequestVO.class);		
		List<AmsRequestVO> results = groupResults.getMappedResults();
		if(results!=null){
		totalcount=results.size();
		}
		*/
		//System.out.println(aggregation);
		return totalcount;
	}

	@Override
	public List<AmsRequestVO> getonlineAssetUsersCount(String commandType,Integer noOfSecondsForAlive) {
		Calendar nofSecAliveUpdated = Calendar.getInstance();
		nofSecAliveUpdated.add(Calendar.SECOND, - noOfSecondsForAlive);
		
		Criteria criteria = new Criteria();
		criteria=criteria.where("action").is("PLAYSTART").andOperator(criteria.where("command").is(commandType)
				,criteria.where("aliveUpdatedTime").gte(nofSecAliveUpdated.getTimeInMillis()));

		Aggregation aggregation = newAggregation(match(criteria),group("franchiseId").count()
				.as("views").last("franchiseId").as("franchiseId"));
		AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, "amsDynamicData",
				AmsRequestVO.class);		
		List<AmsRequestVO> results = groupResults.getMappedResults();
		
		return results;
	}

	@Override
	public List<AmsRequestVO> gethourlyTrafficCount(long fromDate, long toDate,String commandType) {
		Criteria criteria = new Criteria();
		
		//if the difference between current time and toDate is lessthan 20 minutes we are replacing toDate with currentTimeStamp
		if(System.currentTimeMillis() - toDate < 1200000){ 
			toDate = System.currentTimeMillis();			
		}
		List<Criteria> criteriaLst = new ArrayList<Criteria>();
		criteriaLst.add(new Criteria().where("createdDate").gte(fromDate).lte(toDate));
		if(!commandType.equalsIgnoreCase("ALIVE")){
			criteriaLst.add(new Criteria().where("command").is(commandType));
		}
		else{
			criteriaLst.add(new Criteria().where("action").is(commandType));
		}
		criteria.andOperator(criteriaLst.toArray(new Criteria[criteriaLst.size()]));
		//criteria=criteria.where("createdDate").gte(fromDate).lte(toDate).andOperator(criteria.where("command").is(commandType));
		Aggregation aggregation = newAggregation(match(criteria), group("stb").count().as("views")).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
		AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, AmsDynamicData.class,
				AmsRequestVO.class);
		List<AmsRequestVO> results = groupResults.getMappedResults();
		return results;
	}

	@Override
	public List<AmsDynamicData> getPlayStartRecordsByNofSeconds(Long noOfMillisAlive) {
		Criteria criteria = new Criteria();		
		Query query=new Query();
		criteria=criteria.where("action").is("PLAYSTART").andOperator(criteria.where("aliveUpdatedTime").lte(noOfMillisAlive));
		query.addCriteria(criteria);
		List<AmsDynamicData> results=mongoOperations.find(query, AmsDynamicData.class);
		/*Aggregation aggregation = newAggregation(match(criteria),
				group("stb").last("stb").as("stb").last("seqNo").as("seqNo").last("action").as("action").last("command")
						.as("command").last("ipAddress").as("ipAddress").last("sessionId").as("sessionId")
						.last("subDeviceType").as("subDeviceType").last("subDeviceOS").as("subDeviceOS").last("subject")
						.as("subject").last("startDateTime").as("startDateTime").last("endDateTime").as("endDateTime")).
				withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
		AggregationResults<AmsRequest> groupResults = mongoTemplate.aggregate(aggregation, AmsRequest.class,
				AmsRequest.class);
		List<AmsRequest> results = groupResults.getMappedResults();
		*/
		return results;
	}

	@Override
	public void updateInactivePlayStartasPlayStop(String stb) {
		Query query = new Query();
		query.addCriteria(Criteria.where("action").is("PLAYSTART").andOperator(Criteria.where("stb").is(stb)));

		AmsDynamicData ams = mongoOperations.findOne(query, AmsDynamicData.class);
		if(ams!=null){
			Long duration=0l;
			//calculating duration based on currentTime-startDateTime
			Date strDate=new Date();
        	Date endDate=new Date();
        	if(ams.getStartDateTime()!=null){
        	   strDate = new Date(ams.getStartDateTime());
        	}
        	duration  = (endDate.getTime() - strDate.getTime())/1000;   
			ams.setAction("PLAYSTOP");			
			ams.setEndDateTime(System.currentTimeMillis());
			//
			long abc = ams.getEndDateTime()-ams.getStartDateTime();			
			if (abc < 50000 || abc > 86400000) {
				Query query2 = new Query();
				query2.addCriteria(
						Criteria.where("action").is("PLAYSTART").andOperator(Criteria.where("stb").is(ams.getStb())));
				mongoOperations.remove(query2, "amsDynamicData");
				AmsServerLog.error(new StringBuilder("Removed Trap ").append(ams.toString()).toString(), null);
			}
			else{
				mongoOperations.save(ams,"amsTestData");
				mongoOperations.remove(ams, "amsDynamicData");
			}
		}
		//modify and update with save()		
	}

	@Override
	public List<DashboardAllTrendzCollection> topVideoTrendzByType(String commandType,List<Integer> franchiseIdsLst) {
		/*Calendar c24HrBack = Calendar.getInstance();
		c24HrBack.add(Calendar.HOUR, -24);
		Criteria criteria = new Criteria();
		criteria=criteria.where("action").is("PLAYSTOP").andOperator(criteria.where("command").is(commandType)
				,(criteria.where("startDateTime").gte(c24HrBack.getTimeInMillis())
				.orOperator(criteria.where("endDateTime").gte(c24HrBack.getTimeInMillis()))));
		AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "views");
		
		if(!commandType.equalsIgnoreCase("CATCHUP")){
		Aggregation aggregation = newAggregation(match(criteria), 
				group("subject").count().as("views"),sort,limit(20)).
				withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
		AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, AmsRequest.class,
				AmsRequestVO.class);
		List<AmsRequestVO> results = groupResults.getMappedResults();
		//System.out.println(aggregation);
		return results;
		}
		else{
			Aggregation aggregation = newAggregation(match(criteria), 
					group("programId").count().as("views"),sort,limit(20)).
					withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
			AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, AmsRequest.class,
					AmsRequestVO.class);
			List<AmsRequestVO> results = groupResults.getMappedResults();			
			return results;
		}*/
		Criteria criteria = new Criteria();
		criteria=criteria.where("command").is(commandType)/*.andOperator(
				criteria.where("franchiseId").in(franchiseIdsLst))*/;
		AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "views");
		Aggregation aggregation = newAggregation(match(criteria), 
				group("subject").sum("views").as("views"),sort,limit(20)).
				withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
		AggregationResults<DashboardAllTrendzCollection> groupResults = mongoTemplate.aggregate(aggregation, DashboardAllTrendzCollection.class,
				DashboardAllTrendzCollection.class);
		List<DashboardAllTrendzCollection> results = groupResults.getMappedResults();		
		return results;	
	}

	@Override
	public List<AmsRequestVO> getappVersionInDetailsByIdentifierandVersionType(String versionType, String identifier,
			Integer offset, Integer max,String buildNumber) {
		Criteria criteria = new Criteria();		
		criteria=criteria.where("action").is("BOOTUP").andOperator(criteria.where("info").exists(true),
				 criteria.where("stb").regex(identifier,"i"),
				 criteria.where("info").regex(versionType,"i"),
				 criteria.where("build").regex(buildNumber,"i"));
	
		AggregationOperation sort = Aggregation.sort(Sort.Direction.ASC, "startDateTime");
		Aggregation aggregation = newAggregation(match(criteria), 
				group("stb").last("stb").as("stb").last("info").as("info").last("createdDate").as("createdDate")
				.last("build").as("build"),
				skip(offset), limit(max)).
				withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
		AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, "amsDynamicData",
				AmsRequestVO.class);
		List<AmsRequestVO> results = groupResults.getMappedResults();
		//System.out.println(aggregation);
		
		return results;
	}

	@Override
	public List<AmsRequestVO> getappVersionInDetailsCountByIdentifierandVersionType(String versionType,
			String identifier,String buildNumber) {
		Criteria criteria = new Criteria();		
		criteria=criteria.where("action").is("BOOTUP").andOperator(criteria.where("info").exists(true),
				 criteria.where("stb").regex(identifier,"i"),
				 criteria.where("info").regex(versionType,"i"),
				 criteria.where("build").regex(buildNumber,"i"));
	
		AggregationOperation sort = Aggregation.sort(Sort.Direction.ASC, "startDateTime");
		Aggregation aggregation = newAggregation(match(criteria),
				group("stb")).
				withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
		AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, "amsDynamicData",
				AmsRequestVO.class);
		List<AmsRequestVO> results = groupResults.getMappedResults();
		//System.out.println(aggregation);
		
		return results;
	}

	@Override
	public void saveAmsRequestVO(AmsRequestVO amsRequest) {
		mongoOperations.save(amsRequest);
		
	}

	@Override
	public List<AmsRequestVO> removeOldAliveTrapsByDate(long fromDate) {
		Criteria criteria = new Criteria();
		criteria=criteria.where("action").is("ALIVE").andOperator(Criteria.where("createdDate").lt(fromDate));
		Aggregation aggregation = newAggregation(match(criteria));
		AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, AmsRequest.class,
				AmsRequestVO.class);
		List<AmsRequestVO> results = groupResults.getMappedResults();		
		Query query1 = new Query();
		query1.addCriteria(Criteria.where("action").is("ALIVE").
				andOperator(Criteria.where("createdDate").lt(fromDate)));
		mongoOperations.remove(query1, AmsRequest.class);
		return results;
	}

	@Override
	public List<AmsRequestVO> getEstimatedMinutesWatchedByDateAndAssetType(String fromDate, String toDate,
			List<String> serviceAssetIds,String commandType) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date fromDate1 = null;
		Date toDate1 = null;
		Calendar cal1 = Calendar.getInstance();
		Calendar cal2 = Calendar.getInstance();
		try{
		if (StringUtils.isNotBlank(fromDate) && StringUtils.isNotBlank(toDate)) {
			fromDate1 = sdf.parse(fromDate);
			toDate1 = sdf.parse(toDate);
			cal1.setTime(fromDate1);
			cal1.set(Calendar.HOUR_OF_DAY,0);
			cal1.set(Calendar.MINUTE,0);
			cal1.set(Calendar.SECOND,0);
			cal2.setTime(toDate1);
			cal2.set(Calendar.HOUR_OF_DAY,23);
			cal2.set(Calendar.MINUTE,59);
			cal2.set(Calendar.SECOND,59);				
			//System.out.println("from-->" + cal1.getTime() + "to-->" + cal2.getTime());
		}	
		//for apsfl endDateTime - startDateTime for 178 durationInSec
		Criteria criteria = new Criteria();
		criteria=criteria.where("action").is("PLAYSTOP").andOperator(criteria.where("command").is(commandType)
				,criteria.where("createdDate").gt(cal1.getTimeInMillis()).lte(cal2.getTimeInMillis()),
				criteria.where("subject").in(serviceAssetIds));
		AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "duration");
		Aggregation aggregation = newAggregation(match(criteria),project("subject").andExpression("endDateTime - startDateTime").as("duration"),
				group("subject").sum("duration").as("duration").last("subject").as("subject"),sort);
		
		AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, AmsRequest.class,
				AmsRequestVO.class);
		List<AmsRequestVO> results = groupResults.getMappedResults();
		//System.out.println(results);
		return results;
		}
		catch (Exception e) {
			//e.printStackTrace();
			AmsError.error("Error in Method: getEstimatedMinutesWatchedByDateAndAssetType"+e.getMessage(), e);
			return null;
		}
		
	}

	@Override
	public List<AmsRequestVO> getdistinctAppVersions() {		
		Criteria criteria = new Criteria();
		criteria=criteria.where("action").is("BOOTUP").andOperator(criteria.where("info").ne(""));
		AggregationOperation sort = Aggregation.sort(Sort.Direction.ASC, "info");
		Aggregation aggregation = newAggregation(match(criteria),group("info").last("info").as("info"),sort).
				withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());		
		AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, "amsDynamicData",
				AmsRequestVO.class);
		List<AmsRequestVO> results = groupResults.getMappedResults();		
		return results;
	}

	@Override
	public AmsRequest getAliveStbByActionAndNoMinutes(String action, Long noOfMillisAlive, String stb) {
		Query query = new Query();
		Criteria criteria = new Criteria();
		criteria=criteria.where("action").is(action).andOperator(criteria.where("endDateTime").gte(noOfMillisAlive)
				,criteria.where("stb").is(stb));
		query.addCriteria(criteria);
		Aggregation aggregation = newAggregation(match(criteria),
				group("stb").last("stb").as("stb").last("seqNo").as("seqNo").last("action").as("action").last("command")
						.as("command").last("ipAddress").as("ipAddress").last("sessionId").as("sessionId")
						.last("subDeviceType").as("subDeviceType").last("subDeviceOS").as("subDeviceOS").last("subject")
						.as("subject").last("startDateTime").as("startDateTime").last("endDateTime").as("endDateTime"),limit(1)).
				withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
		AggregationResults<AmsRequest> groupResults = mongoTemplate.aggregate(aggregation, AmsRequest.class,
				AmsRequest.class);
		AmsRequest results = mongoOperations.findOne(query, AmsRequest.class);
		//AmsRequest results = (AmsRequest) groupResults.getMappedResults();
		return results;
	}

	@Override
	public List<AmsRequestVO> getAssetViewershipByDateAndAssetType(long fromDate, long toDate, String assetId,Integer offset,Integer max) {
		Criteria criteria = new Criteria();
		/*criteria = criteria.where("action").is("PLAYSTOP").andOperator(criteria.where("subject").is(assetId),
				criteria.where("createdDate").gt(fromDate).lte(toDate));*/
		
		criteria =criteria.where("subject").is(assetId).andOperator(
				criteria.where("createdDate").gt(fromDate).lte(toDate),				  
				  criteria.where("action").is("PLAYSTOP")
				);
		
		AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "duration");
		Aggregation aggregation = newAggregation(match(criteria),
				project("subject").andExpression("endDateTime - startDateTime").as("duration").andExpression("stb")
						.as("stb"),
				group("stb").sum("duration").as("duration").last("subject").as("subject").last("stb").as("stb")
						,sort,skip(offset),limit(max));
		
		AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, AmsRequest.class,
				AmsRequestVO.class);
		List<AmsRequestVO> results = groupResults.getMappedResults();		
		return results;
	}

	@Override
	public List<AmsRequestVO> getUserViewershipByDateAndStb(long fromDate, long toDate, String stb,Integer offset,Integer max) {
		Criteria criteria = new Criteria();		
		/*criteria = criteria.where("action").is("PLAYSTOP").andOperator(criteria.where("stb").regex(stb,"i"),
				criteria.where("createdDate").gt(fromDate).lte(toDate));*/
		
		criteria =criteria.where("createdDate").gt(fromDate).lte(toDate).andOperator(criteria.where("action").is("PLAYSTOP"),
				criteria.where("stb").is(stb));
		AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "duration");
		Aggregation aggregation = newAggregation(match(criteria),
				project("subject").andExpression("endDateTime - startDateTime").as("duration").andExpression("stb")
						.as("stb"),
				group("subject").sum("duration").as("duration").last("subject").as("subject").last("stb").as("stb")
						,sort,skip(offset),limit(max)).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
		
		AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, AmsRequest.class,
				AmsRequestVO.class);
		List<AmsRequestVO> results = groupResults.getMappedResults();
		return results;
		}

	@Override
	public long getAssetViewershipCountByDateAndAssetType(long fromDate, long toDate, String assetId) {
		long count=0l;
		Criteria criteria = new Criteria();
		/*criteria.where("action").is("PLAYSTOP").andOperator(criteria.where("subject").is(assetId),
				criteria.where("createdDate").gt(fromDate).lte(toDate));*/
		criteria = criteria.where("createdDate").gt(fromDate).lte(toDate).andOperator(
					criteria.where("subject").is(assetId),
				   criteria.where("action").is("PLAYSTOP")
				   );
		
		Aggregation aggregation = newAggregation(match(criteria),
				/*project("subject").andExpression("endDateTime - startDateTime").as("duration").andExpression("stb")
						.as("stb"),*/
				group("stb")/*.sum("duration").as("duration").last("subject").as("subject").last("stb").as("stb")*/)
				.withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
		
		AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, AmsRequest.class,
				AmsRequestVO.class);
		List<AmsRequestVO> results = groupResults.getMappedResults();
		if(results!=null){
			count=results.size();
		}
		return count;
	}

	@Override
	public long getUserViewershipCountByDateAndStb(long fromDate, long toDate, String stb) {
		long count=0l;
		Criteria criteria = new Criteria();		
		/*criteria = criteria.where("action").is("PLAYSTOP").andOperator(criteria.where("stb").regex(stb,"i"),
				criteria.where("createdDate").gt(fromDate).lte(toDate));
		*/
		criteria =criteria.where("createdDate").gt(fromDate).lte(toDate).andOperator(criteria.where("action").is("PLAYSTOP"),
				criteria.where("stb").is(stb));
		
		Aggregation aggregation = newAggregation(match(criteria),
				/*project("subject").andExpression("endDateTime - startDateTime").as("duration").andExpression("stb")
						.as("stb"),*/
				group("subject")/*.sum("duration").as("duration").last("subject").as("subject").last("stb").as("stb")*/)
				.withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
		
		AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, AmsRequest.class,
				AmsRequestVO.class);
		List<AmsRequestVO> results = groupResults.getMappedResults();
		if(results!=null){
			count=results.size();
		}
		return count;
	}

	@Override
	public List<AmsRequestVO> getUserViewershipByDate(String fromDate, String toDate) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date fromDate1 = null;
		Date toDate1 = null;
		Calendar cal1 = Calendar.getInstance();
		Calendar cal2 = Calendar.getInstance();
		
		try {
			if (StringUtils.isNotBlank(fromDate) && StringUtils.isNotBlank(toDate)) {
				fromDate1 = sdf.parse(fromDate);
				toDate1 = sdf.parse(toDate);
				cal1.setTime(fromDate1);
				cal1.set(Calendar.HOUR_OF_DAY, 0);
				cal1.set(Calendar.MINUTE, 0);
				cal1.set(Calendar.SECOND, 0);
				cal2.setTime(toDate1);
				cal2.set(Calendar.HOUR_OF_DAY, 23);
				cal2.set(Calendar.MINUTE, 59);
				cal2.set(Calendar.SECOND, 59);
			}
				
		Criteria criteria = new Criteria();		
		criteria = criteria.where("action").is("PLAYSTOP").andOperator(criteria.where("createdDate")
				.gt(cal1.getTimeInMillis()).lte(cal2.getTimeInMillis()));
				
		Aggregation aggregation = newAggregation(match(criteria),
				project("subject").andExpression("endDateTime - startDateTime").as("duration")
				.andExpression("command").as("command"),
				group("subject").sum("duration").as("duration").last("subject").as("subject").
				last("command").as("command"))
				.withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
		
		AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, AmsRequest.class,
				AmsRequestVO.class);
		List<AmsRequestVO> results = groupResults.getMappedResults();
		return results;
		}
		catch (Exception e) {
			AmsError.error("Error in Method: getUserViewershipByDate" + e.getMessage(), e);
			return null;
		}		
	}

	@Override
	public ReportCollection getRecentDatefromReportCollection(String assetType) {
		Criteria criteria = new Criteria();
		criteria = criteria.where("assetType").is(assetType);
		Query query = new Query();
		query.addCriteria(criteria);
		query.limit(1);
		query.with(new Sort(Sort.Direction.DESC, "date"));
		ReportCollection ams = mongoOperations.findOne(query, ReportCollection.class,"reportCollection");		
		return ams;
	}

	@Override
	public void saveReportCollection(ReportCollection reportCollection) {
		mongoOperations.save(reportCollection,"reportCollection");		
	}

	@Override
	public List<ReportCollection> getReportCollectionByDate(String fromDate, String toDate,String assetType,List<String> serviceAssetIds,Integer offset, Integer max) {				
		Criteria criteria = new Criteria();		
		criteria = criteria.where("assetType").is(assetType).andOperator(criteria.where("date")
				.gte(fromDate).lte(toDate),criteria.where("assetId").in(serviceAssetIds));
		
		AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "duration");
		
		Aggregation aggregation = newAggregation(match(criteria),				
				group("assetId").sum("duration").as("duration").last("date").as("date").
				last("assetId").as("assetId").last("assetType").as("assetType"),sort,skip(offset),limit(max))
				.withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
		
		AggregationResults<ReportCollection> groupResults = mongoTemplate.aggregate(aggregation, ReportCollection.class,
				ReportCollection.class);
		//System.out.println(aggregation);
		List<ReportCollection> results = groupResults.getMappedResults();		
		return results;				
	}

	@Override
	public long getReportCollectionCountByDate(String fromDate, String toDate, String assetType,List<String> serviceAssetIds) {
		long count = 0l;
		Criteria criteria = new Criteria();
		criteria = criteria.where("assetType").is(assetType).andOperator(criteria.where("date")
				.gte(fromDate).lte(toDate),criteria.where("assetId").in(serviceAssetIds));

		Aggregation aggregation = newAggregation(match(criteria), group("assetId").sum("duration").as("duration")
				.last("date").as("date").last("assetId").as("assetId").last("assetType").as("assetType"))
				.withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());

		AggregationResults<ReportCollection> groupResults = mongoTemplate.aggregate(aggregation, ReportCollection.class,
				ReportCollection.class);
		List<ReportCollection> results = groupResults.getMappedResults();
		if (results != null) {count = results.size();}
		return count;
	}
	

	@Override
	public List<TopTenVodChannelReport> getTopVodChannelReportByDateAndAssetType(String fromDate, String toDate,
			List<String> serviceAssetIds, String commandType, String nofRecords) {
		try {						
			
			Criteria criteria = new Criteria();
			criteria = criteria.where("commandType").is(commandType).andOperator(
					criteria.where("createdDate").gte(fromDate).lte(toDate),
					criteria.where("subject").in(serviceAssetIds));
			
			AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "views");
			Aggregation aggregation = newAggregation(match(criteria), 
					group("subject").sum("views").as("views"),sort,limit(Integer.parseInt(nofRecords))).
					withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
			
			AggregationResults<TopTenVodChannelReport> groupResults = mongoTemplate.aggregate(aggregation, TopTenVodChannelReport.class,
					TopTenVodChannelReport.class);			
			List<TopTenVodChannelReport> results = groupResults.getMappedResults();
			
			/*AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "views");			
			Aggregation aggregation = newAggregation(match(criteria),				
					sort,limit(Integer.parseInt(nofRecords))).
					withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());*/			
			return results;
		} catch (Exception e) {
			AmsError.error("Error in Method: getTopVodChannelReportByDateAndAssetType" + e.getMessage(), e);
			return null;
		}
	}

	@Override
	public TopTenVodChannelReport getRecentDatefromTopTenVodChannelReport(String commandType) {
		Query query = new Query();
		Criteria criteria = new Criteria();
		criteria = criteria.where("commandType").is(commandType);
		query.addCriteria(criteria);
		query.limit(1);
		query.with(new Sort(Sort.Direction.DESC, "createdDate"));
		TopTenVodChannelReport dba = mongoOperations.findOne(query, TopTenVodChannelReport.class,"topTenVodChannelReport");		
		return dba;
	}

	@Override
	public List<AmsRequestVO> topTenVideoChannelReport(String fromDate, String toDate,String commandType) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date fromDate1 = null;
		Date toDate1 = null;

		Calendar cal1 = Calendar.getInstance();
		Calendar cal2 = Calendar.getInstance();
		try {
			if (StringUtils.isNotBlank(fromDate) && StringUtils.isNotBlank(toDate)) {
				fromDate1 = sdf.parse(fromDate);
				toDate1 = sdf.parse(toDate);
				cal1.setTime(fromDate1);
				cal1.set(Calendar.HOUR_OF_DAY, 0);
				cal1.set(Calendar.MINUTE, 0);
				cal1.set(Calendar.SECOND, 0);
				cal2.setTime(toDate1);
				cal2.set(Calendar.HOUR_OF_DAY, 23);
				cal2.set(Calendar.MINUTE, 59);
				cal2.set(Calendar.SECOND, 59);
			}

			Criteria criteria = new Criteria();
			criteria = criteria.where("action").is("PLAYSTOP").andOperator(criteria.where("command").is(commandType),
					criteria.where("createdDate").gt(cal1.getTimeInMillis()).lte(cal2.getTimeInMillis()));
			AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "views");
			
			if(!commandType.equalsIgnoreCase("CATCHUP")){
				Aggregation aggregation = newAggregation(match(criteria), 
						group("subject").count().as("views"),sort).
						withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
				
				AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, AmsRequest.class,
						AmsRequestVO.class);
					List<AmsRequestVO> results = groupResults.getMappedResults();
					return results;
			  }
			else{				
					Aggregation aggregation = newAggregation(match(criteria),project("programId","eventDateTime")
							,group("programId","eventDateTime").count().as("views").last("eventDateTime").as("eventDateTime").last("programId").as("programId"),sort).
							withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
					
					AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, AmsRequest.class,
							AmsRequestVO.class);
						List<AmsRequestVO> results = groupResults.getMappedResults();
						return results;
			 }
			} catch (Exception e) {
				e.printStackTrace();
			AmsError.error("Error in Method: topTenVideoChannelReport" + e.getMessage(), e);
			return null;
		}
	}
	@Override
	public void saveTopTenVodChannelReportData(TopTenVodChannelReport topTenVodChannelReport) {
		mongoOperations.save(topTenVodChannelReport,"topTenVodChannelReport");		
	}
	
	@Override
	public List<AmsRequestVO> topNowVideoTrendzByType(String commandType) {
		Criteria criteria = new Criteria();
		criteria=criteria.where("action").is("PLAYSTART").andOperator(criteria.where("command").is(commandType));
		AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "views");
		Aggregation aggregation = newAggregation(match(criteria), 
				group("subject").count().as("views"),sort,limit(500)).
				withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
		AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, "amsDynamicData",
				AmsRequestVO.class);
		List<AmsRequestVO> results = groupResults.getMappedResults();
		//System.out.println(aggregation);
		return results;
	}

	@Override
	public List getAmsCatchupDataByAction(String actionType, String assetType, String fromDate, String toDate,
			String eventDate, String subjectId,String programId, Integer offset, Integer max,List<Integer>franchiseIdsLst) {
		Criteria criteria = new Criteria();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "startDateTime");
			Aggregation aggregation=null;
			Date fromDate1 = null;
			Date toDate1 = null;
			
			
			Calendar cal1 = Calendar.getInstance();
			Calendar cal2 = Calendar.getInstance();
			
			if (StringUtils.isNotBlank(fromDate) && StringUtils.isNotBlank(toDate)) {
				fromDate1 = sdf.parse(fromDate);
				toDate1 = sdf.parse(toDate);
				cal1.setTime(fromDate1);
				cal1.set(Calendar.HOUR_OF_DAY,0);
				cal1.set(Calendar.MINUTE,0);
				cal1.set(Calendar.SECOND,0);
				cal2.setTime(toDate1);
				cal2.set(Calendar.HOUR_OF_DAY,23);
				cal2.set(Calendar.MINUTE,59);
				cal2.set(Calendar.SECOND,59);								
			}
			
						
			if (StringUtils.isNotBlank(actionType) && actionType.equalsIgnoreCase("PLAYSTOP")) {
				List<Criteria> criteriaLst = new ArrayList<Criteria>();
				criteriaLst.add(new Criteria().where("startDateTime").exists(true));
				criteriaLst.add(new Criteria().andOperator(criteria.where("endDateTime").exists(true)));
				criteriaLst.add(new Criteria().where("action").is(actionType));
				criteriaLst.add(new Criteria().where("command").is(assetType));
				criteriaLst.add(new Criteria().where("franchiseId").in(franchiseIdsLst));
				criteriaLst.add(new Criteria().where("createdDate").gte(cal1.getTimeInMillis()).lte(cal2.getTimeInMillis()));
				
				if(StringUtils.isNotBlank(eventDate)){
					criteriaLst.add(new Criteria().where("eventDateTime").is(eventDate));
				}
				if(StringUtils.isNotBlank(subjectId)){
					criteriaLst.add(new Criteria().where("subject").is(subjectId.toString()));
				}
				if(StringUtils.isNotBlank(programId)){
					criteriaLst.add(new Criteria().where("programId").is(programId));
				}
				
				criteria.andOperator(criteriaLst.toArray(new Criteria[criteriaLst.size()]));
				aggregation = newAggregation(match(criteria),sort,skip(offset),limit(max)).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());				
			}
			if (StringUtils.isNotBlank(actionType) && actionType.equalsIgnoreCase("PLAYSTART")) {
				List<Criteria> criteriaLst = new ArrayList<Criteria>();
				criteriaLst.add(new Criteria().where("startDateTime").exists(true));
				criteriaLst.add(new Criteria().where("action").is(actionType));
				criteriaLst.add(new Criteria().where("command").is(assetType));
				criteriaLst.add(new Criteria().where("franchiseId").in(franchiseIdsLst));
				criteriaLst.add(new Criteria().where("createdDate").gte(cal1.getTimeInMillis()).lte(cal2.getTimeInMillis()));
				
				if(StringUtils.isNotBlank(eventDate)){
					criteriaLst.add(new Criteria().where("eventDateTime").is(eventDate));
				}
				if(StringUtils.isNotBlank(subjectId)){
					criteriaLst.add(new Criteria().where("subject").is(subjectId.toString()));
				}
				if(StringUtils.isNotBlank(programId)){
					criteriaLst.add(new Criteria().where("programId").is(programId));
				}
				
				criteria.andOperator(criteriaLst.toArray(new Criteria[criteriaLst.size()]));
				
				aggregation = newAggregation(match(criteria),
							    sort,skip(offset),limit(max)).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());

			}			
			/*AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, AmsRequest.class,
					AmsRequestVO.class);
			List<AmsRequestVO> results = groupResults.getMappedResults();			
			return results;*/
			List<AmsRequestVO> results=new ArrayList<AmsRequestVO>();		
			if(StringUtils.isNotBlank(actionType) && actionType.equalsIgnoreCase("PLAYSTART")){
			AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, "amsDynamicData",
					AmsRequestVO.class);
			results = groupResults.getMappedResults();
			}
			else{
				AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, AmsRequest.class,
						AmsRequestVO.class);
				results = groupResults.getMappedResults();
			}
			return results;
		} catch (Exception e) {		
			AmsError.error("Error in Method: getAmsCatchupDataByAction"+e.getMessage(), e);
			return null;
		}
	}

	@Override
	public long getAmsCatchupDataCountByAction(String actionType, String assetType, String fromDate, String toDate,
			String eventDate, String subjectId,String programId,List<Integer> franchiseIdsLst) {
		long totalcount=0l;
		Criteria criteria = new Criteria();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "startDateTime");
			Aggregation aggregation=null;
			Date fromDate1 = null;
			Date toDate1 = null;
						
			Calendar cal1 = Calendar.getInstance();
			Calendar cal2 = Calendar.getInstance();			
			if (StringUtils.isNotBlank(fromDate) && StringUtils.isNotBlank(toDate)) {
				fromDate1 = sdf.parse(fromDate);
				toDate1 = sdf.parse(toDate);
				cal1.setTime(fromDate1);
				cal1.set(Calendar.HOUR_OF_DAY,0);
				cal1.set(Calendar.MINUTE,0);
				cal1.set(Calendar.SECOND,0);
				cal2.setTime(toDate1);
				cal2.set(Calendar.HOUR_OF_DAY,23);
				cal2.set(Calendar.MINUTE,59);
				cal2.set(Calendar.SECOND,59);								
			}
			
						
			if (StringUtils.isNotBlank(actionType) && actionType.equalsIgnoreCase("PLAYSTOP")) {
				List<Criteria> criteriaLst = new ArrayList<Criteria>();
				criteriaLst.add(new Criteria().where("startDateTime").exists(true));
				criteriaLst.add(new Criteria().andOperator(criteria.where("endDateTime").exists(true)));
				criteriaLst.add(new Criteria().where("action").is(actionType));
				criteriaLst.add(new Criteria().where("command").is(assetType));
				criteriaLst.add(new Criteria().where("franchiseId").in(franchiseIdsLst));
				criteriaLst.add(new Criteria().where("createdDate").gte(cal1.getTimeInMillis()).lte(cal2.getTimeInMillis()));
				
				if(StringUtils.isNotBlank(eventDate)){
					criteriaLst.add(new Criteria().where("eventDateTime").is(eventDate));
				}
				if(StringUtils.isNotBlank(subjectId)){
					criteriaLst.add(new Criteria().where("subject").is(subjectId.toString()));
				}
				if(StringUtils.isNotBlank(programId)){
					criteriaLst.add(new Criteria().where("programId").is(programId));
				}
				
				criteria.andOperator(criteriaLst.toArray(new Criteria[criteriaLst.size()]));
				Query query = new Query();
				query.addCriteria(criteria);
				totalcount = mongoOperations.count(query, AmsRequest.class);
				
			}
			if (StringUtils.isNotBlank(actionType) && actionType.equalsIgnoreCase("PLAYSTART")) {
				List<Criteria> criteriaLst = new ArrayList<Criteria>();
				criteriaLst.add(new Criteria().where("startDateTime").exists(true));
				criteriaLst.add(new Criteria().where("action").is(actionType));
				criteriaLst.add(new Criteria().where("command").is(assetType));
				criteriaLst.add(new Criteria().where("franchiseId").in(franchiseIdsLst));
				criteriaLst.add(new Criteria().where("createdDate").gte(cal1.getTimeInMillis()).lte(cal2.getTimeInMillis()));
				
				if(StringUtils.isNotBlank(eventDate)){
					criteriaLst.add(new Criteria().where("eventDateTime").is(eventDate));
				}
				if(StringUtils.isNotBlank(subjectId)){
					criteriaLst.add(new Criteria().where("subject").is(subjectId.toString()));
				}
				if(StringUtils.isNotBlank(programId)){
					criteriaLst.add(new Criteria().where("programId").is(programId));
				}
				
				criteria.andOperator(criteriaLst.toArray(new Criteria[criteriaLst.size()]));
				
				Query query = new Query();
				query.addCriteria(criteria);
				totalcount = mongoOperations.count(query, "amsDynamicData");								
			}
			return totalcount;
		} catch (Exception e) {
			//e.printStackTrace();
			AmsError.error("Error in Method: getAmsCatchupDataCountByAction"+e.getMessage(), e);
			return totalcount;
		}
	}
	@Override
	public List<TopTenVodChannelReport> getTopCatchupReportByDateAndProgramId(String fromDate, String toDate,
			String eventDate, List<String> programIdsLst, String commandType, String nofRecords) {
		
		Criteria criteria = new Criteria();
		List<Criteria> criteriaLst = new ArrayList<Criteria>();
		criteriaLst.add(new Criteria().where("commandType").is(commandType));
		criteriaLst.add(new Criteria().where("createdDate").gte(fromDate).lte(toDate));
		criteriaLst.add(new Criteria().where("programId").in(programIdsLst));
		
		if(StringUtils.isNotBlank(eventDate)){
			criteriaLst.add(new Criteria().where("eventDate").is(eventDate));
		}
		criteria.andOperator(criteriaLst.toArray(new Criteria[criteriaLst.size()]));
		AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "views");
		Aggregation aggregation = newAggregation(match(criteria), 
				group("programId").sum("views").as("views"),sort,limit(Integer.parseInt(nofRecords))).
				withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
		
		AggregationResults<TopTenVodChannelReport> groupResults = mongoTemplate.aggregate(aggregation, TopTenVodChannelReport.class,
				TopTenVodChannelReport.class);			
		List<TopTenVodChannelReport> results = groupResults.getMappedResults();
		return results;
	}

/*	@Override
	public TopAssetDurationReport getRecentDatefromTopAssetDurationReport(String commandType) {
		Query query = new Query();
		Criteria criteria = new Criteria();
		criteria = criteria.where("commandType").is(commandType);
		query.addCriteria(criteria);
		query.limit(1);
		query.with(new Sort(Sort.Direction.DESC, "createdDate"));
		TopAssetDurationReport dba = mongoOperations.findOne(query, TopAssetDurationReport.class,"topAssetDurationReport");		
		return dba;	
		}

	@Override
	public void saveTopAssetDurationReportData(TopAssetDurationReport durationReport) {
		mongoOperations.save(durationReport,"topAssetDurationReport");		
	}*/
	
	@Override
	public List<AmsRequestVO> topAssetDurationReport(String fromDate, String toDate,String commandType) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date fromDate1 = null;
		Date toDate1 = null;

		Calendar cal1 = Calendar.getInstance();
		Calendar cal2 = Calendar.getInstance();
		try {
			if (StringUtils.isNotBlank(fromDate) && StringUtils.isNotBlank(toDate)) {
				fromDate1 = sdf.parse(fromDate);
				toDate1 = sdf.parse(toDate);
				cal1.setTime(fromDate1);
				cal1.set(Calendar.HOUR_OF_DAY, 0);
				cal1.set(Calendar.MINUTE, 0);
				cal1.set(Calendar.SECOND, 0);
				cal2.setTime(toDate1);
				cal2.set(Calendar.HOUR_OF_DAY, 23);
				cal2.set(Calendar.MINUTE, 59);
				cal2.set(Calendar.SECOND, 59);
			}

			Criteria criteria = new Criteria();
			criteria = criteria.where("action").is("PLAYSTOP").andOperator(criteria.where("command").is(commandType),
					criteria.where("createdDate").gt(cal1.getTimeInMillis()).lte(cal2.getTimeInMillis()));
			AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "duration");
			
			if(!commandType.equalsIgnoreCase("CATCHUP")){
				Aggregation aggregation = newAggregation(match(criteria),project("subject").andExpression("endDateTime - startDateTime").as("duration"),
						group("subject").sum("duration").as("duration").last("subject").as("subject"),sort);
				
				
				AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, AmsRequest.class,
						AmsRequestVO.class);
					List<AmsRequestVO> results = groupResults.getMappedResults();
					return results;
			  }
			else{				
				Aggregation aggregation = newAggregation(match(criteria),project("programId","eventDateTime").andExpression("endDateTime - startDateTime").as("duration"),
							group("programId","eventDateTime").sum("duration").as("duration").last("programId").as("programId").last("eventDateTime").as("eventDateTime"),sort);
					
					AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, AmsRequest.class,
							AmsRequestVO.class);
						List<AmsRequestVO> results = groupResults.getMappedResults();
						return results;
			 }
			} catch (Exception e) {
				//e.printStackTrace();
				AmsError.error("Error in Method: topAssetDurationReport" + e.getMessage(), e);
			return null;
		}
	}

	@Override
	public List<ReportCollection> getTopLtvVodAppStoreDurationReportByDateAndId(String fromDate, String toDate,
			List<String> programIdsLst, String commandType, String nofRecords) {
		try {						
			Criteria criteria = new Criteria();
			criteria = criteria.where("assetType").is(commandType).andOperator(
					criteria.where("date").gte(fromDate).lte(toDate),
					criteria.where("assetId").in(programIdsLst));
			
			AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "duration");
			
			if(StringUtils.isNotBlank(nofRecords)){
				Aggregation aggregation = newAggregation(match(criteria), 
						group("assetId").sum("duration").as("duration"),sort,limit(Integer.parseInt(nofRecords))).
						withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
				
				AggregationResults<ReportCollection> groupResults = mongoTemplate.aggregate(aggregation, ReportCollection.class,
						ReportCollection.class);			
				List<ReportCollection> results = groupResults.getMappedResults();
			
			return results;
			}
			else{
				Aggregation aggregation = newAggregation(match(criteria), 
						group("assetId").sum("duration").as("duration"),sort).
						withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
				
				AggregationResults<ReportCollection> groupResults = mongoTemplate.aggregate(aggregation, ReportCollection.class,
						ReportCollection.class);			
				List<ReportCollection> results = groupResults.getMappedResults();
				
				return results;				
			}						
		} catch (Exception e) {
			//e.printStackTrace();
			AmsError.error("Error in Method: getTopLtvVodAppStoreDurationReportByDateAndId" + e.getMessage(), e);
			return null;
		}
	}

	@Override
	public List<ReportCollection> getTopCatchupDurationReportByDateAndProgramId(String fromDate, String toDate,
			String eventDate,List<String> programIdsLst, String commandType, String nofRecords) {
		Criteria criteria = new Criteria();
		List<Criteria> criteriaLst = new ArrayList<Criteria>();
		criteriaLst.add(new Criteria().where("assetType").is(commandType));
		criteriaLst.add(new Criteria().where("date").gte(fromDate).lte(toDate));
		criteriaLst.add(new Criteria().where("programId").in(programIdsLst));
		
		if(StringUtils.isNotBlank(eventDate)){
			criteriaLst.add(new Criteria().where("eventDate").is(eventDate));
		}
		criteria.andOperator(criteriaLst.toArray(new Criteria[criteriaLst.size()]));
		
		AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "duration");
		Aggregation aggregation = newAggregation(match(criteria), 
				group("programId").sum("duration").as("duration"),sort,limit(Integer.parseInt(nofRecords))).
				withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
		
		AggregationResults<ReportCollection> groupResults = mongoTemplate.aggregate(aggregation, ReportCollection.class,
				ReportCollection.class);			
		List<ReportCollection> results = groupResults.getMappedResults();
		return results;
	}
	@Override
	public List<AmsRequestVO> getResultUsersCount() {
		Calendar curTime = Calendar.getInstance();
		curTime.add(Calendar.DATE, - 7);
		
		Criteria criteria = new Criteria();
		criteria=criteria.where("action").is("RESULT").andOperator(criteria.where("command").is("SYSTEM")
				,criteria.where("createdDate").gte(curTime.getTimeInMillis()));
		
		Aggregation aggregation = newAggregation(match(criteria),group("franchiseId").count()
				.as("views").last("franchiseId").as("franchiseId"));
		AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, "amsTestData",
				AmsRequestVO.class);		
		List<AmsRequestVO> results = groupResults.getMappedResults();
		return results;							
	}

	@Override
	public List<AmsDynamicData> getDistinctAppVersionCount() {
		try{
		Criteria criteria = new Criteria();
		List<Criteria> criteriaLst = new ArrayList<Criteria>();
		criteriaLst.add(new Criteria().where("action").is("BOOTUP"));
		
		criteria.andOperator(criteriaLst.toArray(new Criteria[criteriaLst.size()]));
		AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "views");
		Aggregation aggregation = newAggregation(match(criteria),project("info"),
				group("info").count().as("views").last("info").as("info"), sort);

		AggregationResults<AmsDynamicData> groupResults = mongoTemplate.aggregate(aggregation, AmsDynamicData.class,
				AmsDynamicData.class);
		List<AmsDynamicData> results = groupResults.getMappedResults();
		//System.out.println(aggregation);
		return results;
		}
		catch (Exception e) {
			//e.printStackTrace();
			return null;
		}
	}

	@Override
	public List<AmsDynamicData> getDistinctBuildVersionCount() {
		try {
			Criteria criteria = new Criteria();
			List<Criteria> criteriaLst = new ArrayList<Criteria>();
			criteriaLst.add(new Criteria().where("action").is("BOOTUP"));

			criteria.andOperator(criteriaLst.toArray(new Criteria[criteriaLst.size()]));
			AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "views");
			Aggregation aggregation = newAggregation(match(criteria), project("build"), 
					group("build").count().as("views").last("build").as("build"), sort);

			AggregationResults<AmsDynamicData> groupResults = mongoTemplate.aggregate(aggregation, AmsDynamicData.class,
					AmsDynamicData.class);
			List<AmsDynamicData> results = groupResults.getMappedResults();
			// System.out.println(aggregation);
			return results;
		} catch (Exception e) {
			//e.printStackTrace();
			return null;
		}
	}

	@Override
	public Integer getEventViews(String assetId,Long startTime,Long endTime) {
		Query query = new Query();
		Criteria criteria = new Criteria();
		criteria=Criteria.where("subject").is(assetId).orOperator(
				
				new Criteria().orOperator(criteria.where("startDateTime").gte(startTime).lte(endTime),
				criteria.where("endDateTime").gte(startTime).lte(endTime))
				
				,criteria.where("startDateTime").lt(startTime).andOperator(criteria.where("endDateTime").gt(endTime))								
				);
		query.addCriteria(criteria);
		//System.out.println(query);
		List<AmsRequest> lst= mongoOperations.find(query, AmsRequest.class);
		//System.out.println("Result-->"+lst.size());
		
		return lst.size();
	}

	@Override
	public List<AmsDynamicData> getDistinctBuildVersionByFranchiseId(String franchiseId, String buildVersion) {
		try {
			Criteria criteria = new Criteria();
			List<Criteria> criteriaLst = new ArrayList<Criteria>();
			criteriaLst.add(new Criteria().where("action").is("BOOTUP").andOperator(criteria.where("franchiseId").is(Integer.parseInt(franchiseId))));
			
			if(StringUtils.isNotBlank(buildVersion))
				criteriaLst.add(new Criteria().where("build").is(buildVersion));
			
			
			criteria.andOperator(criteriaLst.toArray(new Criteria[criteriaLst.size()]));
			AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "views");
			Aggregation aggregation = newAggregation(match(criteria), project("build"), 
					group("build").count().as("views").last("build").as("build"), sort);

			AggregationResults<AmsDynamicData> groupResults = mongoTemplate.aggregate(aggregation, AmsDynamicData.class,
					AmsDynamicData.class);
			List<AmsDynamicData> results = groupResults.getMappedResults();
			
			return results;
		} catch (Exception e) {
			//e.printStackTrace();
			return null;
		}
	}

	@Override
	public List<AmsDynamicData> getDistinctAppVersionByFranchiseId(String franchiseId, String appVersion) {
		try {
			Criteria criteria = new Criteria();
			List<Criteria> criteriaLst = new ArrayList<Criteria>();
			criteriaLst.add(new Criteria().where("action").is("BOOTUP").andOperator(criteria.where("franchiseId").is(Integer.parseInt(franchiseId))));
			
			if(StringUtils.isNotBlank(appVersion))
				criteriaLst.add(new Criteria().where("info").is(appVersion));
			
			
			criteria.andOperator(criteriaLst.toArray(new Criteria[criteriaLst.size()]));
			AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "views");
			Aggregation aggregation = newAggregation(match(criteria), project("info"), 
					group("info").count().as("views").last("info").as("info"), sort);

			AggregationResults<AmsDynamicData> groupResults = mongoTemplate.aggregate(aggregation, AmsDynamicData.class,
					AmsDynamicData.class);
			List<AmsDynamicData> results = groupResults.getMappedResults();
			
			return results;
		} catch (Exception e) {
			//e.printStackTrace();
			return null;
		}
	}

	@Override
	public List<AmsDynamicData> getAllAppVersionsByFranchiseIdAndAppVersion(String franchiseId, String appVersion,String buildVersion) {
		try {
			Criteria criteria = new Criteria();
			List<Criteria> criteriaLst = new ArrayList<Criteria>();
			criteriaLst.add(new Criteria().where("action").is("BOOTUP").andOperator(criteria.where("franchiseId").is(Integer.parseInt(franchiseId))));
			
			if(StringUtils.isNotBlank(appVersion))
				criteriaLst.add(new Criteria().where("info").is(appVersion));
			
			if(StringUtils.isNotBlank(buildVersion))
				criteriaLst.add(new Criteria().where("build").is(buildVersion));
						
			criteria.andOperator(criteriaLst.toArray(new Criteria[criteriaLst.size()]));
			Query query=new Query();
			query.addCriteria(criteria);
			
			List<AmsDynamicData> results = mongoOperations.find(query, AmsDynamicData.class);			
			return results;
		} catch (Exception e) {		
			return null;
		}
	}

	@Override
	public Long getEventDuration(String assetId,Long startTime,Long endTime) {
		try{
		Long duration=0l;
		//CASE-1
		Criteria criteria0 = new Criteria();
		criteria0.andOperator(criteria0.where("subject").is(assetId),criteria0.where("startDateTime").gte(startTime).lte(endTime),
				criteria0.where("endDateTime").gte(startTime).lte(endTime));
		
		Aggregation aggregation0 = newAggregation(match(criteria0),project("subject").andExpression("endDateTime - startDateTime").as("duration"),
				group("subject").sum("duration").as("duration").last("subject").as("subject")).
				withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());		
		AggregationResults<AmsRequest> groupResults0 = mongoTemplate.aggregate(aggregation0, AmsRequest.class,AmsRequest.class);		
		List<AmsRequest> results0= groupResults0.getMappedResults();	
		for (AmsRequest amsRequest : results0) {
			duration+=(amsRequest.getDuration()/60000);
		}
		//CASE-2
		Criteria criteria1 = new Criteria();
		criteria1.andOperator(criteria1.where("subject").is(assetId),criteria1.where("startDateTime").lte(startTime),
				criteria1.where("endDateTime").gte(startTime).lte(endTime));
		
		Aggregation aggregation1= newAggregation(match(criteria1),project("subject","duration").and("endDateTime").minus(startTime.longValue()).as("duration"),
				group("subject").sum("duration").as("duration").last("subject").as("subject")).
				withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());		
		AggregationResults<AmsRequest> groupResults1 = mongoTemplate.aggregate(aggregation1, AmsRequest.class,AmsRequest.class);		
		List<AmsRequest> results1= groupResults1.getMappedResults();	
		//System.out.println(results1.size());
		for (AmsRequest amsRequest : results1) {
			duration+=(amsRequest.getDuration()/60000);
		}
		
		//CASE-3		
		Criteria criteria2 = new Criteria();
		criteria2.andOperator(criteria2.where("subject").is(assetId),criteria2.where("startDateTime").gte(startTime).lte(endTime),
				criteria2.where("endDateTime").gte(endTime));
		
		Aggregation aggregation2 = newAggregation(match(criteria2),project("subject","duration").and("startDateTime").minus(endTime.longValue()).as("duration"),
				group("subject").sum("duration").as("duration").last("subject").as("subject")).
				withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());		
		AggregationResults<AmsRequest> groupResults2 = mongoTemplate.aggregate(aggregation2, AmsRequest.class,AmsRequest.class);		
		List<AmsRequest> results2 = groupResults2.getMappedResults();	
		//System.out.println(results2.size());
		//System.out.println(aggregation2);
		for (AmsRequest amsRequest : results2) {
			duration+=Math.abs(amsRequest.getDuration()/60000);
		}
		
		//CASE-4		
		Criteria criteria3 = new Criteria();
		criteria3.andOperator(criteria3.where("subject").is(assetId),criteria3.where("startDateTime").lte(startTime),
				criteria3.where("endDateTime").gte(endTime));
		
		Aggregation aggregation3 = newAggregation(match(criteria3),
				group("id").last("id").as("id")).
				withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());		
		AggregationResults<AmsRequest> groupResults3 = mongoTemplate.aggregate(aggregation3, AmsRequest.class,AmsRequest.class);		
		List<AmsRequest> results3= groupResults3.getMappedResults();	
		duration+=results3.size()*((endTime-startTime)/60000);
		
		return duration;
		}
		catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public void saveServiceAssetEventSummary(ServiceAssetEventsSummary saes) {
		mongoOperations.save(saes);
		
	}

	@Override
	public List<ServiceAssetEventsSummary> getServiceAssetEventSummaryCollectionByDate(String fromDate, String toDate,
			String assetId, Integer offset, Integer max) {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date fromDate1 = null;Date toDate1 = null;
			Calendar cal1 = Calendar.getInstance();
			Calendar cal2 = Calendar.getInstance();
			fromDate1 = sdf.parse(fromDate);
			toDate1 = sdf.parse(toDate);
			cal1.setTime(fromDate1);
			cal2.setTime(toDate1);
			cal2.set(Calendar.HOUR_OF_DAY,23);
			cal2.set(Calendar.MINUTE,59);
			cal2.set(Calendar.SECOND,59);
			
			Criteria criteria = new Criteria();
			criteria = criteria.where("subject").is(assetId)
					.andOperator(criteria.where("createdDate").gte(cal1.getTimeInMillis()).lte(cal2.getTimeInMillis()));
			AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "views");
			Aggregation aggregation = newAggregation(match(criteria),				
					group("programId").sum("duration").as("duration").sum("views").as("views").last("programId").as("programId")
					.last("programName").as("programName"),sort,skip(offset),limit(max))
					.withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
			
			AggregationResults<ServiceAssetEventsSummary> groupResults = mongoTemplate.aggregate(aggregation, ServiceAssetEventsSummary.class,
					ServiceAssetEventsSummary.class);
			
			List<ServiceAssetEventsSummary> results = groupResults.getMappedResults();		
			return results;
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}		
	}

	@Override
	public Long getServiceAssetEventSummaryCollectionCountByDate(String fromDate, String toDate, String assetId) {
		try {
			Long totalCount=0l;
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date fromDate1 = null;Date toDate1 = null;
			Calendar cal1 = Calendar.getInstance();
			Calendar cal2 = Calendar.getInstance();
			fromDate1 = sdf.parse(fromDate);
			toDate1 = sdf.parse(toDate);
			cal1.setTime(fromDate1);
			cal2.setTime(toDate1);
			cal2.set(Calendar.HOUR_OF_DAY,23);
			cal2.set(Calendar.MINUTE,59);
			cal2.set(Calendar.SECOND,59);
			
			Criteria criteria = new Criteria();
			criteria = criteria.where("subject").is(assetId)
					.andOperator(criteria.where("createdDate").gte(cal1.getTimeInMillis()).lte(cal2.getTimeInMillis()));
			
			Aggregation aggregation = newAggregation(match(criteria),group("programId").last("programId").as("programId"))
					.withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
			
			AggregationResults<ServiceAssetEventsSummary> groupResults = mongoTemplate.aggregate(aggregation, ServiceAssetEventsSummary.class,
					ServiceAssetEventsSummary.class);
			
			List<ServiceAssetEventsSummary> results = groupResults.getMappedResults();	
			totalCount=(long) results.size();
			return totalCount;
		} catch (ParseException e) {
			e.printStackTrace();
			return 0l;
		}
	}

	@Override
	public List<ServiceAssetEventsSummary> getServiceAssetEventSummaryCollectionByProgramId(String fromDate,
			String toDate, String assetId, String programId) {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date fromDate1 = null;Date toDate1 = null;
			Calendar cal1 = Calendar.getInstance();
			Calendar cal2 = Calendar.getInstance();
			fromDate1 = sdf.parse(fromDate);
			toDate1 = sdf.parse(toDate);
			cal1.setTime(fromDate1);
			cal2.setTime(toDate1);
			cal2.set(Calendar.HOUR_OF_DAY,23);
			cal2.set(Calendar.MINUTE,59);
			cal2.set(Calendar.SECOND,59);
			
			Criteria criteria = new Criteria();
			criteria = criteria.where("subject").is(assetId)
					.andOperator(criteria.where("createdDate").gte(cal1.getTimeInMillis()).lte(cal2.getTimeInMillis()),
					criteria.where("programId").is(programId));
			
			Query query=new Query();
			query.addCriteria(criteria);
			List<ServiceAssetEventsSummary> results = mongoOperations.find(query, ServiceAssetEventsSummary.class);		
			return results;
		} catch (ParseException e) {
			e.printStackTrace();
			return null;
		}		
	}
	@Override
	public List<Integer> getDistinctFranchiseIds() {
		Query query = new Query().addCriteria(Criteria.where("action").is("BOOTUP"));
		List<Integer> franchiseIds = mongoTemplate.getCollection("amsDynamicData").distinct("franchiseId",query.getQueryObject());	
		return franchiseIds;
	}

	@Override
	public void removeDashboardAllTrendzCollection(String commandType,Integer franchiseId) {
		Query query=new Query();
		query.addCriteria(Criteria.where("command").is(commandType)
			 /*andOperator(Criteria.where("franchiseId").is(franchiseId))*/);
		mongoTemplate.remove(query, "dashboardAllTrendzCollection");
	}

	@Override
	public List<AmsRequestVO> getTopAllTrendzByFranchiseId(Integer franchiseId,String commandType) {
		Calendar c24HrBack = Calendar.getInstance();
		c24HrBack.add(Calendar.HOUR, -24);
		Criteria criteria = new Criteria();
		criteria=criteria.where("action").is("PLAYSTOP").andOperator(criteria.where("command").is(commandType)
				/*,criteria.where("franchiseId").is(franchiseId)*/,(criteria.where("startDateTime").gte(c24HrBack.getTimeInMillis())
				.orOperator(criteria.where("endDateTime").gte(c24HrBack.getTimeInMillis()))));
		AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "views");
		if(!commandType.equalsIgnoreCase("CATCHUP")){
			Aggregation aggregation = newAggregation(match(criteria), 
					group("subject").count().as("views"),sort,limit(20)).
					withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
			AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, AmsRequest.class,
					AmsRequestVO.class);
			List<AmsRequestVO> results = groupResults.getMappedResults();
			//System.out.println(aggregation);
			return results;
			}
			else{
				Aggregation aggregation = newAggregation(match(criteria), 
						group("programId").count().as("views"),sort,limit(20)).
						withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
				AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, AmsRequest.class,
						AmsRequestVO.class);
				List<AmsRequestVO> results = groupResults.getMappedResults();			
				return results;
			}
		//System.out.println(aggregation);		
	}
	@Override
	public void saveDashboardAllTrendzCollection(DashboardAllTrendzCollection dashboardAllTrendzCollection) {
			mongoOperations.save(dashboardAllTrendzCollection);
	}

	@Override
	public List<AmsRequestVO> getAmsAliveDashBoardCountByAction(Long noOfSecondsAlive) {
		Criteria criteria =new Criteria();
		criteria = criteria.where("action").is("ALIVE").andOperator(criteria.where("command").is("SYSTEM"),
				   criteria.where("endDateTime").gte(noOfSecondsAlive));

		Aggregation aggregation = newAggregation(match(criteria),group("franchiseId").count()
				.as("views").last("franchiseId").as("franchiseId"));
		AggregationResults<AmsRequestVO> groupResults = mongoTemplate.aggregate(aggregation, "amsDynamicData",
				AmsRequestVO.class);		
		List<AmsRequestVO> results = groupResults.getMappedResults();
		return results;
	}
	
	/*public Integer fetchBuildVersionCountByAppVersion(String info, String build) {
		Integer count=0;
		Criteria criteria = new Criteria();
		criteria=criteria.where("action").is("BOOTUP").andOperator(criteria.where("build").is(build),
				criteria.where("info").is(info));
		Query query = new Query();
		query.addCriteria(criteria);
		count = (int) mongoOperations.count(query, AmsDynamicData.class);
		return count;		
	}   */

	public String fetchMaxDateHourFromHourlyAssetWiseTraffic() {
		Criteria criteria = new Criteria();
		criteria = criteria.where("date").exists(true);
		String maxDateHr = "";
		AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "date", "hour");
		Aggregation aggregation = newAggregation(match(criteria),
				project("date", "hour").andExpression("concat(date,' ',hour,':59:59')").as("date"), sort, limit(1));

		AggregationResults<HourlyTrafficAssetWise> groupResults = mongoTemplate.aggregate(aggregation,
				HourlyTrafficAssetWise.class, HourlyTrafficAssetWise.class);
		List<HourlyTrafficAssetWise> results = groupResults.getMappedResults();
		for (HourlyTrafficAssetWise to : results) {
			maxDateHr = to.getDate();
		}
		return maxDateHr;
	}
	public String fetchMaxDateHourFromAdnetHourlyTraffic() {
		Criteria criteria = new Criteria();
		criteria = criteria.where("date").exists(true);
		String maxDateHr = "";
		AggregationOperation sort = Aggregation.sort(Sort.Direction.DESC, "date", "hour");
		Aggregation aggregation = newAggregation(match(criteria),
				project("date", "hour").andExpression("concat(date,' ',hour,':59:59')").as("date"), sort, limit(1));

		AggregationResults<AdnetHourlyReport> groupResults = mongoTemplate.aggregate(aggregation,
				AdnetHourlyReport.class, AdnetHourlyReport.class);
		List<AdnetHourlyReport> results = groupResults.getMappedResults();
		for (AdnetHourlyReport to : results) {
			maxDateHr = to.getDate();
		}
		return maxDateHr;
	}

	public List<AmsRequest> gethourlyTrafficAssetWiseCount(long fromDate, long toDate, String assetType) {
		Criteria criteria = new Criteria();
		criteria = criteria.where("action").is("PLAYSTOP")
				.andOperator(criteria.where("command").is(assetType),criteria.where("createdDate").gte(fromDate).lte(toDate));

		Aggregation aggregation = newAggregation(match(criteria), project("subject", "stb"),
				group("subject", "stb"),group("subject").count().as("count").last("subject").as("subject")
						.last("stb").as("stb")
				).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
		
		AggregationResults<AmsRequest> groupResults = mongoTemplate.aggregate(aggregation,
				AmsRequest.class, AmsRequest.class);
		//System.out.println(aggregation);
		List<AmsRequest> results = groupResults.getMappedResults();
		return results;
	}

	public void saveHourlyTrafficAssetWiseReport(HourlyTrafficAssetWise htaw) {
		mongoTemplate.save(htaw);
	}

	@Override
	public List<HourlyTrafficAssetWise> getHourlyTrafficAssetWiseByDate(String date, String assetId) {
		Criteria criteria = new Criteria();
		criteria=criteria.where("date").is(date).andOperator(criteria.where("assetId").is(assetId));
		Query query = new Query();
		query.addCriteria(criteria);		
	
		List<HourlyTrafficAssetWise> htwlst = mongoOperations.find(query, HourlyTrafficAssetWise.class);
		return htwlst;
	}

	public List<AmsRequest> getAdnetHourlyTraffic(long fromDate, long toDate, String assetType) {
		Criteria criteria = new Criteria();
		criteria = criteria.where("action").is("PLAYSTOP")
				.andOperator(criteria.where("command").is(assetType),criteria.where("createdDate").gte(fromDate).lte(toDate));

		Aggregation aggregation = newAggregation(match(criteria), 
				group("subject").sum("count").as("count").last("subject").as("subject").last("slotId").as("slotId")
				).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build());
		
		AggregationResults<AmsRequest> groupResults = mongoTemplate.aggregate(aggregation,
				AmsRequest.class, AmsRequest.class);
		//System.out.println(aggregation);
		List<AmsRequest> results = groupResults.getMappedResults();
		return results;
		}

	public void saveAdnetHourlyReport(AdnetHourlyReport adnet) {
		mongoTemplate.save(adnet);
	}

	@Override
	public List<AdnetHourlyReport> getHourlyAdnetTrafficByDate(String date, String adId) {

		Criteria criteria = new Criteria();
		criteria = criteria.where("date").is(date).andOperator(criteria.where("adId").is(adId));
		Query query = new Query();
		query.addCriteria(criteria);

		List<AdnetHourlyReport> adLst = mongoOperations.find(query, AdnetHourlyReport.class);
		//System.out.println(adLst);
		return adLst;

	}

	@Override
	public void removeAppVersionReportCollection() {
		mongoTemplate.remove(new Query(), "appVersionReportCollection");
	}

	@Override
	public void saveAppVersionReportCollection(AppBuildVersion appBv) {
		mongoOperations.save(appBv,"appVersionReportCollection");
	}

	@Override
	public void removeBuildVersionReportCollection() {
		mongoTemplate.remove(new Query(), "buildVersionReportCollection");
		
	}

	@Override
	public void saveBuildVersionReportCollection(AppBuildVersion appBv) {
		mongoOperations.save(appBv,"buildVersionReportCollection");
	}

	@Override
	public List<AppBuildVersion> getAppVersionReportCollection() {
		List<AppBuildVersion> appVersionLst = mongoOperations.findAll(AppBuildVersion.class,"appVersionReportCollection");
		return appVersionLst;
	}

	@Override
	public List<AppBuildVersion> getBuildVersionReportCollection() {
		List<AppBuildVersion> buildVersionLst = mongoOperations.findAll(AppBuildVersion.class,"buildVersionReportCollection");
		return buildVersionLst;
	}	
}
