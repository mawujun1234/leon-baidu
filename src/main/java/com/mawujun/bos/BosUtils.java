package com.mawujun.bos;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.baidubce.auth.DefaultBceCredentials;
import com.baidubce.services.bos.BosClient;
import com.baidubce.services.bos.BosClientConfiguration;
import com.baidubce.services.bos.model.ListObjectsRequest;
import com.baidubce.services.bos.model.ListObjectsResponse;
import com.baidubce.services.bos.model.PutObjectResponse;

/**
 * 百度的bos的操作工具类
 * @author mawujun email:16064988@qq.com qq:16064988
 *
 */
public class BosUtils {
	static Logger logger=LogManager.getLogger(BosUtils.class);
	static Properties baidu_pps;
	static BosClient client ;
	static {
		init("baidu.properties");//默认初始化baidu.properties文件
	}
	/**
	 * 初始化百度的bos工具类
	 * @author mawujun email:160649888@163.com qq:16064988
	 * @param properties_file
	 */
	public static void init(String properties_file){
		if(client!=null){
			return;
		}
		try {
		if(properties_file==null){
			throw new NullPointerException("baidu。properties路径必须先指定!");
		}
		InputStream in = BosUtils.class.getClassLoader().getResourceAsStream(properties_file);
		if(in==null){
			throw new NullPointerException("不能读取到微信框架的配置文件,请检查路径!");
		}
		baidu_pps = new Properties();
		baidu_pps.load(in);
		
		String ACCESS_KEY_ID = baidu_pps.getProperty("ACCESS_KEY_ID");                  // 用户的Access Key ID
		String SECRET_ACCESS_KEY = baidu_pps.getProperty("SECRET_ACCESS_KEY");             // 用户的Secret Access Key
		String ENDPOINT = baidu_pps.getProperty("ENDPOINT");                        // 用户自己指定的域名

		BosClientConfiguration config = new BosClientConfiguration();
		config.setCredentials(new DefaultBceCredentials(ACCESS_KEY_ID,SECRET_ACCESS_KEY));
		if(ENDPOINT!=null && !"".equals(ENDPOINT)){
			config.setEndpoint(ENDPOINT);
		}
		
		client = new BosClient(config);
		} catch(IOException e){
			logger.error("配置文件加载失败:"+properties_file,e);
			throw new RuntimeException("配置文件加载失败:"+properties_file);
		}
	}
	/**
	 * 若用户需要判断某个Bucket是否存在
	 * @author mawujun email:160649888@163.com qq:16064988
	 * @param client
	 * @param bucketName
	 */
	public static boolean doesBucketExist (String bucketName) {

	    // 获取Bucket的存在信息
	    boolean exists = client.doesBucketExist(bucketName);                //指定Bucket名称

	    // 输出结果
	    if (exists) {
	    	//logger.error("配置文件加载失败:"+properties_file,e);	
	    } else {
	    	logger.error("bucket不存在{}",bucketName);
	    }
	    return exists;
	}
	/**
	 * 按文件的形式进行上传
	 * 
	 * @author mawujun email:160649888@163.com qq:16064988
	 * @param bucketName
	 * @param objectKey fun/movie/001.avi,fun/movie/007.avi,fun/test.jpg
	 * @param file
	 */
	public static void putObject(String bucketName,String objectKey,File file) {
		 PutObjectResponse putObjectFromFileResponse = client.putObject(bucketName, objectKey, file);	 
	}
	/**
	 * 以数据流形式上传Object
	 * @author mawujun email:160649888@163.com qq:16064988
	 * @param bucketName
	 * @param objectKey fun/movie/001.avi,fun/movie/007.avi,fun/test.jpg
	 * @param filepath
	 */
	public static void putObject(String bucketName,String objectKey,String filepath) { 
		 InputStream inputStream=null;
		try {
			// 获取数据流
			 inputStream = new FileInputStream(filepath);
			 // 以数据流形式上传Object
		    PutObjectResponse putObjectResponseFromInputStream = client.putObject(bucketName, objectKey, inputStream);	
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("{}文件不存在。",filepath);
		} finally {
			if(inputStream!=null){
				try {
					inputStream.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}
	    
	}
	/**
	 * 以二进制串上传Object
	 * @author mawujun email:160649888@163.com qq:16064988
	 * @param bucketName
	 * @param objectKey fun/movie/001.avi,fun/movie/007.avi,fun/test.jpg
	 * @param byte1
	 */
	public static void putObject(String bucketName,String objectKey,byte[] byte1) {
		PutObjectResponse putObjectResponseFromByte = client.putObject(bucketName, objectKey, byte1);
	}
	/**
	 * 以字符串上传Object
	 * @author mawujun email:160649888@163.com qq:16064988
	 * @param bucketName
	 * @param objectKey fun/movie/001.avi,fun/movie/007.avi,fun/test.jpg
	 * @param string1
	 */
	public static void putStringObject(String bucketName,String objectKey,String string1) {
		PutObjectResponse putObjectResponseFromString = client.putObject(bucketName, objectKey, string1);
	}
	/**
	 * 默认是50个对象一个分页
	 * ListObjectsResponse.getNextMarker()获取到的是下一页的开始对象
	 * @author mawujun email:160649888@163.com qq:16064988
	 * @param bucketName
	 * @param marker 相当于分页的下一页开始，为null或不设置表示是从第一个开始
	 * @param prefix 查看某个目录下的文件，注意不能以/符号开头
	 */
	public ListObjectsResponse listObjects(String bucketName,String prefix,String marker) {

		// 构造ListObjectsRequest请求
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucketName);

		// 设置参数
		listObjectsRequest.setDelimiter("/");
		listObjectsRequest.setMaxKeys(50);
		if(marker!=null && !marker.equals("")){
			listObjectsRequest.setMarker(marker);
		}
		if(prefix!=null && !prefix.equals("")){
			// 递归列出fun目录下的所有文件
			listObjectsRequest.setPrefix(prefix);
		}
		ListObjectsResponse listing = client.listObjects(listObjectsRequest);
		return listing;
	}
	/**
	 * 默认是50个对象一个分页
	 * ListObjectsResponse.getNextMarker()获取到的是下一页的开始对象
	 * @author mawujun email:160649888@163.com qq:16064988
	 * @param bucketName
	 * @param marker 相当于分页的下一页开始，为null或不设置表示是从第一个开始
	 * @param prefix 查看某个目录下的文件，注意不能以/符号开头,为null或者不设置表示查询的是根目录下的对象
	 */
	public ListObjectsResponse listObjects(String bucketName,String prefix,String marker,int maxKeys) {

		// 构造ListObjectsRequest请求
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucketName);

		// 设置参数
		listObjectsRequest.setDelimiter("/");
		listObjectsRequest.setMaxKeys(maxKeys);
		if(marker!=null && !marker.equals("")){
			listObjectsRequest.setMarker(marker);
		}
		if(prefix!=null && !prefix.equals("")){
			// 递归列出fun目录下的所有文件
			listObjectsRequest.setPrefix(prefix);
		}
		ListObjectsResponse listing = client.listObjects(listObjectsRequest);
		return listing;
	}
	/**
	 * 用户可以通过如下代码获取指定Object的URL
	 * 用户在调用该函数前，需要手动设置endpoint为所属Region域名。
	 * @author mawujun email:160649888@163.com qq:16064988
	 * @param client
	 * @param bucketName
	 * @param objectKey
	 * @param expirationInSeconds 为指定的URL有效时长，时间从当前时间算起，为可选参数，不配置时系统默认值为1800秒。如果要设置为永久不失效的时间，可以将expirationInSeconds参数设置为 -1，不可设置为其他负数。
	 * @return
	 */
	public static String generatePresignedUrl(String bucketName, String objectKey, int expirationInSeconds) {

		   URL url = client.generatePresignedUrl(bucketName, objectKey, expirationInSeconds);          

		   //指定用户需要获取的Object所在的Bucket名称、该Object名称、时间戳、URL的有效时长   

		   return url.toString();
	}
	
}
