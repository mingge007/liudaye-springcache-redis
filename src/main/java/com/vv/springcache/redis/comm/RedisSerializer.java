package com.vv.springcache.redis.comm;

import com.caucho.hessian.io.AbstractHessianInput;
import com.caucho.hessian.io.AbstractHessianOutput;
import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;
import com.caucho.hessian.io.SerializerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;


public class RedisSerializer {
	private static Logger log = LoggerFactory.getLogger(RedisSerializer.class);

	private static SerializerFactory factory;
	
	public static SerializerFactory getSerializerFactory() {
		if (null == factory) {
			factory = new SerializerFactory();
		}
		return factory;
	}
	
	private static byte[] _serializeHessian(Object object) throws IOException {
		
		if (object == null)
			return null;
		
		ByteArrayOutputStream os = new ByteArrayOutputStream();
    	AbstractHessianOutput out = new Hessian2Output(os);;
    	SerializerFactory serializerFactory = getSerializerFactory();
    	out.setSerializerFactory(serializerFactory);
    	//out.startReply();
    	out.writeObject(object);
    	//out.completeReply();
    	out.flush();
    	out.close();
    	os.flush();
    	os.close();
    	
    	return os.toByteArray();
	}
	
	private  static Object _unserializeHessian(byte[] bytes) throws IOException, ClassNotFoundException {
		
		if (bytes == null)
			return null;
		
		ByteArrayInputStream is = new ByteArrayInputStream(bytes);
    	AbstractHessianInput in = new Hessian2Input(is);
    	in.setSerializerFactory(getSerializerFactory());
    	try {
			//in.startReply();
		} catch (Throwable e) {
			e.printStackTrace();
		}
    	Object userb = in.readObject();
    	//in.completeReply();
    	in.close();
    	is.close();
    	return userb;
	}
	
	
	
	private static byte[] _serializeJava(Object object) throws IOException {
		ObjectOutputStream oos = null;
		ByteArrayOutputStream baos = null;

	
		 baos = new ByteArrayOutputStream();
		 oos = new ObjectOutputStream(baos);
		 oos.writeObject(object);
		 byte[] bytes = baos.toByteArray();
		 return bytes;
	}

	

	private static Object _unserializeJava(byte[] bytes) throws IOException, ClassNotFoundException {

		ByteArrayInputStream bais = null;
		
		 //反序列化
		 bais = new ByteArrayInputStream(bytes);
		 ObjectInputStream ois = new ObjectInputStream(bais);
		 return ois.readObject();
	}

	
	public static byte[] in(Object object) {
		try{
			//return _serializeJava(object);
			return _serializeHessian(object);
		}catch(Exception e){
			log.error("error",e);
			return null;
		}
	}
	
	public static Object out(byte[] bytes) {
		try{
			//return _unserializeJava(bytes);
			return _unserializeHessian(bytes);
		}catch(Exception e){
			log.error("error",e);
			return null;
		}
	}
	
	public static Object get(Jedis jedis, byte[] key){
		if (jedis != null){
			byte [] data = jedis.get(key);
			if (data != null){
				log.info("redis get "  + new String(key) + " succeed.");
				return out(data);
			}else{
				log.info("redis get "  + new String(key) + " failed.");
			}
		}else{
			log.warn("jedis connection is null, when get " + new String(key));
		}
		return null;
	}
}
