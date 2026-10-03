package co.edu.uniminuto.transacciones;
import com.hazelcast.core.*;
import com.hazelcast.config.*;
import com.hazelcast.cache.impl.HazelcastServerCachingProvider;
import javax.cache.*;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.*;
import java.util.concurrent.TimeUnit;
public final class CacheDemo {
 private static Config config(int port) {
  Config c=new Config();c.setClusterName("uniminuto-s6");c.setProperty("hazelcast.logging.type","none");
  c.setProperty("hazelcast.operation.thread.count","2");c.setProperty("hazelcast.operation.generic.thread.count","2");
  c.getNetworkConfig().setPort(port).setPortAutoIncrement(false).setPublicAddress("127.0.0.1:"+port);
  c.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
  JoinConfig j=c.getNetworkConfig().getJoin();j.getMulticastConfig().setEnabled(false);
  j.getTcpIpConfig().setEnabled(true).addMember("127.0.0.1:5706").addMember("127.0.0.1:5707");return c;
 }
 public static void run() throws Exception {
  HazelcastInstance a=Hazelcast.newHazelcastInstance(config(5706));HazelcastInstance b=null;
  try {
   b=Hazelcast.newHazelcastInstance(config(5707));
   long deadline=System.currentTimeMillis()+20000;
   while(a.getCluster().getMembers().size()!=2 && System.currentTimeMillis()<deadline)Thread.sleep(100);
   DbConfig.check(a.getCluster().getMembers().size()==2,"No se formo el cluster de dos miembros");
   CacheManager ma=new HazelcastServerCachingProvider(a).getCacheManager();
   CacheManager mb=new HazelcastServerCachingProvider(b).getCacheManager();
   MutableConfiguration<Long,Integer> cfg=new MutableConfiguration<Long,Integer>().setTypes(Long.class,Integer.class)
    .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(new Duration(TimeUnit.SECONDS,60)));
   Cache<Long,Integer> ca=ma.createCache("productos",cfg);Cache<Long,Integer> cb=mb.getCache("productos",Long.class,Integer.class);
   if(cb==null)cb=mb.createCache("productos",cfg);
   Integer initial=ca.get(1L);DbConfig.check(initial==null,"Se esperaba cache vacia");
   int stock=DbConfig.stock();ca.put(1L,stock);System.out.println("CACHE MISS nodo A: consulta MySQL, stock="+stock);
   Integer remote=cb.get(1L);DbConfig.check(remote!=null && remote==stock,"El nodo B no comparte el dato");
   System.out.println("CACHE HIT nodo B: stock="+remote+" miembros="+a.getCluster().getMembers().size());
   ca.remove(1L);DbConfig.check(cb.get(1L)==null,"Invalidacion no compartida");
   System.out.println("CACHE INVALIDACION: nodo B observa null");
   DbConfig.event(java.util.UUID.randomUUID().toString(),"CACHE","Dos miembros; MISS MySQL; HIT remoto; invalidacion comprobada");
   ma.destroyCache("productos");
  } finally {if(b!=null)b.shutdown();a.shutdown();}
 }
}

