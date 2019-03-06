package Utils

import java.util.Properties

/**
  * Properties的工具类
  *
  * Created by yore on 2017-11-9 14:05
  */
object PropertiesUtil_ka_str {

  /**
    *
    * 获取配置文件Properties对象
    *
    * @author yore
    * @return java.util.Properties
    */
  def getProperties() :Properties = {
    val properties = new Properties()
    //读取源码中resource文件夹下的ka_str.properties配置文件
    val reader = getClass.getResourceAsStream("/ka_str.properties")
    properties.load(reader)
    properties
  }

  /**
    *
    * 获取配置文件中key对应的字符串值
    *
    * @author yore
    * @return java.util.Properties
    */
  def getPropString(key : String) : String = {
    getProperties().getProperty(key)
  }

  /**
    *
    * 获取配置文件中key对应的整数值
    *
    * @author yore
    * @return java.util.Properties
    */
  def getPropInt(key : String) : Int = {
    getProperties().getProperty(key).toInt
  }

  /**
    *
    * 获取配置文件中key对应的布尔值
    *
    * @author yore
    * @return java.util.Properties
    */
  def getPropBoolean(key : String) : Boolean = {
    getProperties().getProperty(key).toBoolean
  }

}
