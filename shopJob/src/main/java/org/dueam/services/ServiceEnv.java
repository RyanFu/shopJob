package org.dueam.services;

/**
 * 服务参数
 * User: windonly
 * Date: 11-6-27 下午4:51
 */
public interface ServiceEnv {
    String SP_HOST_LOCAL = "http://127.0.0.1:8080";
    String SP_HOST = "http://110.75.26.140";
    String SP_USER_ID = SP_HOST + "/app/user/id/";
    String SP_USER_NICK = SP_HOST + "/app/user/nick/";
    String SP_HOST_SERVICE = "http://172.23.178.166";
    String SERVICE_USER_ID = SP_HOST_SERVICE + "/app/user/id/";
    String SERVICE_USER_NICK = SP_HOST_SERVICE + "/app/user/nick/";
}
