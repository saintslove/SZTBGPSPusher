/*
 * SZTBGPSPusher.cpp
 *
 *  Created on: 2017年2月17日
 *      Author: wong
 */

#include "SZTBGPSPusher.h"

#include <cstring>
#include <stdio.h>
#include <stdlib.h>
#include <boost/bind.hpp>

#include "base/Logging.h"
#include "BATNetSDKRawAPI.h"
#include "BATNetSDKAPI.h"
#include "Config.h"
#include "Util.h"


#define CHECK_RET(exp) \
    { \
        int ret = exp; \
        if (ret != CCMS_RETURN_OK) \
        { \
            LOG_WARN << #exp << " = " << ret; \
            break; \
        } \
    }

SZTBGPSPusher::SZTBGPSPusher(const std::string& sn, const std::string& whiteListFile)
: m_hIntServer(-1)
, m_hExtServer(-1)
, m_threadCheck(
    boost::bind(&SZTBGPSPusher::CheckThreadFunc, this), "CheckThread")
, m_threadPush(
    boost::bind(&SZTBGPSPusher::PushThreadFunc, this), "PushThread")
, m_bCheckThreadRunning(true)
, m_bPushThreadRunning(true)
, m_whiteListFile(whiteListFile)
, m_mutexConn()
, m_condConn(m_mutexConn)
, m_mutexMsg()
, m_condMsg(m_mutexMsg)
{
    BATNetSDK_Init(CCMS_DEVTYPE_DELIVER, (char*)sn.c_str(), true);
    m_threadCheck.start();
    m_threadPush.start();
}

SZTBGPSPusher::~SZTBGPSPusher()
{
    m_bCheckThreadRunning = false;
    m_bPushThreadRunning = false;
    m_threadCheck.join();
    m_threadPush.join();
    if (m_hIntServer != -1)
    {
        BATNetSDK_DeleteObj(m_hIntServer);
    }
    if (m_hExtServer != -1)
    {
        BATNetSDKRaw_DeleteObj (m_hExtServer);
    }
    BATNetSDK_Release();
}

void SZTBGPSPusher::CheckThreadFunc()
{
    while (m_bCheckThreadRunning)
    {
        muduo::MutexLockGuard lock(m_mutexConn);
        LoadWhiteList();
        CheckClientList();
        m_condConn.waitForSeconds(60 * 5); // 5分钟
    }
}

void SZTBGPSPusher::PushThreadFunc()
{
    while (m_bPushThreadRunning)
    {
        std::vector<SendData> sendQueue;
        {
            muduo::MutexLockGuard lock(m_mutexMsg);
            if (m_sendQueue.empty())
            {
                m_condMsg.waitForMillSeconds(10);
                continue;
            }
            else
            {
                LOG_DEBUG << "m_packgeQueue.size() = " << m_sendQueue.size();
                sendQueue.swap(m_sendQueue);
            }
        }
        if (sendQueue.size() > 1000)
        {
            LOG_WARN<< " sendQueue too long! " << sendQueue.size();
        }
        for (size_t i = 0; i < sendQueue.size(); ++i)
        {
            if (sendQueue[i].buf == NULL)
            {
                LOG_ERROR<< i << " buf == NULL len = " << sendQueue[i].len;
                continue;
            }
            DoPusher(sendQueue[i].buf, sendQueue[i].len);
            SAFE_DELETEA(sendQueue[i].buf);
        }
        sendQueue.clear();
    }
}

void SZTBGPSPusher::SetConsumer(RdkafkaConsumer* consumer, const std::string& topic)
{
    m_consumer = consumer;
    m_consumer->Consume(topic, ConsumeCB, this);
}

int SZTBGPSPusher::StartInternalServer(const std::string& ip, uint16_t port)
{
    LOG_INFO << "StartInternalServer " << ip << " " << port;
    CCMS_NETADDR addr = { {0}, 0};
    memcpy(addr.chIP, ip.c_str(), ip.length());
    addr.nPort = port;
    m_hIntServer = BATNetSDK_CreateServerObj(&addr);
    BATNetSDK_SetMsgCallBack(m_hIntServer, IntRecvCB, this);
    BATNetSDK_Start(m_hIntServer);
    return 0;
}

int SZTBGPSPusher::StartExternalServer(const std::string& ip, uint16_t port)
{
    LOG_INFO << "StartExternalServer " << ip << " " << port;
    CCMS_NETADDR addr = { {0}, 0};
    memcpy(addr.chIP, ip.c_str(), ip.length());
    addr.nPort = port;
    m_hExtServer = BATNetSDKRaw_CreateServerObj(&addr);
    BATNetSDKRaw_SetConnCallBack(m_hExtServer, ExtConnCB, this);
    //BATNetSDKRaw_SetMsgCallBack(m_hIntServer, RecvCB, this);
    BATNetSDKRaw_Start(m_hExtServer);
    return 0;
}

int SZTBGPSPusher::RefreshWihteList()
{
    muduo::MutexLockGuard lock(m_mutexConn);
    m_condConn.notifyAll();
    return 0;
}

void SZTBGPSPusher::OnConsume(const std::string& key, void* payload, size_t len) {
  LOG_DEBUG << key << " " << (char*) payload;
// chVehicleID,nLongitude/1000000.0,nLatitude/1000000.0,stDateTime,nDeviceID,nSpeed,nDirect,nStatus,nAlarmType,chSimCard,,nVehicleColor
// 粤B6BR41,114.015770,22.666000,2017-06-30 10:51:53,1343457,33,251,0,0,13046711703,,蓝色
// 粤BU3536,111.992271,25.579161,2017-06-30 10:51:00,1584877,50,301,0,0,13428740980,,黄色
// 粤BQ1300,114.049339,22.656984,2017-06-30 10:51:47,1495331,43,177,0,0,1064882081009,,黄色
// 粤BW0Q45,114.053398,22.525167,2017-06-30 10:51:53,1319292,0,119,0,0,13046711598,,蓝色
// 粤B2NW56,113.813301,22.623899,2017-06-30 10:51:54,1405816,0,0,0,0,13509693414,,蓝色

//  SZTBGPS_Str2PositionData(payload, T_CCMS_POSITION_DATA);
//  SZTBGPS_PackPositionData(T_CCMS_POSITION_DATA, sendBuf);
//  DoPusher(sendBuf, packLen);
}

int SZTBGPSPusher::OnIntRecv(int sessionId, int msgId, const char* buf, int len)
{
//  Util::HexDump(buf, len);
  return len;
}

int SZTBGPSPusher::OnExtConn(int sessionId, int status, const char* ip, unsigned short port)
{
    LOG_INFO << "OnExtConn " << sessionId << " " << status << " " << ip << ":" << port;
    std::string strIP = ip;
    if (status == 0)
    {
        {
            muduo::MutexLockGuard lock(m_mutexConn);
            if (std::count(m_whiteList.begin(), m_whiteList.end(), strIP) == 0)
            {
                LOG_WARN << "not in whitelist! " << strIP;
                return -1;
            }
        }
        if (m_clientList.count(strIP) >= 3)
        {
            LOG_WARN << "reach the conn limit of ip " << strIP;
            return -1;
        }
        for (ClientMap::iterator it = m_clientList.lower_bound(strIP);
            it != m_clientList.upper_bound(strIP); ++it)
        {
            if (it->second.first == port)
            {
                return 0; // wbf.mark
            }
        }
        m_clientList.insert(std::make_pair(strIP, std::make_pair(port, sessionId)));
    }
    else if (status == 1)
    {
        for (ClientMap::iterator it = m_clientList.lower_bound(strIP);
            it != m_clientList.upper_bound(strIP); ++it)
        {
            if (it->second.first == port)
            {
                m_clientList.erase(it);
                break;
            }
        }
    }
    return 0;
}

void SZTBGPSPusher::DoPusher(const char* buf, size_t len)
{
    BATNetSDKRaw_SendAll(m_hExtServer, buf, len);
}

void SZTBGPSPusher::LoadWhiteList()
{
    m_whiteList.clear();

    FILE * fp;
    fp = fopen(m_whiteListFile.c_str(), "r");
    if (fp == NULL)
    {
        LOG_ERROR<< "WhiteList fopen failed! path = " << m_whiteListFile;
        return;
    }

    LOG_INFO << "load WhiteList:";
    char line[1024] = {0};
    char ip[64] = {0};
    while (fgets(line, 1024, fp))
    {
        if (line[0] == '#' || strcmp(line, "") == 0)
        {
            continue;
        }
        ip[0] = 0;
        sscanf(line, "%s", ip);
        if (strcmp(ip, "") == 0)
        {
            continue;
        }
        m_whiteList.push_back(ip);
        LOG_INFO << "  > " << ip;
    }

    fclose(fp);
}

void SZTBGPSPusher::CheckClientList()
{
    for (ClientMap::iterator it = m_clientList.begin(); it != m_clientList.end();)
    {
        std::string ip = it->first;
        if (std::count(m_whiteList.begin(), m_whiteList.end(), ip) == 0)
        {
            for (; it != m_clientList.upper_bound(ip); ++it)
            {
                BATNetSDKRaw_Disconnect(m_hExtServer, it->second.second);
            }
            size_t count = m_clientList.erase(ip);
            LOG_INFO << "Disconnect [" << count << "] clients not in whiteList";
        }
        it = m_clientList.upper_bound(ip);
    }
}
