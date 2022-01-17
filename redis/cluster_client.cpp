#include "redis/cluster_client.h"
#include <memory>
#include <folly/hash/Hash.h>
#include <folly/Random.h>

namespace redis
{
    namespace 
    {
        /*
         * Copyright 2001-2010 Georges Menie (www.menie.org)
         * Copyright 2010 Salvatore Sanfilippo (adapted to Redis coding style)
         * All rights reserved.
         * Redistribution and use in source and binary forms, with or without
         * modification, are permitted provided that the following conditions are met:
         *
         *     * Redistributions of source code must retain the above copyright
         *       notice, this list of conditions and the following disclaimer.
         *     * Redistributions in binary form must reproduce the above copyright
         *       notice, this list of conditions and the following disclaimer in the
         *       documentation and/or other materials provided with the distribution.
         *     * Neither the name of the University of California, Berkeley nor the
         *       names of its contributors may be used to endorse or promote products
         *       derived from this software without specific prior written permission.
         *
         * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND ANY
         * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
         * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
         * DISCLAIMED. IN NO EVENT SHALL THE REGENTS AND CONTRIBUTORS BE LIABLE FOR ANY
         * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
         * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
         * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
         * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
         * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
         * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
         */

         /* CRC16 implementation according to CCITT standards.
          *
          * Note by @antirez: this is actually the XMODEM CRC 16 algorithm, using the
          * following parameters:
          *
          * Name                       : "XMODEM", also known as "ZMODEM", "CRC-16/ACORN"
          * Width                      : 16 bit
          * Poly                       : 1021 (That is actually x^16 + x^12 + x^5 + 1)
          * Initialization             : 0000
          * Reflect Input byte         : False
          * Reflect Output CRC         : False
          * Xor constant to output CRC : 0000
          * Output for "123456789"     : 31C3
          */
        static const std::size_t SHARDS = 16383;

        static const uint16_t crc16tab[256] = {
            0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
            0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
            0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
            0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
            0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
            0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
            0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
            0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
            0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
            0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
            0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
            0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
            0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
            0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
            0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
            0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
            0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
            0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
            0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
            0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
            0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
            0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
            0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
            0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
            0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
            0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
            0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
            0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
            0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
            0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
            0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
            0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0
        };

        uint16_t crc16(const char* buf, int len) {
            int counter;
            uint16_t crc = 0;
            for (counter = 0; counter < len; counter++)
                crc = (crc << 8) ^ crc16tab[((crc >> 8) ^ *buf++) & 0x00FF];
            return crc;
        }

        int32_t CalsSlot(std::string_view key){
            // The following code is copied from: https://redis.io/topics/cluster-spec
            // And I did some minor changes.

            const auto* k = key.data();
            const auto keylen = key.size();

            // start-end indexes of { and }.
            std::size_t s = 0;
            std::size_t e = 0;

            // Search the first occurrence of '{'.
            for (s = 0; s < keylen; s++)
                if (k[s] == '{') break;

            // No '{' ? Hash the whole key. This is the base case.
            if (s == keylen) return crc16(k, keylen) & SHARDS;

            // '{' found? Check if we have the corresponding '}'.
            for (e = s + 1; e < keylen; e++)
                if (k[e] == '}') break;

            // No '}' or nothing between {} ? Hash the whole key.
            if (e == keylen || e == s + 1) return crc16(k, keylen) & SHARDS;

            // If we are here there is both a { and a } on its right. Hash
            // what is in the middle between { and }.
            return crc16(k + s + 1, e - s - 1) & SHARDS;
        }
    }

    //连接到集群单个节点,然后更新整个集群
    folly::SemiFuture<folly::Unit> ClusterConns::Connect(const std::string& host, int port, std::string pass /*= ""*/, int32_t timeout_ms)
    {
        auto conn = std::make_shared<Conn>(shared_from_this());
        conns_.emplace(std::make_pair(Node{ host,port,false }, conn));
        shareds_.emplace(std::make_pair(Slot{ 0,SHARDS }, Node{ host,port,false }));
        pass_ = pass;
        timeout_ms_ = timeout_ms;
        return conn->Connect(host, port,std::move(pass),0, timeout_ms);
    }

    void ClusterConns::Close()
    {
        //所有连接关闭
        for(auto& n:conns_)
        {
            if (n.second)n.second->Close();
        }
        conns_.clear();
    }

    folly::SemiFuture<Reply> ClusterConns::Query(int32_t slot,Command cmd)
    {
        const auto conn = GetConn(slot);
        if (!conn)return folly::makeSemiFuture<Reply>(std::runtime_error(fmt::format("redis cluster no valid connection to slot {}", slot)));
        return conn->Query(std::move(cmd));
    }

    void ClusterConns::Run(int32_t slot,Command cmd)
    {
        const auto conn = GetConn(slot);
        if (!conn)return;
        conn->Run(std::move(cmd));
    }

    Node ParseNodeInfo(Reply&& rpl)
    {
        if (!rpl.IsArray())folly::throw_exception(std::invalid_argument("need a array reply"));
        auto arr = std::move(rpl).AsArray();
        if (arr.size() < 2)folly::throw_exception(std::invalid_argument("cluster node info error"));

        return Node{ std::move(arr[0]).AsString(),static_cast<int32_t>(arr[1].AsInteger()),false };
    }
    std::pair<Slot,Node> ParseSlotInfo(Reply&& rpl)
    {
        if(!rpl.IsArray())folly::throw_exception(std::invalid_argument("need a array reply"));
        auto arr = std::move(rpl).AsArray();
        if (arr.size() < 3)folly::throw_exception(std::invalid_argument("slot info error"));
        const auto min = static_cast<int32_t>(arr[0].AsInteger());
        const auto max = static_cast<int32_t>(arr[1].AsInteger());
        //TODO parse slave node 
        return std::make_pair(Slot{ min,max }, ParseNodeInfo(std::move(arr[3])));
    }
    ClusterConns::Shards ClusterConns::ParseSlots(Reply&& rpl)
    {
        if (!rpl.IsArray())folly::throw_exception(std::invalid_argument("need a array reply"));
        auto arr = std::move(rpl).AsArray();
        if (arr.empty()) folly::throw_exception(std::invalid_argument("empty slots"));
        Shards shards;
        for(auto& slot: arr)
        {
            shards.emplace(ParseSlotInfo(std::move(slot)));
        }
        return shards;
    }

    folly::SemiFuture<folly::Unit> ClusterConns::UpdateShards(Shards&& shards)
    {
        //TODO 优化
        std::set<Node> news,olds,removes,adds;
        for(auto& s:shards)
        {
            news.insert(s.second);
        }
        for (auto& s : shareds_)
        {
            news.insert(s.second);
        }
        std::set_difference(olds.begin(),olds.end(), news.begin(),news.end(), std::inserter(removes,removes.end()));
        std::set_difference(news.begin(), news.end(), olds.begin(), olds.end(), std::inserter(adds, adds.end()));
        shareds_ = std::move(shards);
        //删除旧节点
        for(auto& r:removes)
        {
            auto it = conns_.find(r);
            if (it == conns_.end())continue;
            if(it->second)it->second->Close();
            conns_.erase(r);
        }
        //连接新节点
        std::vector<folly::SemiFuture<folly::Unit>> futs;
        for(auto& n:adds)
        {
            auto conn = std::make_shared<Conn>(shared_from_this());
            futs.push_back(conn->Connect(n.host, n.port, pass_, 0, timeout_ms_));
            conns_.emplace(std::make_pair(n, conn));
        }
        return folly::collectAll(futs).deferValue([](std::vector<folly::Try<folly::Unit>>&& results)
        {
            for(auto t: results)
            {
                if (t.hasException()) 
                    return folly::makeSemiFuture<folly::Unit>(
                        std::runtime_error(fmt::format("connect to cluster node error:{}",t.exception().what())));
            }
            return folly::makeSemiFuture();
        });
    }

    std::shared_ptr<Conn> ClusterConns::GetConn(int32_t slot)
    {
        const auto iter = shareds_.lower_bound(Slot{ slot,slot });
        if(iter==shareds_.end() || slot < iter->first.min)
        {
            return nullptr;
        }
        const auto& node = iter->second;
        const auto conn_iter = conns_.find(node);
        if(conn_iter==conns_.end())
        {
            return nullptr;
        }
        return conn_iter->second;
    }

    //更新槽位节点信息
    folly::SemiFuture<folly::Unit> ClusterClient::Update()
    {
        return Cmd("CLUSTER")
        .Arg("SLOTS")
        .Query()
        .thenValue([](Reply&& rpl)
        {
            return ClusterConns::ParseSlots(std::move(rpl));
        })
        .thenValue([share = shared()](ClusterConns::Shards&& shards)
        {
            return share->conn_->UpdateShards(std::move(shards));
        });
    }
    folly::Future<folly::Unit> ClusterClient::Connect(const std::string& host, int port, const std::string& pass,int dbindex,
        int32_t timeout_ms)
    {
        return conn_->Connect(host, port, pass,timeout_ms).deferValue([shared=shared()](folly::Unit&&)
        {
            return shared->Update();
        }).via(exec_);
    }

    void ClusterClient::Close()
    {
        if (conn_)conn_->Close();
    }
    int32_t CheckCommandSlot(Command& cmd)
    {
        if (cmd.Empty())return folly::Random::rand32(0, SHARDS+1);
        int32_t _slot = -1;
        for (auto& c : cmd.Commands())
        {
            if (c.key.empty())continue;
            auto cSlot = CalsSlot(c.key);
            if (_slot < 0)_slot = cSlot;
            if (_slot != cSlot)
            {
                folly::throw_exception(std::invalid_argument("pipeline commands in redis cluster must have same hash tag"));
            }
        }
        return _slot;
    }

    folly::Future<Reply> ClusterClient::Query(Command cmd)
    {
        const auto slot = CheckCommandSlot(cmd);
        return conn_->Query(slot,std::move(cmd)).via(exec_);
    }
    void ClusterClient::Run(Command cmd)
    {
        const auto slot = CheckCommandSlot(cmd);
        conn_->Run(slot, std::move(cmd));
    }
}

namespace std
{
    size_t hash<redis::Node>::operator()(const redis::Node& x) const
    {
        return folly::hash::hash_combine(x.host, x.port);
    }
}

