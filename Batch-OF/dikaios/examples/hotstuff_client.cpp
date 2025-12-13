/**
 * Copyright 2018 VMware
 * Copyright 2018 Ted Yin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cassert>
#include <random>
#include <memory>
#include <signal.h>
#include <sys/time.h>
#include <algorithm>

#include "salticidae/type.h"
#include "salticidae/netaddr.h"
#include "salticidae/network.h"
#include "salticidae/util.h"

#include "hotstuff/util.h"
#include "hotstuff/type.h"
#include "hotstuff/client.h"
#include "small_bank.h"

using salticidae::Config;

using hotstuff::ReplicaID;
using hotstuff::NetAddr;
using hotstuff::EventContext;
using hotstuff::MsgReqCmd;
using hotstuff::MsgRespCmd;
using hotstuff::CommandDummy;
using hotstuff::HotStuffError;
using hotstuff::uint256_t;
using hotstuff::opcode_t;
using hotstuff::command_t;

EventContext ec;
ReplicaID proposer;
size_t max_async_num;   // tx/s
int max_iter_num;
uint32_t cid;
uint32_t cnt = 0;
uint32_t nfaulty;

static uint32_t starting_cnt;
static size_t target_bytes_amount;

struct Request {
    command_t cmd;
    size_t confirmed;
    salticidae::ElapsedTime et;
    Request(const command_t &cmd): cmd(cmd), confirmed(0) { et.start(); }
};

using Net = salticidae::MsgNetwork<opcode_t>;
using TimerEvent = salticidae::TimerEvent;

std::unordered_map<ReplicaID, Net::conn_t> conns;
std::unordered_map<const uint256_t, Request> waiting;
std::vector<NetAddr> replicas;
std::vector<std::pair<struct timeval, double>> elapsed;
std::unique_ptr<Net> mn;
SmallBankManager *small_bank_manager;
double time_consumed_in_cmd_generation = 0.0;

void connect_all() {
    for (size_t i = 0; i < replicas.size(); i++) {
        auto conn = mn->connect_sync(replicas[i]);
        conns.insert(std::make_pair(i, conn));
    }
}

bool try_send(bool check = true) {
    (void)check;

    if (!max_iter_num)
        return false;

    salticidae::ElapsedTime et_cmd_generation;
    et_cmd_generation.start();

    auto next_tx = small_bank_manager->get_next_transaction_serialized();
    auto cmd = new CommandDummy(cid, cnt++, next_tx);

    MsgReqCmd msg(*cmd, target_bytes_amount);

    std::vector<Net::conn_t> shuffled_conns;
    shuffled_conns.reserve(conns.size());
    for (auto &p: conns) shuffled_conns.push_back(p.second);
    static thread_local std::mt19937_64 rng(std::random_device{}());
    std::shuffle(shuffled_conns.begin(), shuffled_conns.end(), rng);

    for (auto &c: shuffled_conns)
        mn->send_msg(msg, c);

#ifndef HOTSTUFF_ENABLE_BENCHMARK
    std::string data;
    const uint64_t *payload = cmd->get_payload();
    for (int i = 0; i < cmd->get_payload_size(); i++) {
        data += std::to_string(payload[i]) + " ";
    }

    HOTSTUFF_LOG_INFO("send new cmd %.10s with payload (size %ld) %s",
                        get_hex(cmd->get_hash()).c_str(),
                        cmd->get_payload_size(),
                        data.c_str());
#endif

    et_cmd_generation.stop();
    time_consumed_in_cmd_generation += et_cmd_generation.elapsed_sec;

    waiting.insert(std::make_pair(cmd->get_hash(), Request(cmd)));

    if (max_iter_num > 0)
        max_iter_num--;

    return true;
}

void client_resp_cmd_handler(MsgRespCmd &&msg, const Net::conn_t &) {
    auto &fin = msg.fin;
    HOTSTUFF_LOG_DEBUG("got %s", std::string(msg.fin).c_str());

    const uint256_t &cmd_hash = fin.cmd_hash;
    auto it = waiting.find(cmd_hash);
    if (it == waiting.end()) return;
    auto &et = it->second.et;
    et.stop();

    if (++it->second.confirmed <= nfaulty) return; // wait for f + 1 ack

    struct timeval tv;
    gettimeofday(&tv, nullptr);
    elapsed.push_back(std::make_pair(tv, et.elapsed_sec));

    waiting.erase(it);

}

std::pair<std::string, std::string> split_ip_port_cport(const std::string &s) {
    auto ret = salticidae::trim_all(salticidae::split(s, ";"));
    return std::make_pair(ret[0], ret[1]);
}

int main(int argc, char **argv) {
    Config config("hotstuff.conf");

    auto opt_sb_users = Config::OptValInt::create(10);
    auto opt_sb_prob_choose_mtx = Config::OptValDouble::create(0.9);
    auto opt_sb_skew_factor = Config::OptValDouble::create(0.1);
    auto opt_fairness_parameter = Config::OptValDouble::create(1);  // Themis
    auto opt_idx = Config::OptValInt::create(0);
    auto opt_replicas = Config::OptValStrVec::create();
    auto opt_max_iter_num = Config::OptValInt::create(100);
    auto opt_max_async_num = Config::OptValInt::create(10); // tx/s
    auto opt_cid = Config::OptValInt::create(-1);
    auto opt_max_cli_msg = Config::OptValInt::create(65536); // 64K by default

    auto shutdown = [&](int) { ec.stop(); };
    salticidae::SigEvent ev_sigint(ec, shutdown);
    salticidae::SigEvent ev_sigterm(ec, shutdown);
    ev_sigint.add(SIGINT);
    ev_sigterm.add(SIGTERM);

    mn = std::make_unique<Net>(ec, Net::Config().max_msg_size(opt_max_cli_msg->get()));
    mn->reg_handler(client_resp_cmd_handler);
    mn->start();

    config.add_opt("sb-users", opt_sb_users, Config::SET_VAL);
    config.add_opt("sb-prob-choose_mtx", opt_sb_prob_choose_mtx, Config::SET_VAL);
    config.add_opt("sb-skew-factor", opt_sb_skew_factor, Config::SET_VAL);
    config.add_opt("fairness-parameter", opt_fairness_parameter, Config::SET_VAL);  // Themis
    config.add_opt("idx", opt_idx, Config::SET_VAL);
    config.add_opt("cid", opt_cid, Config::SET_VAL);
    config.add_opt("replica", opt_replicas, Config::APPEND);
    config.add_opt("iter", opt_max_iter_num, Config::SET_VAL);
    config.add_opt("max-async", opt_max_async_num, Config::SET_VAL);
    config.add_opt("max-cli-msg", opt_max_cli_msg, Config::SET_VAL, 'S',
                   "the maximum client message size");

    config.parse(argc, argv);

    auto idx = opt_idx->get();
    max_iter_num = opt_max_iter_num->get();
    max_async_num = opt_max_async_num->get();

    target_bytes_amount = opt_max_cli_msg->get();

    HOTSTUFF_LOG_INFO(
        "Message Size Bytes: %d", target_bytes_amount
    );

    std::vector<std::string> raw;
    for (const auto &s: opt_replicas->get())
    {
        auto res = salticidae::trim_all(salticidae::split(s, ","));
        if (res.size() < 1)
            throw HotStuffError("format error");
        raw.push_back(res[0]);
    }

    if (!(0 <= idx && (size_t)idx < raw.size() && raw.size() > 0))
        throw std::invalid_argument("out of range");
    cid = opt_cid->get() != -1 ? opt_cid->get() : idx;

    std::mt19937_64 rng(std::random_device{}());
    starting_cnt = std::uniform_int_distribution<uint32_t>()(rng);
    cnt = starting_cnt;

    for (const auto &p: raw)
    {
        auto _p = split_ip_port_cport(p);
        size_t _;
        replicas.push_back(NetAddr(NetAddr(_p.first).ip, htons(stoi(_p.second, &_))));
    }

    double fairness_parameter = opt_fairness_parameter->get();
    // nfaulty = (replicas.size() - 1) / 4;
    nfaulty = (replicas.size() * ((2 * fairness_parameter) - 1)) / 4;
    HOTSTUFF_LOG_INFO("nfaulty = %zu", nfaulty);

    HOTSTUFF_LOG_INFO("opt_sb_users = %ld, opt_sb_prob_choose_mtx = %f, opt_sb_skew_factor = %f",
                      opt_sb_users->get(),
                      opt_sb_prob_choose_mtx->get(),
                      opt_sb_skew_factor->get());

    small_bank_manager = new SmallBankManager(opt_sb_users->get(),
                                              opt_sb_prob_choose_mtx->get(),
                                              opt_sb_skew_factor->get());

    connect_all();

    const uint64_t PRECISION = 1;
    const double BURST_DURATION_SEC = 1.0 / PRECISION;
    const double BURST_DURATION_MS  = BURST_DURATION_SEC * 1000.0;
    const uint64_t burst = static_cast<uint64_t>(max_async_num) / PRECISION;

    HOTSTUFF_LOG_INFO("Transactions rate: %zu tx/s (max-async)", max_async_num);
    if (burst == 0) {
        HOTSTUFF_LOG_WARN("burst == 0 (max-async == 0): no transactions will be sent");
    }
    HOTSTUFF_LOG_INFO("Start sending transactions");

    TimerEvent send_timer(ec, [&](TimerEvent &te) {
        if (!max_iter_num) {
            HOTSTUFF_LOG_INFO("client finished sending all transactions, stopping event loop");
            ec.stop();
            return;
        }

        salticidae::ElapsedTime loop_et;
        loop_et.start();

        uint64_t sent_in_burst = 0;
        for (uint64_t x = 0; x < burst && max_iter_num; x++) {
            // if (x == (cnt - starting_cnt) % burst) {
            //     HOTSTUFF_LOG_INFO("Sending sample transaction #%u", cnt);
            // }
            if (!try_send())
                break;
            sent_in_burst++;
        }

        loop_et.stop();
        double elapsed_ms = loop_et.elapsed_sec * 1000.0;

        if (sent_in_burst > 0 && elapsed_ms > BURST_DURATION_MS) {
            HOTSTUFF_LOG_INFO("Transaction rate too high for this client: "
                            "burst of %lu tx took %.3f ms (target %.3f ms)",
                            (unsigned long)sent_in_burst,
                            elapsed_ms,
                            BURST_DURATION_MS);
        }

        te.add(BURST_DURATION_SEC);
    });


    send_timer.add(BURST_DURATION_SEC);

    ec.dispatch();

    for (const auto &e: elapsed)
    {
        char fmt[64];
        struct tm *tmp = localtime(&e.first.tv_sec);
        strftime(fmt, sizeof fmt, "%Y-%m-%d %H:%M:%S.%%06u [hotstuff info] %%.6f\n", tmp);
        fprintf(stderr, fmt, e.first.tv_usec, e.second);
    }
    
    HOTSTUFF_LOG_INFO("time_consumed_in_cmd_generation = %lf", time_consumed_in_cmd_generation);
    return 0;
}
