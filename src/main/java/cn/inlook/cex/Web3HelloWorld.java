package cn.inlook.cex;

import lombok.extern.slf4j.Slf4j;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameterName;
import org.web3j.protocol.core.methods.response.EthGetBalance;
import org.web3j.protocol.http.HttpService;
import org.web3j.utils.Convert;

import java.math.BigDecimal;
import java.math.BigInteger;

// [ZH] Web3 初体验：连接以太坊主网查询余额
// [EN] Web3 First Taste: Connect to Ethereum Mainnet and query balance
@Slf4j
public class Web3HelloWorld {

    public static void main(String[] args) {
        log.info("🌐 正在初始化 Web3j 客户端...");

        // [ZH] 1. 连接到以太坊主网 RPC 节点 (这里使用 Cloudflare 提供的免费公共节点)
        // [EN] 1. Connect to Ethereum Mainnet RPC node (using Cloudflare's free public node)
        String rpcUrl = "https://cloudflare-eth.com";
        Web3j web3j = Web3j.build(new HttpService(rpcUrl));

        try {
            // [ZH] 验证是否连接成功，获取当前以太坊主网的最新区块高度
            // [EN] Verify connection by getting the latest block number
            BigInteger latestBlock = web3j.ethBlockNumber().send().getBlockNumber();
            log.info("✅ 成功连接到以太坊主网！当前最新区块高度 / Latest Block: {}", latestBlock);

            // [ZH] 2. 设定我们要查询的钱包地址 (这是以太坊创始人 V神的公开地址)
            // [EN] 2. Set the target wallet address (Vitalik Buterin's public address)
            String vitalikAddress = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045";

            // [ZH] 3. 发起 RPC 请求，查询该地址在 "最新区块" 的 ETH 余额
            // [EN] 3. Send RPC request to query the ETH balance of this address at the "latest" block
            log.info("🔍 正在查询 V神 钱包地址的余额: {}", vitalikAddress);
            EthGetBalance balanceResponse = web3j.ethGetBalance(
                    vitalikAddress,
                    DefaultBlockParameterName.LATEST // 查询最新状态
            ).send();

            // [ZH] 拿到的是以 Wei 为单位的极小数值，Web3 底层没有小数
            // [EN] The result is in Wei (the smallest unit), as Web3 has no decimals at the protocol level
            BigInteger balanceInWei = balanceResponse.getBalance();

            // [ZH] 4. 将 Wei 转换为人类可读的 Ether (1 Ether = 10^18 Wei)
            // [EN] 4. Convert Wei to human-readable Ether
            BigDecimal balanceInEth = Convert.fromWei(balanceInWei.toString(), Convert.Unit.ETHER);

            log.info("💰 V神的钱包余额为 / Balance: {} ETH", balanceInEth.toPlainString());

        } catch (Exception e) {
            log.error("❌ 与区块链节点交互失败 / Failed to interact with blockchain node", e);
        } finally {
            // [ZH] 关闭客户端，释放资源
            // [EN] Shutdown client and release resources
            web3j.shutdown();
        }
    }
}