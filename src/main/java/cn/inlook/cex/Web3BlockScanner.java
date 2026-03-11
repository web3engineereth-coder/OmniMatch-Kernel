package cn.inlook.cex;

import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.web3j.abi.EventEncoder;
import org.web3j.abi.FunctionReturnDecoder;
import org.web3j.abi.TypeReference;
import org.web3j.abi.datatypes.Address;
import org.web3j.abi.datatypes.Event;
import org.web3j.abi.datatypes.Type;
import org.web3j.abi.datatypes.generated.Uint256;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameterNumber;
import org.web3j.protocol.core.methods.request.EthFilter;
import org.web3j.protocol.core.methods.response.EthLog;
import org.web3j.protocol.core.methods.response.Log;
import org.web3j.protocol.http.HttpService;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

// [ZH] Web3 实战：生产级链上扫块引擎 (单块步进模式)
// [EN] Web3 Action: Production-grade Block Scanner (Single-block Stepping Mode)
@Slf4j
public class Web3BlockScanner {

    public static void main(String[] args) {
        log.info("Initializing production-grade USDT block scanner...");

        // [ZH] 1. 强化 OkHttp 配置：大幅提升超时阈值，应对大数据量返回
        // [EN] 1. Enhance OkHttp Config: Significantly increase timeouts for large data responses
        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS);

        // [ZH] 强制绑定本地代理端口 7890
        // [EN] Force bind local proxy port 7890
        Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("127.0.0.1", 7890));
        clientBuilder.proxy(proxy);

        // [ZH] 注入浏览器伪装头，绕过节点 WAF 拦截
        // [EN] Inject Browser User-Agent to bypass node WAF
        clientBuilder.addInterceptor(chain -> {
            Request newRequest = chain.request().newBuilder()
                    .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0")
                    .build();
            return chain.proceed(newRequest);
        });

        // [ZH] 2. 连接以太坊 RPC 节点
        // [EN] 2. Connect to Ethereum RPC node
        String rpcUrl = "https://eth.llamarpc.com";
        Web3j web3j = Web3j.build(new HttpService(rpcUrl, clientBuilder.build(), false));

        try {
            // [ZH] USDT 合约地址与 Transfer 事件定义
            // [EN] USDT Contract Address and Transfer Event Definition
            String usdtContractAddress = "0xdAC17F958D2ee523a2206206994597C13D831ec7";
            Event transferEvent = new Event("Transfer",
                    Arrays.asList(
                            new TypeReference<Address>(true) {},
                            new TypeReference<Address>(true) {},
                            new TypeReference<Uint256>(false) {}
                    )
            );

            String eventSignatureHash = EventEncoder.encode(transferEvent);
            log.info("Listening for event signature: {}", eventSignatureHash);

            // [ZH] 3. 获取初始高度。在生产环境中，这个高度应该从数据库中读取
            // [EN] 3. Get initial block height. In production, this should be read from a database
            BigInteger lastProcessedBlock = web3j.ethBlockNumber().send().getBlockNumber();
            log.info("Scanner started. Initial block height: {}", lastProcessedBlock);

            // [ZH] 4. 进入核心扫描循环
            // [EN] 4. Enter core scanning loop
            while (true) {
                try {
                    BigInteger latestBlock = web3j.ethBlockNumber().send().getBlockNumber();

                    // [ZH] 检查是否有待处理的新区块
                    // [EN] Check if there are new blocks to process
                    if (latestBlock.compareTo(lastProcessedBlock) > 0) {

                        // [ZH] 核心策略：每次只扫描一个区块，彻底解决超时问题
                        // [EN] Core Strategy: Scan only one block at a time to solve timeout issues
                        BigInteger targetBlock = lastProcessedBlock.add(BigInteger.ONE);
                        log.info("Scanning single block: {}", targetBlock);

                        EthFilter filter = new EthFilter(
                                new DefaultBlockParameterNumber(targetBlock),
                                new DefaultBlockParameterNumber(targetBlock),
                                usdtContractAddress
                        );
                        filter.addSingleTopic(eventSignatureHash);

                        // [ZH] 发起 RPC 请求获取日志
                        // [EN] Send RPC request to fetch logs
                        EthLog ethLog = web3j.ethGetLogs(filter).send();

                        if (ethLog.hasError()) {
                            log.error("RPC Error on block {}: {}", targetBlock, ethLog.getError().getMessage());
                            Thread.sleep(2000);
                            continue;
                        }

                        List<EthLog.LogResult> logs = ethLog.getLogs();

                        // [ZH] 5. 解析并处理转账日志
                        // [EN] 5. Parse and process transfer logs
                        for (EthLog.LogResult logResult : logs) {
                            Log logObj = (Log) logResult.get();
                            List<String> topics = logObj.getTopics();

                            if (topics.size() >= 3) {
                                // [ZH] 提取 From 和 To 地址 (去掉前导零)
                                // [EN] Extract From and To addresses (remove leading zeros)
                                String fromAddress = "0x" + topics.get(1).substring(26);
                                String toAddress = "0x" + topics.get(2).substring(26);

                                // [ZH] 解码金额 (Data 字段)
                                // [EN] Decode amount (Data field)
                                String data = logObj.getData();
                                List<Type> decodedData = FunctionReturnDecoder.decode(data, transferEvent.getNonIndexedParameters());

                                if (!decodedData.isEmpty()) {
                                    BigInteger rawValue = (BigInteger) decodedData.get(0).getValue();
                                    // [ZH] 精度转换：USDT 为 6 位
                                    // [EN] Precision conversion: USDT is 6 decimals
                                    BigDecimal amount = new BigDecimal(rawValue).divide(BigDecimal.TEN.pow(6));

                                    log.info("[USDT Transfer Detected] Block: {} | Hash: {} | From: {} | To: {} | Amount: {} USDT",
                                            logObj.getBlockNumber(), logObj.getTransactionHash(), fromAddress, toAddress, amount.toPlainString());
                                }
                            }
                        }

                        // [ZH] 处理完成，更新进度
                        // [EN] Processing complete, update progress
                        lastProcessedBlock = targetBlock;

                        // [ZH] 追赶模式：如果当前进度落后于主网最新块，则不休眠直接进入下一块
                        // [EN] Catch-up Mode: If current progress lags behind, skip sleep and proceed to next block
                        if (lastProcessedBlock.compareTo(latestBlock) < 0) {
                            continue;
                        }
                    }

                    // [ZH] 已追上进度，进入标准等待模式 (12秒为以太坊出块周期)
                    // [EN] Caught up, enter standard wait mode (12s block time)
                    Thread.sleep(12000);

                } catch (Exception e) {
                    log.error("Cycle error: {}", e.getMessage());
                    Thread.sleep(5000); // [ZH] 出错后短休眠 / [EN] Short sleep on error
                }
            }

        } catch (Exception e) {
            log.error("Fatal error starting scanner", e);
        } finally {
            web3j.shutdown();
        }
    }
}