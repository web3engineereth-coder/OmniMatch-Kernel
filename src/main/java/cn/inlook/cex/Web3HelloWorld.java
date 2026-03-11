package cn.inlook.cex;

import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameterName;
import org.web3j.protocol.core.methods.response.EthGetBalance;
import org.web3j.protocol.http.HttpService;
import org.web3j.utils.Convert;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.concurrent.TimeUnit;

// [ZH] Web3 终极探针版：底层 OkHttp 显式代理 + 报文抓取
// [EN] Web3 Ultimate Probe: Explicit OkHttp Proxy + Payload Sniffing
@Slf4j
public class Web3HelloWorld {

    public static void main(String[] args) {
        log.info("正在初始化 Web3j 客户端 (底层 OkHttp 显式接管版)...");

        // [ZH] 1. 构造底层的 OkHttpClient，设置超时时间
        // [EN] 1. Construct the underlying OkHttpClient, set timeout durations
        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder()
                .connectTimeout(10, TimeUnit.SECONDS)
                .readTimeout(10, TimeUnit.SECONDS);

        // ==========================================
        // 核心控制台 / Core Console:
        // [ZH] 显式绑定本地代理 (根据之前的配置，Clash 端口为 7890)
        // [EN] Explicitly bind local proxy (Clash port is 7890 based on previous config)
        // ==========================================
        Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("127.0.0.1", 7890));
        clientBuilder.proxy(proxy);

        // [ZH] 2. 添加强大的拦截器：伪装浏览器 + 抓取真实返回值
        // [EN] 2. Add a powerful interceptor: spoof browser + capture real response
        clientBuilder.addInterceptor(chain -> {
            Request originalRequest = chain.request();
            Request newRequest = originalRequest.newBuilder()
                    // [ZH] 伪装成真实的 Chrome 浏览器，防止被 WAF 拦截
                    // [EN] Spoof as a real Chrome browser to prevent WAF interception
                    .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0")
                    .header("Accept", "application/json")
                    .build();

            Response response = chain.proceed(newRequest);

            // [ZH] 偷看一眼服务器到底返回了什么鬼东西 (核心抓包逻辑)
            // [EN] Peek at what the server actually returned (Core packet sniffing logic)
            ResponseBody peekBody = response.peekBody(1024 * 1024);
            String rawResponse = peekBody.string();

            // [ZH] 正常的 RPC 响应一定是 JSON 格式 (以 '{' 开头)
            // [EN] A normal RPC response must be in JSON format (starts with '{')
            if (!rawResponse.startsWith("{")) {
                log.error("抓到罪魁祸首了！节点并没有返回 JSON，而是返回了以下内容 / Caught the culprit! The node returned the following instead of JSON:\n{}", rawResponse);
            }

            return response;
        });

        // [ZH] 3. 换一个极其干净、没有防爬虫的备用节点 LlamaRPC
        // [EN] 3. Switch to a clean, non-anti-bot backup node LlamaRPC
        String rpcUrl = "https://eth.llamarpc.com";
        HttpService httpService = new HttpService(rpcUrl, clientBuilder.build(), false);
        Web3j web3j = Web3j.build(httpService);

        try {
            // [ZH] 验证是否连接成功，获取当前以太坊主网的最新区块高度
            // [EN] Verify connection by getting the latest block number of Ethereum mainnet
            BigInteger latestBlock = web3j.ethBlockNumber().send().getBlockNumber();
            log.info("成功连接到以太坊主网！当前最新区块高度 / Latest Block: {}", latestBlock);

            // [ZH] 设定我们要查询的钱包地址 (这是以太坊创始人 V神的公开地址)
            // [EN] Set the target wallet address (Vitalik Buterin's public address)
            String vitalikAddress = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045";
            log.info("正在查询 V神 钱包地址的余额 / Querying Vitalik's wallet balance: {}", vitalikAddress);

            // [ZH] 发起 RPC 请求，查询该地址在 "最新区块" 的 ETH 余额
            // [EN] Send RPC request to query the ETH balance of this address at the "latest" block
            EthGetBalance balanceResponse = web3j.ethGetBalance(
                    vitalikAddress,
                    DefaultBlockParameterName.LATEST
            ).send();

            // [ZH] 校验节点返回值是否包含业务级错误
            // [EN] Check if the node's response contains business-level errors
            if (balanceResponse.hasError()) {
                log.error("节点业务错误 / Node business error: {}", balanceResponse.getError().getMessage());
                return;
            }

            // [ZH] 拿到的是以 Wei 为单位的极小数值，将 Wei 转换为人类可读的 Ether (1 Ether = 10^18 Wei)
            // [EN] The result is in Wei. Convert Wei to human-readable Ether (1 Ether = 10^18 Wei)
            BigDecimal balanceInEth = Convert.fromWei(balanceResponse.getBalance().toString(), Convert.Unit.ETHER);
            log.info("V神的钱包余额为 / Balance: {} ETH", balanceInEth.toPlainString());

        } catch (Exception e) {
            // [ZH] 捕获所有网络与解析异常
            // [EN] Catch all network and parsing exceptions
            log.error("交互最终失败 / Interaction ultimately failed", e);
        } finally {
            // [ZH] 关闭客户端，释放资源
            // [EN] Shutdown client and release resources
            web3j.shutdown();
        }
    }
}