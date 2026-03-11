package cn.inlook.cex;

import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.web3j.abi.FunctionEncoder;
import org.web3j.abi.FunctionReturnDecoder;
import org.web3j.abi.TypeReference;
import org.web3j.abi.datatypes.Address;
import org.web3j.abi.datatypes.Function;
import org.web3j.abi.datatypes.Type;
import org.web3j.abi.datatypes.generated.Uint256;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameterName;
import org.web3j.protocol.core.methods.request.Transaction;
import org.web3j.protocol.core.methods.response.EthCall;
import org.web3j.protocol.http.HttpService;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

// [ZH] Web3 实战：查询 ERC-20 智能合约代币 (USDT) 余额
// [EN] Web3 Action: Query ERC-20 Smart Contract Token (USDT) Balance
@Slf4j
public class Web3Erc20HelloWorld {

    public static void main(String[] args) {
        // [ZH] 开始初始化 Web3j 客户端以进行 ERC-20 查询...
        // [EN] Starting Web3j client for ERC-20 query...
        log.info("Starting Web3j client for ERC-20 query...");

        // [ZH] 1. 构造底层的 OkHttpClient 并配置超时时间
        // [EN] 1. Construct the underlying OkHttpClient and set timeouts
        OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder()
                .connectTimeout(10, TimeUnit.SECONDS)
                .readTimeout(10, TimeUnit.SECONDS);

        // [ZH] 配置本地代理 (根据之前的配置，Clash 端口为 7890)
        // [EN] Configure local proxy (Clash port is 7890 based on previous config)
        Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("127.0.0.1", 7890));
        clientBuilder.proxy(proxy);

        // [ZH] 2. 添加拦截器：伪装成真实的 Chrome 浏览器，防止被 WAF 拦截
        // [EN] 2. Add interceptor: Spoof as a real Chrome browser to prevent WAF interception
        clientBuilder.addInterceptor(chain -> {
            Request newRequest = chain.request().newBuilder()
                    .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0")
                    .build();
            return chain.proceed(newRequest);
        });

        // [ZH] 3. 使用备用节点 LlamaRPC
        // [EN] 3. Use the backup node LlamaRPC
        String rpcUrl = "https://eth.llamarpc.com";
        Web3j web3j = Web3j.build(new HttpService(rpcUrl, clientBuilder.build(), false));

        try {
            // [ZH] 4. 核心参数准备
            // [EN] 4. Core parameter preparation

            // [ZH] 设定我们要查询的钱包地址 (以太坊创始人 V神的公开地址)
            // [EN] Set the target wallet address (Vitalik Buterin's public address)
            String targetAddress = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045";

            // [ZH] 设定以太坊主网上真实的 USDT 智能合约地址
            // [EN] Set the real USDT smart contract address on the Ethereum mainnet
            String usdtContractAddress = "0xdAC17F958D2ee523a2206206994597C13D831ec7";

            // [ZH] 正在查询指定地址的 USDT 余额
            // [EN] Querying USDT balance for the specified address
            log.info("Querying USDT balance for address: {}", targetAddress);

            // [ZH] 5. 构建智能合约的方法调用 (ABI 编码)
            // [EN] 5. Build the smart contract method call (ABI Encoding)
            // [ZH] 这相当于在 Solidity 智能合约中调用：balanceOf(address)
            // [EN] This is equivalent to calling in a Solidity smart contract: balanceOf(address)
            Function function = new Function(
                    "balanceOf", // [ZH] 调用的方法名 / [EN] Method name to call
                    Arrays.asList(new Address(targetAddress)), // [ZH] 传入的参数列表 / [EN] Input parameter list
                    Collections.singletonList(new TypeReference<Uint256>() {}) // [ZH] 期望的返回类型 (无符号256位整数) / [EN] Expected return type (unsigned 256-bit integer)
            );

            // [ZH] 将 Java 的 Function 对象编码为十六进制的机器指令
            // [EN] Encode the Java Function object into a hexadecimal machine instruction
            String encodedFunction = FunctionEncoder.encode(function);

            // [ZH] 6. 将指令打包成一个 "只读" 的以太坊交易 (eth_call)
            // [EN] 6. Pack the instruction into a "read-only" Ethereum transaction (eth_call)
            // [ZH] 注意：eth_call 不消耗 Gas 手续费，因为它只读取状态，不修改状态
            // [EN] Note: eth_call does not consume Gas fees because it only reads state and does not modify it
            Transaction transaction = Transaction.createEthCallTransaction(
                    targetAddress, // [ZH] 发起方 (这里填被查询的地址即可) / [EN] Sender (fill in the queried address here)
                    usdtContractAddress, // [ZH] 目标方 (必须是 USDT 的合约地址) / [EN] Target (must be the USDT contract address)
                    encodedFunction // [ZH] 编码后的方法指令 / [EN] Encoded method instruction
            );

            // [ZH] 7. 发送 RPC 请求并获取结果
            // [EN] 7. Send RPC request and get the result
            EthCall response = web3j.ethCall(transaction, DefaultBlockParameterName.LATEST).send();

            // [ZH] 校验节点返回值是否包含业务级错误
            // [EN] Check if the node's response contains business-level errors
            if (response.hasError()) {
                log.error("RPC node error: {}", response.getError().getMessage());
                return;
            }

            // [ZH] 8. 对合约返回的十六进制结果进行 ABI 解码
            // [EN] 8. Perform ABI decoding on the hexadecimal result returned by the contract
            String returnValue = response.getValue();
            List<Type> decodedValues = FunctionReturnDecoder.decode(returnValue, function.getOutputParameters());

            if (decodedValues.isEmpty()) {
                log.error("Failed to decode contract response.");
                return;
            }

            // [ZH] 提取出底层的极小单位数值
            // [EN] Extract the underlying smallest unit value
            BigInteger balanceInSmallestUnit = (BigInteger) decodedValues.get(0).getValue();

            // ==========================================
            // [ZH] 致命陷阱警告 (CEX 研发必考题)
            // [EN] Fatal Trap Warning (Must-know for CEX R&D)
            // [ZH] ETH 的精度是 18 位，但 USDT 的精度只有 6 位！
            // [EN] ETH has 18 decimal places, but USDT only has 6!
            // ==========================================
            BigDecimal divisor = BigDecimal.TEN.pow(6);
            BigDecimal usdtBalance = new BigDecimal(balanceInSmallestUnit).divide(divisor);

            // [ZH] 打印最终的 USDT 余额
            // [EN] Print the final USDT balance
            log.info("Vitalik's USDT Balance: {} USDT", usdtBalance.toPlainString());

        } catch (Exception e) {
            // [ZH] 捕获查询过程中的异常
            // [EN] Catch exceptions during the query process
            log.error("Query failed", e);
        } finally {
            // [ZH] 关闭客户端，释放资源
            // [EN] Shutdown client and release resources
            web3j.shutdown();
        }
    }
}