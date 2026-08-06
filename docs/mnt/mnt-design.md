# goshard MNT（Multi-Native Token）设计

## 文档目的

本文介绍 MNT 的目标、主要设计、PR 拆分和测试方案。具体实现见各 PR。

协议行为以 pyquarkchain / goquarkchain 为准。相同逻辑不重复展开，本文重点说明 MNT 在 goshard 中的整体设计，以及与 goquarkchain 的差异。

## 背景与目标

QuarkChain 的 MNT 允许一个账户同时持有多种原生代币，并允许交易指定：

- **transfer token**：本次价值转移使用哪种代币；
- **gas token**：本次交易计划使用哪种代币支付 gas。

QKC 是默认原生代币，继续保持现有余额和 EVM 语义；其他原生代币由 MNT 扩展管理。

除了交易直接转账，合约也可以通过 `transferMnt` 使用指定 MNT 转账；接收合约通过 `currentMntID` 获取本次转账的 token ID。

goshard 是基于 go-ethereum 的 QuarkChain 客户端。本次工作让 goshard 能读取并执行 QuarkChain MNT 状态。验收标准是：**相同前置状态和交易必须得到与参考实现相同的 state root**。

当前 PR 以 `core.Message` 作为执行入口；QuarkChain 交易类型、签名和交易解码尚未接入，后续 PR 再把交易字段转换为对应的 Message 字段。

## 整体设计

Message 中的两个 token ID 分别控制 gas 结算和 value 转账。两条路径修改账户余额后，统一写入 QuarkChain 账户格式：

```text
                       Message
              ┌──────────┴──────────┐
              ▼                     ▼
         GasTokenID           TransferTokenID
              │                     │
              ▼                     ▼
       buy-gas 与退款          本次 value 的币种
  （汇率、储备、扣费、退款）          │
              │                     │   一条 Message 中可出现以下多次 value 转移：
              │                     ├── 入口 Message 携带 value：使用 Message.TransferTokenID
              │                     ├── 普通 CALL 携带 value：沿用当前调用的 token ID
              │                     └── transferMnt 转账：使用参数指定的 token ID
              │                             │
              │                             ▼
              │                   CanTransfer / Transfer
              │                             │
              └──────────────┬──────────────┘
                             ▼
                    StateDB 中的余额变化
               ┌─────────────┴─────────────┐
               ▼                           ▼
        默认 QKC：Balance          非默认币：MntBalances
               └─────────────┬─────────────┘
                             ▼
                 QuarkChain 6 元素账户编码
                             ▼
                      state trie root
```

一条 Message 可以包含入口 value、多个 `CALL` value 和多个 `transferMnt` 转账。对单次 value 转移，其来源只会是其中一种。

账户编码决定状态字节，StateDB 负责余额修改和回滚，EVM 决定本次操作使用哪种代币。任一层与参考实现不一致，state root 都会不同。

### 1. 账户与状态编码

本项目只使用 QuarkChain 的 6 元素 trie 账户定义，不保留并行的 geth 4 元素 `StateAccount` 或兼容 codec：

```go
type StateAccount struct {
    Nonce        uint64
    Balance      *uint256.Int
    Root         common.Hash
    CodeHash     []byte
    MntBalances  *TokenBalances // 非 QKC 代币余额
    FullShardKey uint32
}
```

QKC 余额保留在 `Balance`，`MntBalances` 只保存非默认币的非零余额。写入 trie 时，把 `Balance` 作为 QKC token ID `35760` 合并到余额集合；读取时再拆回 `Balance`。这是 goshard 唯一的账户内存表示和共识编码路径。

余额集合在不超过 16 种代币时，使用与 pyquarkchain 相同的内联格式，并按 token ID 排序。空账户判断也必须包含 MNT：主币为零但仍持有非默认币的账户不能被 EIP-158/EIP-161 清理。

snapshot 仍使用 slim account，但增加 `MntBalances` 和 `FullShardKey`，防止读取时丢失字段。写入 trie 前，再转成 QuarkChain 6 元素格式。

### 2. StateDB 中的多币余额

StateDB 为非默认币提供查询、增加、扣减和设置接口。默认 QKC 不通过这些接口，避免同一份余额同时存在两条修改路径。

需要特别处理两项共识相关状态：

- 单个 token 的零余额会从 map 中删除，但账户的 TokenBalances 字节必须按以下三种情况编码：
  - `MntBalances == nil` 且 QKC `Balance` 为零：编码为 `0x80`，表示账户没有余额集合；
  - `MntBalances` 非 nil 但为空，且 QKC `Balance` 为零：编码为空的 list format，即 `0x8200c0`，保留“余额集合曾被创建、后来清空”的历史状态；
  - QKC 或任一 MNT 余额非零：先把 QKC 以 token ID `35760` 合并进 `MntBalances`，再按正常 list format 编码。

  `0x80` 与 `0x8200c0` 虽然都表示当前没有余额，但字节不同，会生成不同的 state root；解码再编码必须保留原始语义。主网中存在这两种账户，不能统一折叠为空值；
- 账户的 `FullShardKey` 由首次创建它的顶层 Message 确定；后续 Message 和账户重建必须保留该值，不能用当前 Message 的 shard key 覆盖。

### 3. 预编译合约与系统合约

预编译合约负责原生余额操作，系统合约负责 token 管理和 gas 结算规则。

#### 3.1 MNT 预编译合约

五个 MNT 预编译合约的实现与 pyquarkchain / goquarkchain 保持一致：

| 预编译合约 | 地址 | 职责 |
|---|---|---|
| `currentMntID` | `0x000000000000000000000000000000514b430001` | 返回当前 value 转账实际使用的 token ID；合约调用它即明确表示自己知道本次可能收到 MNT，并会按币种处理 |
| `transferMnt` | `0x000000000000000000000000000000514b430002` | 接收目标地址、token ID、金额和可选 calldata，使用指定币种向目标转账，并可继续执行目标合约 |
| `deploySystemContract` | `0x000000000000000000000000000000514b430003` | 根据系统合约编号，将 goquarkchain 的对应合约字节码部署到协议规定的固定地址；重复部署触发地址冲突并回滚状态 |
| `mintMNT` | `0x000000000000000000000000000000514b430004` | 为指定账户增加非默认币余额；仅允许 `NonReservedNativeToken` 系统合约调用，不能铸造 QKC |
| `balanceMNT` | `0x000000000000000000000000000000514b430005` | 接收账户地址和 token ID，返回该账户对应币种的余额 |

MNT 预编译按主网历史分两阶段激活：`currentMntID`、`transferMnt` 和 `deploySystemContract` 使用 `qkc/config.QuarkChainConfig.EnableEvmTimeStamp`，在 `timestamp > enableTime` 时启用；`mintMNT` 和 `balanceMNT` 使用 `EnableNonReservedNativeTokenTimestamp`，同样在严格越过时间戳后启用。激活前，这些地址按普通账户处理，避免历史重放执行 MNT 逻辑。系统合约部署使用 `qkc/config.QuarkChainConfig` 中的对应时间字段，边界为 `timestamp >= enableTime`；RootChainPoSW 的部署时间固定为 0。

#### 3.2 MNT 系统合约

系统合约是部署在固定地址的 Solidity 合约，负责 token 注册、铸币管理和非默认 gas token 的经济结算。goshard 不重新实现这些业务逻辑，而是直接内嵌并部署 goquarkchain 的合约字节码。系统合约通过 `deploySystemContract` 部署。

| 系统合约 | 地址 | 职责 |
|---|---|---|
| `RootChainPoSW` | `0x514b430000000000000000000000000000000001` | 管理 root chain PoSW 质押状态 |
| `NonReservedNativeToken` | `0x514b430000000000000000000000000000000002` | 管理非保留 token ID 的注册、拍卖、所有权和铸币；只有该合约可以调用 `mintMNT` |
| `GeneralNativeToken` | `0x514b430000000000000000000000000000000003` | 管理保留 token，并提供非默认 gas token 所需的汇率、QKC 储备和退款规则 |

非默认 gas token 的 `GasTokenID` 不只是选择扣款余额，还决定汇率、储备、退款和销毁。汇率精度、取整和回滚方式都会影响共识状态。这些结算规则必须通过 `GeneralNativeToken` 实现，并与 goquarkchain 保持一致：

1. 交易校验阶段在 snapshot 中调用系统合约查询汇率和可用储备，随后回滚，保证检查无副作用；
2. buy-gas 阶段由系统合约的 QKC 储备垫付矿工所需主币，并从用户收取折算后的 gas token；
3. 交易结束后按实际 gas 使用量和退款比例返还用户 gas token；
4. 按参考实现处理系统合约余额、剩余储备和需要销毁的部分。

### 4. 转账

MNT 转账包括顶层 Message 转账和合约内部转账，两者使用相同的余额选择规则。

#### 4.1 顶层 Message 转账

`Message.TransferTokenID` 决定顶层 value 使用哪种代币：

- `TransferTokenID` 为 QKC 的正式 ID `35760` 时，转移标准 `Balance`；
- `TransferTokenID` 为其他 token ID（包括 `0`）时，转移对应的 `MntBalances`。QKC 执行路径中的 Message 必须显式设置 token ID，不能把零值解释成“未设置”。

#### 4.2 合约内部转账

普通 EVM `CALL` 没有 token ID 参数，因此内部调用沿用当前 transfer token。要改用其他币种，合约需调用 `transferMnt`，传入接收方、token ID、金额和可选 calldata。

goshard 为**每一次合约调用分别记录 transfer token**，规则如下：

- Message 进入 EVM 的第一次调用使用 `Message.TransferTokenID`；
- 普通内部调用沿用调用方的 token ID；
- `transferMnt` 创建的内部调用使用其参数指定的 token ID 和 Value。

goquarkchain 会临时替换 EVM 的全局 token ID。goshard 的目标设计按调用保存 token ID，内部调用返回或失败时不会影响调用方。

#### 4.3 统一余额选择

两类转账最终都经过带 token ID 的 `CanTransfer` / `Transfer`：只有 `35760` 访问默认 QKC 的 `Balance`，包括 `0` 在内的其他 token ID 访问 `MntBalances`。buy-gas、退款和 value 转账必须使用同一条精确 token ID 规则，否则同一笔交易可能在不同阶段访问不同余额集合。

### 5. 合约接收确认

合约可能根据收到的 value 执行兑换、购买或记账。如果只检查金额、不检查币种，攻击者可以用同数量的低价值 token 换取资产。因此，合约收到非默认币时必须调用 `currentMntID` 读取实际币种，否则回滚。

EOA 不需要确认。合约收到非默认币且 value 非零时需要确认。顶层 Message 直接调用合约时，token ID 来自 `Message.TransferTokenID`；通过 `transferMnt` 转账时，token ID 来自其参数。`currentMntID` 返回本次 value 使用的币种。

## 注意事项

- **合约接收确认**：合约收到非默认代币且 value 非零时，必须通过 `currentMntID` 明确确认本次实际币种，否则回滚该笔转账；确认结果需要在 `DELEGATECALL` / `CALLCODE` 等代理调用中正确传递。
- **buy-gas 的双代币**：transfer token 决定 value 转账使用的币种，gas token 决定 gas 折算、扣费和退款使用的币种，两者可能不同，执行过程中不能混用。

## 提交前实现检查项

- [x] QuarkChain 账户、TokenBalances 和 snapshot/pathdb 基础支持；StateDB 已覆盖余额修改、回滚、复制和空账户判断。
- [x] QKC/MNT 余额选择、`35760` 默认币与 token `0` 普通 MNT 规则、`transferMnt`、`balanceMNT`、合约接收确认及 QKC 转账日志兼容。
- [x] 五个 MNT 预编译、三个系统合约字节码、固定地址部署、global/local-chain-0 scope，以及 `qkc/config.QuarkChainConfig` 时间字段的分阶段激活。
- [ ] 真实 state dump 重建 root 尚不能视为仓库内验收完成：已有 `tools/dump_state` 和 `tools/verify_state`，但没有提交可复现的 26,018 账户输入、预期 root 或自动化结果。应补充固定样本的校验脚本/测试及运行记录后再勾选。
- [x] 从 Message 写入 `FullShardKey`、`GasTokenID` 和 `TransferTokenID`，支持顶层 Message MNT 转账；当前 MNT 范围不修改交易，交易字段转换留给后续实现。
- [ ] transfer token 尚未按调用传递：`transferMnt` 仍临时改写 `evm.TxContext.TransferTokenID`。应把 token ID 放入每个调用 frame/message，并由 `Call`/`CallCode`/`DelegateCall`/`StaticCall` 显式继承或覆盖，不能修改交易级全局上下文。
- [ ] 非默认 gas token 结算未完成：当前仅按原始 gas price 直接扣除 MNT，`returnGas` 又无条件退回 QKC；尚未调用 GeneralNativeToken 的 `calculateGasPrice`/`payAsGas`，也没有实现储备垫付、refund rate、退款和销毁。
- [ ] 测试未补齐：现有测试主要覆盖 MNT state、`transferMnt` 和接收确认；缺 token `0`、三个系统合约、两阶段激活、非默认 gas token 全流程/回滚，以及真实 MNT 区块执行后 state root、receipt、gas used 对拍。

### 未实现项的参考实现与落地方式

1. **统一 token 路由（已完成）。** 参考 pyquarkchain `quarkchain/evm/state.py` 的 `get_balance`/`delta_token_balance`：仅 `DefaultTokenID == 35760` 访问 `StateAccount.Balance`，其余 ID（包括 `0`）访问 `MntBalances`。`CanTransfer`、`Transfer`、buy-gas 和预编译余额访问已统一；完整的非默认 gas token 退款仍归第 5 项。
2. **把 transfer token 下沉到调用 frame。** 参考 `quarkchain/evm/vm.py::Message.transfer_token_id` 和 `quarkchain/evm/specials.py::proc_transfer_mnt`：顶层 frame 从 `core.Message.TransferTokenID` 初始化，普通内部调用继承当前 frame，`transferMnt` 创建带参数 token ID 的新 frame；`currentMntID` 读取当前 frame。接收确认标志也随 CALL 类指令按 pyquarkchain 规则传播。
3. **完整实现系统合约部署（已完成）。** 已按 `quarkchain/evm/specials.py::_system_contracts`、`SYSTEM_CONTRACT_SCOPE_MAP` 和 `proc_deploy_system_contract` 嵌入三份字节码，通过指定目标地址的创建路径部署，并覆盖 index 默认值、scope、enable time 和固定地址测试。
4. **拆分激活条件（已完成）。** `currentMntID`、`transferMnt`、`deploySystemContract` 使用 `qkc/config.QuarkChainConfig.EnableEvmTimeStamp`，`mintMNT`、`balanceMNT` 使用 `EnableNonReservedNativeTokenTimestamp`，均在严格越过配置时间后启用；系统合约部署另用自身配置时间的 `timestamp >= enableTime`，测试覆盖等于及越过边界的行为。
5. **实现非默认 gas token 结算。** 参考 `quarkchain/evm/messages.py::_call_general_native_token_manager`、`get_gas_utility_info`、`pay_native_token_as_gas` 和 `_refund`：校验阶段在 snapshot 中调用 `calculateGasPrice` 后回滚；buy-gas 阶段调用 `payAsGas`，按转换价从用户扣 gas token，并由 GeneralNativeToken 的 QKC 储备垫付；执行结束按 `refund_rate` 退 gas token，剩余部分销毁，同时按实际 gas 给矿工结算 QKC。所有系统合约调用失败、储备不足和交易执行失败路径都要验证回滚。
6. **补齐可复现验收。** 将脱敏或固定的 state dump fixture、预期 state root 和 `tools/verify_state` 命令纳入测试资产；再选取 pyquarkchain 的真实 MNT 区块，导出 pre-state 和 Message 输入，在 goshard 执行后比较 state root、receipt、gas used 与关键余额。由于当前范围止于 Message，不要求接入 QuarkChain transaction 解码。

账户编码属于共识数据，上线需要明确的分叉高度或 regenesis；以后移除 MNT 也需要迁移状态，不能只回退代码。

## 非目标

本次不支持单账户持有超过 16 种代币时使用的独立 balance trie。主网样本尚未发现该场景，因此当前超过上限时显式报错，避免生成与 goquarkchain 不兼容的状态；未来如需支持，必须实现与 goquarkchain SecureTrie 逐字节一致的编码。

## PR 拆分

实现拆成三个有依赖关系的 PR（`mnt-core-types` → `mnt-state` → `mnt-evm`），一起提交 review，并按该顺序合并。

| 顺序 | 分支 / PR | Reviewer 重点 | 主要内容 |
|---|---|---|---|
| 1 | `feature/mnt-core-types` | 编码是否逐字节兼容 | `TokenBalances`、QuarkChain 6 元素账户编解码、携带 MNT 字段的 slim account、`FullShardKey`、兼容向量 |
| 2 | `feature/mnt-state` | 状态生命周期是否完整 | MNT 余额 API、journal、copy、空账户判断、snapshot/pathdb 编码转换、state 对拍工具 |
| 3 | `feature/mnt-evm` | 执行语义是否正确 | 按 token ID 选择余额、接收确认、完整的非默认 gas token 结算、五个预编译、三个系统合约字节码、`qkc/config.QuarkChainConfig` 分阶段激活条件 |

## 测试与验收

测试包括单元测试、与 pyquarkchain 的结果对比和仓库回归检查。

### 1. 单元测试

| 层次 | 重点用例 |
|---|---|
| Types | token 排序和零值删除、账户 RLP 与 slim account 编码/解码、pyquarkchain 已知输入及预期编码、16/17 种代币边界、`FullShardKey` |
| State | MNT 增减、默认币隔离、journal revert、`Copy()` 无别名、MNT 空账户判断、pathdb 转换 |
| EVM | `35760` 默认 QKC 与 token `0` 普通 MNT、按 token ID 选择余额、余额不足、`transferMnt` 转账、`currentMntID` 返回当前 token ID 并设置接收确认、未确认时回滚、代理调用中的确认传递、非默认 gas token 的汇率/储备/退款/回滚、铸币权限、`qkc/config.QuarkChainConfig` 分阶段激活前后的预编译行为 |

### 2. 与 pyquarkchain 结果对比

#### 2.1 状态 root 对比

从 pyquarkchain 导出真实状态 trie，用 `tools/verify_state` 在 goshard 中重建 root，并与 dump 中的 root 比较。该测试覆盖账户解码、TokenBalances 编码、账户排序和 trie 写入。

#### 2.2 区块执行结果对比

选取包含 MNT 交易的真实区块，导出执行前的状态 trie，再用 goshard EVM 执行该区块。比较执行后的 state root、receipt、gas used 和关键账户余额。

执行后的 state root 必须与 pyquarkchain 链上结果一致。当前用例应覆盖账户编码、顶层 Message、合约转账、接收确认，以及非默认 gas token 的折算、扣费、退款和销毁；交易类型接入后的覆盖留给后续 PR。

### 3. 回归检查

每个 PR 先运行相关包的定向测试。提交后，三个 PR 都必须通过 goshard GitHub Actions，包括 build、完整测试、lint、生成代码检查和依赖检查。

因 QuarkChain 账户编码变化而失效的上游测试，应优先更新预期结果或替换为等价测试。确实不再适用于 goshard 的测试可以 `skip`，但必须写明不适用原因。
