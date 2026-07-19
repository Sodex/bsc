// Copyright 2025 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package filters

// DexLogs — single WebSocket/IPC subscription for V2/V3/V4/Obric/Tessera/ListaStable
// DEX events and Obric oracle updates with per-block boundary notifications.
//
// JSON-RPC:
//
//	{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["dexLogs",{
//	  "v2v3": {
//	    "addresses": ["0xV2Pool1...", "0xV3Pool2..."],
//	    "topics": ["0xSyncTopic...", "0xSwapTopicV3..."]
//	  },
//	  "obric": {
//	    "addresses": ["0xObricPool1...", "0xObricPool2..."],
//	    "topics": ["0xObricStateEvent..."]
//	  },
//	  "listaStable": {
//	    "addresses": ["0xListaPool1...", "0xListaPool2..."],
//	    "topics": ["0xTokenExchange...", "0xAddLiquidity...", "...остальные 12 lista-топиков + Upgraded"]
//	  },
//	  "v4": [
//	    {
//	      "address": "0xUniV4PoolManager...",
//	      "topics": ["0xSwapV4Topic...", "0xModifyLiqV4Topic..."],
//	      "poolIds": ["0xPoolId1...", "0xPoolId2..."]
//	    }
//	  ],
//	  "obricOracle": {
//	    "address": "0xOracleContract...",
//	    "poolIds": [0, 1]
//	  },
//	  "tessera": {
//	    "addresses": ["0xTesseraPool1...", "0xTesseraPool2..."],
//	    "swapTopic": "0xTesseraSwapTopic..."
//	  }
//	}]}
//
// Criteria groups semantics:
//   - v2v3 / obric / listaStable — plain (addresses × topics) matching; every
//     matched log is delivered untouched as {"type":"log"} and the consumer
//     dispatches by topic[0]. ListaStable emits ONLY real logs (no synthetics,
//     no eth_call enrichment); its topics list must include the EIP-1967
//     Upgraded topic explicitly (unlike tessera, where the node hardcodes it).
//   - tessera — real logs (Swap + Upgraded) via the log path, PLUS a per-block
//     transaction scan producing the synthetic tessera* events below.
//   - obricOracle — matched oracle logs are enriched in-process (eth_call) into
//     "obricOracleUpdate".
//
// Tessera emits no log on a price update — the price is pushed by a plain ~22-byte
// transaction to the pool — so when "tessera" is present the subscription switches
// to a per-block (ChainEvent) clock: it re-matches the real logs from the block
// receipts AND scans the block's transactions for the synthetic Tessera events,
// emitting the union in (txIndex, logIndex) order. Without "tessera" the behaviour
// is unchanged (log-driven). Every synthetic event carries (blockNumber, txIndex,
// logIndex) in its header, like a real log, so it can be ordered within the block.
//
// Stream per block (closed by blockBoundary; no boundary is sent for empty blocks).
// obricOracleUpdate and every tessera* share ONE layout: type, payload, then the
// common header [blockNumber, transactionHash, transactionIndex, blockHash,
// blockTimestamp, logIndex, removed].
//
//	{"type":"log",                "log":{...}}   // incl. Tessera Swap/Upgraded and ALL ListaStable events — dispatch by topic[0]
//	{"type":"obricOracleUpdate",  "pools":[...],   <common header>}
//	{"type":"tesseraPriceUpdate", "tessera":{"pool","oraclePrice","blockOfPriceUpdate","seq"}, <common header>}
//	{"type":"tesseraConfigUpdate","tessera":{"pool","selector","calldata"}, <common header>}   // setConfig / trading on-off (raw calldata)
//	{"type":"tesseraLadderUpdate","tessera":{"pool","selector":"0xdba310cf","calldata"}, <common header>}
//	{"type":"blockBoundary",      "blockNumber":"0x...", "blockHash":"0x...", "logsCount":N}

import (
	"context"
	"errors"
	"math/big"
	"sort"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/gopool"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

type V2V3DirectCriteria struct {
	Addresses []common.Address `json:"addresses"`
	Topics    []common.Hash    `json:"topics"`
}

type V4ManagerCriteria struct {
	Address common.Address `json:"address"`
	Topics  []common.Hash  `json:"topics"`
	PoolIds []common.Hash  `json:"poolIds"`
}

// ObricOracleConfig specifies the oracle contract to watch and which pool
// indices should be enriched with price_raw via in-process eth_call.
type ObricOracleConfig struct {
	Address common.Address `json:"address"`
	PoolIds []uint32       `json:"poolIds"`
}

// TesseraCriteria specifies the Tessera pools to watch. Tessera does NOT emit a
// log on a price update: the price is pushed by a plain transaction (~22 bytes of
// calldata) to the pool. So the subscription needs both the pool addresses (to
// match Swap/Upgraded logs through the normal log path) and to scan each block's
// transactions for the synthetic events (price-push, config, ladder).
type TesseraCriteria struct {
	Addresses []common.Address `json:"addresses"`
	SwapTopic common.Hash      `json:"swapTopic"`
}

type DexFilterCriteria struct {
	V2V3        V2V3DirectCriteria  `json:"v2v3"`
	Obric       V2V3DirectCriteria  `json:"obric"`
	ListaStable V2V3DirectCriteria  `json:"listaStable"` // Lista StableSwap: только реальные логи, механика как v2v3/obric
	V4          []V4ManagerCriteria `json:"v4"`
	ObricOracle *ObricOracleConfig  `json:"obricOracle"`
	Tessera     *TesseraCriteria    `json:"tessera"`
}

// ---------------------------------------------------------------------------
// Notification types sent to the subscriber
// ---------------------------------------------------------------------------

// OraclePoolUpdate carries the enriched data for a single pool from an oracle
// update event: k_100 from the event data and price_raw from eth_call.
type OraclePoolUpdate struct {
	PoolId   uint32       `json:"poolId"`
	K100     hexutil.Big  `json:"k100"`
	PriceRaw hexutil.Uint `json:"priceRaw"` // uint32: bits[23:0]=price, bits[31:24]=depth
}

// TesseraSynthetic is the payload of a synthetic Tessera event decoded from a
// transaction (the node "manufactures" it because Tessera emits no log for it).
// The (block, txIndex, logIndex) ordering lives in the dexNotification header,
// just like a real log, so the consumer can interleave it in block order.
//
//   - price-push  -> OraclePrice + BlockOfPriceUpdate + Seq are set
//   - config/ladder/trading -> Selector + Calldata are set (decoded by the bot)
type TesseraSynthetic struct {
	Pool               common.Address  `json:"pool"`
	OraclePrice        *hexutil.Big    `json:"oraclePrice,omitempty"`
	BlockOfPriceUpdate *hexutil.Uint64 `json:"blockOfPriceUpdate,omitempty"`
	Seq                *hexutil.Uint   `json:"seq,omitempty"`
	Selector           *hexutil.Bytes  `json:"selector,omitempty"`
	Calldata           *hexutil.Bytes  `json:"calldata,omitempty"`
}

// Field order is significant: the consumer decodes positionally. obricOracleUpdate
// and every tessera* event share one layout — payload (pools|tessera) right after
// "type", then the common header (blockNumber, transactionHash, transactionIndex,
// blockHash, blockTimestamp, logIndex, removed) — so the consumer reads the header
// with a single shared routine. (A plain "log" carries the same header fields inside
// its log object; "blockBoundary" carries only blockNumber/blockHash/logsCount.)
type dexNotification struct {
	Type             string             `json:"type"`                       // "log" | "obricOracleUpdate" | "blockBoundary" | "tesseraPriceUpdate" | "tesseraConfigUpdate" | "tesseraLadderUpdate"
	Pools            []OraclePoolUpdate `json:"pools,omitempty"`            // payload for type="obricOracleUpdate"
	Tessera          *TesseraSynthetic  `json:"tessera,omitempty"`          // payload for type="tessera*"
	BlockNumber      hexutil.Uint64     `json:"blockNumber,omitempty"`      // set for type="obricOracleUpdate"|"blockBoundary"|"tessera*"
	TransactionHash  *common.Hash       `json:"transactionHash,omitempty"`  // set for type="obricOracleUpdate"|"tessera*"
	TransactionIndex *hexutil.Uint      `json:"transactionIndex,omitempty"` // set for type="obricOracleUpdate"|"tessera*"
	BlockHash        *common.Hash       `json:"blockHash,omitempty"`        // set for type="obricOracleUpdate"|"blockBoundary"|"tessera*"
	BlockTimestamp   *hexutil.Uint64    `json:"blockTimestamp,omitempty"`   // set for type="obricOracleUpdate"|"tessera*"
	LogIndex         *hexutil.Uint      `json:"logIndex,omitempty"`         // set for type="obricOracleUpdate"|"tessera*"
	Removed          *bool              `json:"removed,omitempty"`          // set for type="obricOracleUpdate"|"tessera*"
	Log              *types.Log         `json:"log,omitempty"`              // set for type="log"
	LogsCount        int                `json:"logsCount,omitempty"`        // set for type="blockBoundary"
}

// ---------------------------------------------------------------------------
// Validation errors
// ---------------------------------------------------------------------------

var (
	errDexNoAddresses    = errors.New("dexLogs: at least one v2v3/obric/listaStable address, v4 poolManager, tessera address, or obricOracle is required")
	errDexTooManyPoolIds = errors.New("dexLogs: v4.poolIds exceeds LogQueryLimit")
)

// obricOracleEventTopic is the keccak256 signature of the oracle update event
// emitted by the Obric oracle registry contract.
var obricOracleEventTopic = common.HexToHash("0x3166394b161ec8a4df959a00f9ba094ecd71070e9b9e9f3b28c3898ce9114629")

// tesseraUpgradedTopic is the EIP-1967 Upgraded(address) event emitted by the
// Tessera pool proxy when its implementation changes. We watch it (through the
// normal log path) to trigger a full re-bootstrap of the pool on the bot side.
var tesseraUpgradedTopic = common.HexToHash("0xbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b")

// tesseraPriceUpdateCalldataLen is the calldata size of a Tessera price-push: the
// pool proxy writes the top 176 bits of slot0 ([seq:1][block_of_price_update:5][oracle_price:16]).
const tesseraPriceUpdateCalldataLen = 22

// Tessera pool state-changing function selectors (verified against the decompiled
// pool_implementation.sol). config/ladder/trading calldata is forwarded raw to the
// bot, which decodes it; price-push has no selector (classified by calldata length).
var (
	tesseraSelSetConfig     = [4]byte{0x75, 0xe8, 0x42, 0x2f} // setConfig
	tesseraSelUpdateLadder  = [4]byte{0xdb, 0xa3, 0x10, 0xcf} // updateLadder (resets demand=0)
	tesseraSelDisableTrade  = [4]byte{0x17, 0x70, 0x0f, 0x01} // disableTrading
	tesseraSelEnableTrade   = [4]byte{0x8a, 0x8c, 0x52, 0x3c} // enableTrading
	tesseraSelOperatorDline = [4]byte{0x16, 0x67, 0xd8, 0x75} // operator kill-switch / deadline (slot 0x32)
)

// ---------------------------------------------------------------------------
// DexLogs — RPC entry point
// ---------------------------------------------------------------------------

// DexLogs creates a subscription that delivers V2/V3/V4/Obric DEX events and
// Obric oracle updates in block-ordered sequence, followed by a synthetic
// "blockBoundary" notification after all logs of each block have been sent.
//
// Internally it builds a single FilterQuery with:
//   - Addresses = v2v3 pairs + obric pools + v4 PoolManagers + obric oracle
//   - Topics[0] = union of all relevant event signatures
//   - Topics[1] = nil  (no global constraint on second topic)
//   - AddressTopics = per-address override enforcing per-address topic rules
//
// When an oracle log is matched and a ContractCaller backend is available, the
// delivery goroutine enriches it with price_raw from an in-process eth_call
// and emits an "obricOracleUpdate" notification instead of a plain "log".
func (api *FilterAPI) DexLogs(ctx context.Context, crit DexFilterCriteria) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	// --- Validation --------------------------------------------------------

	tesseraEnabled := crit.Tessera != nil && len(crit.Tessera.Addresses) > 0
	if len(crit.V2V3.Addresses) == 0 && len(crit.Obric.Addresses) == 0 && len(crit.ListaStable.Addresses) == 0 &&
		len(crit.V4) == 0 && crit.ObricOracle == nil && !tesseraEnabled {
		return nil, errDexNoAddresses
	}

	// --- Build FilterQuery -------------------------------------------------

	var (
		allAddresses  []common.Address
		addressTopics = make(map[common.Address][][]common.Hash)
		globalTopics0 = make(map[common.Hash]struct{})
		tesseraPools  map[common.Address]struct{} // populated only when tesseraEnabled
	)

	// 1. V2/V3 pools
	if len(crit.V2V3.Addresses) > 0 {
		allAddresses = append(allAddresses, crit.V2V3.Addresses...)
		v2v3Rule := [][]common.Hash{crit.V2V3.Topics}
		for _, addr := range crit.V2V3.Addresses {
			addressTopics[addr] = v2v3Rule
		}
		for _, t := range crit.V2V3.Topics {
			globalTopics0[t] = struct{}{}
		}
	}

	// 2. Obric pools (same mechanics as V2/V3: address + topic0 filter)
	if len(crit.Obric.Addresses) > 0 {
		allAddresses = append(allAddresses, crit.Obric.Addresses...)
		obricRule := [][]common.Hash{crit.Obric.Topics}
		for _, addr := range crit.Obric.Addresses {
			addressTopics[addr] = obricRule
		}
		for _, t := range crit.Obric.Topics {
			globalTopics0[t] = struct{}{}
		}
	}

	// 2a. Lista StableSwap pools — только реальные логи, механика идентична
	// v2v3/obric: (адреса × топики) через addressTopics. Топик Upgraded клиент
	// передаёт в списке сам (нода ничего не хардкодит).
	if len(crit.ListaStable.Addresses) > 0 {
		allAddresses = append(allAddresses, crit.ListaStable.Addresses...)
		listaRule := [][]common.Hash{crit.ListaStable.Topics}
		for _, addr := range crit.ListaStable.Addresses {
			addressTopics[addr] = listaRule
		}
		for _, t := range crit.ListaStable.Topics {
			globalTopics0[t] = struct{}{}
		}
	}

	// 2b. Tessera pools. Real logs (Swap + Upgraded) flow through the normal log
	// path; the synthetic price-push/config/ladder events come from the per-block
	// transaction scan (added in the delivery path). Matching both topics on the
	// pool address lets Swap and Upgraded(address) reach the subscriber.
	if tesseraEnabled {
		allAddresses = append(allAddresses, crit.Tessera.Addresses...)
		tesseraRule := [][]common.Hash{{crit.Tessera.SwapTopic, tesseraUpgradedTopic}}
		tesseraPools = make(map[common.Address]struct{}, len(crit.Tessera.Addresses))
		for _, addr := range crit.Tessera.Addresses {
			addressTopics[addr] = tesseraRule
			tesseraPools[addr] = struct{}{}
		}
		globalTopics0[crit.Tessera.SwapTopic] = struct{}{}
		globalTopics0[tesseraUpgradedTopic] = struct{}{}
	}

	// 3. V4 managers (topics[0] = events, topics[1] = poolIds)
	for _, mgr := range crit.V4 {
		allAddresses = append(allAddresses, mgr.Address)
		v4Rule := [][]common.Hash{mgr.Topics, mgr.PoolIds}
		addressTopics[mgr.Address] = v4Rule
		for _, t := range mgr.Topics {
			globalTopics0[t] = struct{}{}
		}
	}

	// 4. Obric oracle
	oracle := crit.ObricOracle
	var contractCaller ContractCaller
	if oracle != nil {
		if cc, ok := api.sys.backend.(ContractCaller); ok {
			contractCaller = cc
		}
		allAddresses = append(allAddresses, oracle.Address)
		addressTopics[oracle.Address] = [][]common.Hash{{obricOracleEventTopic}}
		globalTopics0[obricOracleEventTopic] = struct{}{}
	}

	uniqueTopics0 := make([]common.Hash, 0, len(globalTopics0))
	for t := range globalTopics0 {
		uniqueTopics0 = append(uniqueTopics0, t)
	}

	query := ethereum.FilterQuery{
		Addresses:     allAddresses,
		Topics:        [][]common.Hash{uniqueTopics0},
		AddressTopics: addressTopics,
	}

	// --- Subscribe ---------------------------------------------------------

	rpcSub := notifier.CreateSubscription()

	// emitOneLog turns a matched log into the right notification: an oracle log is
	// enriched into "obricOracleUpdate", everything else (including a Tessera Swap or
	// Upgraded log) is a plain "log" the consumer dispatches by topic[0]. Shared by
	// both delivery paths so their per-log behaviour is identical.
	emitOneLog := func(l *types.Log) {
		if oracle != nil &&
			l.Address == oracle.Address &&
			len(l.Topics) > 0 && l.Topics[0] == obricOracleEventTopic &&
			contractCaller != nil {

			pools, err := enrichOracleLog(context.Background(), contractCaller, oracle, l)
			if err == nil {
				txHash := l.TxHash
				bh := l.BlockHash
				txIdx := hexutil.Uint(l.TxIndex)
				logIdx := hexutil.Uint(l.Index)
				ts := hexutil.Uint64(l.BlockTimestamp)
				notifier.Notify(rpcSub.ID, dexNotification{ //nolint:errcheck
					Type:             "obricOracleUpdate",
					Pools:            pools,
					BlockNumber:      hexutil.Uint64(l.BlockNumber),
					TransactionHash:  &txHash,
					TransactionIndex: &txIdx,
					BlockHash:        &bh,
					BlockTimestamp:   &ts,
					LogIndex:         &logIdx,
					Removed:          &l.Removed,
				})
				return
			}
			// enrichment failed — fall through to plain log delivery
		}

		notifier.Notify(rpcSub.ID, dexNotification{ //nolint:errcheck
			Type: "log",
			Log:  l,
		})
	}

	// --- Path A: no Tessera -> existing log-driven delivery (unchanged behaviour) ---
	if !tesseraEnabled {
		matchedLogs := make(chan []*types.Log, 32)
		logsSub, err := api.events.SubscribeLogs(query, matchedLogs)
		if err != nil {
			return nil, err
		}

		gopool.Submit(func() {
			defer logsSub.Unsubscribe()

			for {
				select {
				case logs := <-matchedLogs:
					if len(logs) == 0 {
						continue
					}
					// Logs within a batch belong to the same block, already
					// sorted by (TxIndex, LogIndex).
					for _, l := range logs {
						emitOneLog(l)
					}
					first := logs[0]
					bh := first.BlockHash
					notifier.Notify(rpcSub.ID, dexNotification{ //nolint:errcheck
						Type:        "blockBoundary",
						BlockNumber: hexutil.Uint64(first.BlockNumber),
						BlockHash:   &bh,
						LogsCount:   len(logs),
					})

				case <-rpcSub.Err():
					return
				}
			}
		})

		return rpcSub, nil
	}

	// --- Path B: Tessera present -> ChainEvent is the per-block clock ---
	// Each block we (a) re-match the real logs from receipts with the SAME matcher
	// the EventSystem uses, (b) scan transactions for the synthetic Tessera events,
	// then emit everything in (txIndex, logIndex) order, closed by one blockBoundary.
	chainCh := make(chan core.ChainEvent, 32)
	chainSub := api.sys.backend.SubscribeChainEvent(chainCh)
	// Reorg parity with the log path: forward matched removed logs (Removed==true)
	// so the consumer detects the reorg and restarts, exactly as in Path A.
	removedCh := make(chan core.RemovedLogsEvent, 32)
	removedSub := api.sys.backend.SubscribeRemovedLogsEvent(removedCh)

	gopool.Submit(func() {
		defer chainSub.Unsubscribe()
		defer removedSub.Unsubscribe()

		for {
			select {
			case ev := <-chainCh:
				deliverTesseraBlock(ev, query, tesseraPools, emitOneLog, notifier, rpcSub)
			case rev := <-removedCh:
				for _, l := range filterLogs(rev.Logs, nil, nil, query.Addresses, query.Topics, query.AddressTopics) {
					notifier.Notify(rpcSub.ID, dexNotification{ //nolint:errcheck
						Type: "log",
						Log:  l,
					})
				}
			case <-rpcSub.Err():
				return
			}
		}
	})

	return rpcSub, nil
}

// deliverTesseraBlock processes one canonical block for a Tessera-enabled
// subscription: it re-matches the real logs from the block receipts, scans the
// transactions for synthetic Tessera events (price-push / config / ladder), emits
// the union in (txIndex, logIndex) order, and finally a single blockBoundary whose
// logsCount equals the total number of emitted events. No boundary is sent for a
// block that yields no events (kept for backward compatibility).
func deliverTesseraBlock(
	ev core.ChainEvent,
	query ethereum.FilterQuery,
	tesseraPools map[common.Address]struct{},
	emitOneLog func(*types.Log),
	notifier *rpc.Notifier,
	rpcSub *rpc.Subscription,
) {
	// 1. All logs of the block, taken from the receipts (already ordered by
	//    (txIndex, Index)), then matched with the SAME predicate the EventSystem uses.
	var blockLogs []*types.Log
	for _, r := range ev.Receipts {
		blockLogs = append(blockLogs, r.Logs...)
	}
	matched := filterLogs(blockLogs, nil, nil, query.Addresses, query.Topics, query.AddressTopics)

	// 2. Scan transactions for the synthetic Tessera events.
	synth := scanTesseraSynthetics(ev, tesseraPools)

	if len(matched) == 0 && len(synth) == 0 {
		return // empty block -> no boundary (backward compatible)
	}

	// 3. Emit real logs + synthetic events in (txIndex, logIndex) order.
	for _, it := range orderBlockItems(matched, synth) {
		if it.log != nil {
			emitOneLog(it.log)
		} else {
			notifier.Notify(rpcSub.ID, *it.note) //nolint:errcheck
		}
	}

	// 4. Single blockBoundary with the COMBINED event count (real + synthetic).
	bh := ev.Header.Hash()
	notifier.Notify(rpcSub.ID, dexNotification{ //nolint:errcheck
		Type:        "blockBoundary",
		BlockNumber: hexutil.Uint64(ev.Header.Number.Uint64()),
		BlockHash:   &bh,
		LogsCount:   len(matched) + len(synth),
	})
}

// scanTesseraSynthetics walks the block transactions and returns a synthetic
// notification for each successful state-changing call to a watched Tessera pool.
// A synthetic event takes the global log-index slot of its transaction (logs before
// it + that tx's own logs), so it sorts at the transaction's position in the block.
func scanTesseraSynthetics(ev core.ChainEvent, tesseraPools map[common.Address]struct{}) []dexNotification {
	blockNum := ev.Header.Number.Uint64()
	blockTime := ev.Header.Time
	blockHash := ev.Header.Hash()
	var synth []dexNotification
	logsSoFar := uint(0)
	for i, tx := range ev.Transactions {
		txLogs := 0
		if i < len(ev.Receipts) {
			txLogs = len(ev.Receipts[i].Logs)
		}
		synLogIdx := logsSoFar + uint(txLogs)
		logsSoFar += uint(txLogs)

		to := tx.To()
		if to == nil {
			continue
		}
		if _, ok := tesseraPools[*to]; !ok {
			continue
		}
		if i >= len(ev.Receipts) || ev.Receipts[i].Status != types.ReceiptStatusSuccessful {
			continue
		}
		if note, ok := buildTesseraSynthetic(tx, uint(i), synLogIdx, blockNum, blockTime, blockHash); ok {
			synth = append(synth, note)
		}
	}
	return synth
}

// blockItem is one emittable element of a block: exactly one of log/note is set.
type blockItem struct {
	txIndex, logIndex uint
	log               *types.Log
	note              *dexNotification
}

// orderBlockItems merges real matched logs and synthetic notifications into a
// single stream sorted by (txIndex, logIndex) — the order they appear in the block.
func orderBlockItems(matched []*types.Log, synth []dexNotification) []blockItem {
	items := make([]blockItem, 0, len(matched)+len(synth))
	for _, l := range matched {
		items = append(items, blockItem{uint(l.TxIndex), uint(l.Index), l, nil})
	}
	for k := range synth {
		n := &synth[k]
		items = append(items, blockItem{uint(*n.TransactionIndex), uint(*n.LogIndex), nil, n})
	}
	sort.SliceStable(items, func(a, b int) bool {
		if items[a].txIndex != items[b].txIndex {
			return items[a].txIndex < items[b].txIndex
		}
		return items[a].logIndex < items[b].logIndex
	})
	return items
}

// buildTesseraSynthetic classifies a transaction sent to a Tessera pool and builds
// the corresponding synthetic notification, or returns ok=false if the transaction
// is not a recognised state change. The (txIndex, logIndex) are carried in the
// header so the consumer can order it among the real logs.
func buildTesseraSynthetic(tx *types.Transaction, txIndex, logIndex uint, blockNum, blockTime uint64, blockHash common.Hash) (dexNotification, bool) {
	data := tx.Data()
	pool := *tx.To()
	txHash := tx.Hash()
	txIdx := hexutil.Uint(txIndex)
	logIdx := hexutil.Uint(logIndex)
	ts := hexutil.Uint64(blockTime)
	removed := false // synthetic events come from canonical-block txs

	note := dexNotification{
		BlockNumber:      hexutil.Uint64(blockNum),
		TransactionHash:  &txHash,
		TransactionIndex: &txIdx,
		BlockHash:        &blockHash,
		BlockTimestamp:   &ts,
		LogIndex:         &logIdx,
		Removed:          &removed,
	}

	// price-push: raw 22-byte calldata = [seq:1][block_of_price_update:5][oracle_price:16].
	if len(data) == tesseraPriceUpdateCalldataLen {
		seq := hexutil.Uint(data[0])
		var blk uint64
		for _, b := range data[1:6] {
			blk = blk<<8 | uint64(b)
		}
		bpu := hexutil.Uint64(blk)
		oraclePrice := (*hexutil.Big)(new(big.Int).SetBytes(data[6:22]))
		note.Type = "tesseraPriceUpdate"
		note.Tessera = &TesseraSynthetic{
			Pool:               pool,
			OraclePrice:        oraclePrice,
			BlockOfPriceUpdate: &bpu,
			Seq:                &seq,
		}
		return note, true
	}

	// config / ladder / trading: forward the raw calldata; the bot decodes by selector.
	if len(data) >= 4 {
		var sel [4]byte
		copy(sel[:], data[:4])
		switch sel {
		case tesseraSelUpdateLadder:
			note.Type = "tesseraLadderUpdate"
		case tesseraSelSetConfig, tesseraSelDisableTrade, tesseraSelEnableTrade, tesseraSelOperatorDline:
			note.Type = "tesseraConfigUpdate"
		default:
			return dexNotification{}, false
		}
		selBytes := hexutil.Bytes(append([]byte(nil), data[:4]...))
		calldata := hexutil.Bytes(append([]byte(nil), data...))
		note.Tessera = &TesseraSynthetic{
			Pool:     pool,
			Selector: &selBytes,
			Calldata: &calldata,
		}
		return note, true
	}

	return dexNotification{}, false
}
