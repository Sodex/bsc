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

// DexLogs — single WebSocket/IPC subscription for V2/V3/V4/Obric DEX events
// and Obric oracle updates with per-block boundary notifications.
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
//	  }
//	}]}
//
// Stream per block:
//
//	{"type":"log",           "log":{...}}
//	{"type":"oracleUpdate",  "pools":[...], "blockNumber":"0x...", ...}
//	{"type":"blockBoundary", "blockNumber":"0x...", "blockHash":"0x...", "logsCount":N}

import (
	"context"
	"errors"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/gopool"
	"github.com/ethereum/go-ethereum/common/hexutil"
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

type DexFilterCriteria struct {
	V2V3        V2V3DirectCriteria  `json:"v2v3"`
	Obric       V2V3DirectCriteria  `json:"obric"`
	V4          []V4ManagerCriteria `json:"v4"`
	ObricOracle *ObricOracleConfig  `json:"obricOracle"`
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

type dexNotification struct {
	Type             string             `json:"type"`                       // "log" | "oracleUpdate" | "blockBoundary"
	Pools            []OraclePoolUpdate `json:"pools,omitempty"`            // set for type="oracleUpdate"
	BlockNumber      hexutil.Uint64     `json:"blockNumber,omitempty"`      // set for type="oracleUpdate"|"blockBoundary"
	TransactionHash  *common.Hash       `json:"transactionHash,omitempty"`  // set for type="oracleUpdate"
	TransactionIndex *hexutil.Uint      `json:"transactionIndex,omitempty"` // set for type="oracleUpdate"
	BlockHash        *common.Hash       `json:"blockHash,omitempty"`        // set for type="oracleUpdate"|"blockBoundary"
	BlockTimestamp   *hexutil.Uint64    `json:"blockTimestamp,omitempty"`   // set for type="oracleUpdate"
	LogIndex         *hexutil.Uint      `json:"logIndex,omitempty"`         // set for type="oracleUpdate"
	Removed          *bool              `json:"removed,omitempty"`          // set for type="oracleUpdate"
	Log              *types.Log         `json:"log,omitempty"`              // set for type="log"
	LogsCount        int                `json:"logsCount,omitempty"`        // set for type="blockBoundary"
}

// ---------------------------------------------------------------------------
// Validation errors
// ---------------------------------------------------------------------------

var (
	errDexNoAddresses    = errors.New("dexLogs: at least one v2v3 address, obric address, v4 poolManager, or obricOracle is required")
	errDexTooManyPoolIds = errors.New("dexLogs: v4.poolIds exceeds LogQueryLimit")
)

// obricOracleEventTopic is the keccak256 signature of the oracle update event
// emitted by the Obric oracle registry contract.
var obricOracleEventTopic = common.HexToHash("0x3166394b161ec8a4df959a00f9ba094ecd71070e9b9e9f3b28c3898ce9114629")

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
// and emits an "oracleUpdate" notification instead of a plain "log".
func (api *FilterAPI) DexLogs(ctx context.Context, crit DexFilterCriteria) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	// --- Validation --------------------------------------------------------

	if len(crit.V2V3.Addresses) == 0 && len(crit.Obric.Addresses) == 0 && len(crit.V4) == 0 && crit.ObricOracle == nil {
		return nil, errDexNoAddresses
	}

	// --- Build FilterQuery -------------------------------------------------

	var (
		allAddresses  []common.Address
		addressTopics = make(map[common.Address][][]common.Hash)
		globalTopics0 = make(map[common.Hash]struct{})
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

	matchedLogs := make(chan []*types.Log, 32)
	logsSub, err := api.events.SubscribeLogs(query, matchedLogs)
	if err != nil {
		return nil, err
	}

	rpcSub := notifier.CreateSubscription()

	// --- Delivery goroutine ------------------------------------------------

	gopool.Submit(func() {
		defer logsSub.Unsubscribe()

		for {
			select {
			case logs := <-matchedLogs:
				if len(logs) == 0 {
					continue
				}

				// Logs within a batch are guaranteed by geth to belong to the
				// same block and are already sorted by (TxIndex, LogIndex).
				for _, l := range logs {
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
								Type:             "oracleUpdate",
								Pools:            pools,
								BlockNumber:      hexutil.Uint64(l.BlockNumber),
								TransactionHash:  &txHash,
								TransactionIndex: &txIdx,
								BlockHash:        &bh,
								BlockTimestamp:   &ts,
								LogIndex:         &logIdx,
								Removed:          &l.Removed,
							})
							continue
						}
						// enrichment failed — fall through to plain log delivery
					}

					notifier.Notify(rpcSub.ID, dexNotification{ //nolint:errcheck
						Type: "log",
						Log:  l,
					})
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
