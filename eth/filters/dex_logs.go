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

// DexLogs — single WebSocket/IPC subscription for V2/V3/V4 DEX events with
// per-block boundary notifications.
//
//
// JSON-RPC:
//
//  {"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["dexLogs",{
//    "v2v3": {
//      "addresses": ["0xV2Pool1...", "0xV3Pool2..."],
//      "topics": ["0xSyncTopic...", "0xSwapTopicV3..."]
//    },
//    "v4": [
//      {
//        "address": "0xUniV4PoolManager...",
//        "topics": ["0xSwapV4Topic...", "0xModifyLiqV4Topic..."],
//        "poolIds": ["0xPoolId1...", "0xPoolId2..."]
//      },
//      {
//        "address": "0xPancakeV4PoolManager...",
//        "topics": ["0xSwapV4Topic...", "0xModifyLiqV4Topic..."],
//        "poolIds": ["0xPoolId3..."]
//      }
//    ]
//  }]}
//
// Stream per block:
//
//	{"type":"log",           "log":{...}}
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
	Topics    []common.Hash    `json:"topics"` // Теперь передаем события извне
}

type V4ManagerCriteria struct {
	Address common.Address `json:"address"`
	Topics  []common.Hash  `json:"topics"`  // События для конкретного менеджера
	PoolIds []common.Hash  `json:"poolIds"` // PoolId для конкретного менеджера
}

type DexFilterCriteria struct {
	V2V3 V2V3DirectCriteria  `json:"v2v3"`
	V4   []V4ManagerCriteria `json:"v4"` // Теперь это массив объектов
}

// ---------------------------------------------------------------------------
// Notification type sent to the subscriber
// ---------------------------------------------------------------------------

type dexNotification struct {
	Type        string         `json:"type"`                  // "log" | "blockBoundary"
	Log         *types.Log     `json:"log,omitempty"`         // set for type="log"
	BlockNumber hexutil.Uint64 `json:"blockNumber,omitempty"` // set for type="blockBoundary"
	BlockHash   *common.Hash   `json:"blockHash,omitempty"`   // set for type="blockBoundary"
	LogsCount   int            `json:"logsCount,omitempty"`   // set for type="blockBoundary"
}

// ---------------------------------------------------------------------------
// Validation errors
// ---------------------------------------------------------------------------

var (
	errDexNoAddresses    = errors.New("dexLogs: at least one v2v3 address or v4 poolManager is required")
	errDexTooManyPoolIds = errors.New("dexLogs: v4.poolIds exceeds LogQueryLimit")
)

// ---------------------------------------------------------------------------
// DexLogs — RPC entry point
// ---------------------------------------------------------------------------

// DexLogs creates a subscription that delivers V2/V3/V4 DEX events in
// block-ordered sequence, followed by a synthetic "blockBoundary" notification
// after all logs of each block have been sent.
//
// Internally it builds a single FilterQuery with:
//   - Addresses = v2v3 pairs + v4 PoolManagers
//   - Topics[0] = union of all relevant event signatures
//   - Topics[1] = nil  (no global constraint on second topic)
//   - AddressTopics = per-PoolManager override enforcing topics[1] ∈ poolIds
//
// The per-address topic override (AddressTopics) is applied inside filterLogs,
// which runs in the event system goroutine — the earliest possible point for
// live subscriptions, before logs reach this goroutine.
func (api *FilterAPI) DexLogs(ctx context.Context, crit DexFilterCriteria) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	// --- Validation --------------------------------------------------------

	if len(crit.V2V3.Addresses) == 0 && len(crit.V4) == 0 {
		return nil, errDexNoAddresses
	}

	// --- Build FilterQuery -------------------------------------------------

	var (
		allAddresses  []common.Address
		addressTopics = make(map[common.Address][][]common.Hash)
		globalTopics0 = make(map[common.Hash]struct{}) // Собираем уникальные Topic0 для оптимизации
	)

	// 1. Обрабатываем V2/V3
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

	// 2. Обрабатываем V4 (каждый менеджер индивидуально)
	for _, mgr := range crit.V4 {
		allAddresses = append(allAddresses, mgr.Address)

		// Настраиваем: topics[0] = события, topics[1] = PoolId
		v4Rule := [][]common.Hash{
			mgr.Topics,
			mgr.PoolIds,
		}
		addressTopics[mgr.Address] = v4Rule

		for _, t := range mgr.Topics {
			globalTopics0[t] = struct{}{}
		}
	}

	// Превращаем карту уникальных Topic0 обратно в слайс
	uniqueTopics0 := make([]common.Hash, 0, len(globalTopics0))
	for t := range globalTopics0 {
		uniqueTopics0 = append(uniqueTopics0, t)
	}

	query := ethereum.FilterQuery{
		Addresses:     allAddresses,
		Topics:        [][]common.Hash{uniqueTopics0}, // Глобальный фильтр по всем типам событий
		AddressTopics: addressTopics,                  // Точечный фильтр по контрактам и PoolId
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
