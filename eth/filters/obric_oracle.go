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

import (
	"context"
	"encoding/binary"
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
)

// enrichOracleLog decodes the k_100 array from the oracle event data, then for
// each configured poolId calls oracle.0xf478fdee(poolId) at the event's block
// to fetch price_raw and returns the combined OraclePoolUpdate slice.
func enrichOracleLog(
	ctx context.Context,
	caller ContractCaller,
	cfg *ObricOracleConfig,
	l *types.Log,
) ([]OraclePoolUpdate, error) {
	k100s, err := decodeOracleEventData(l.Data)
	if err != nil {
		return nil, err
	}

	blockNumber := new(big.Int).SetUint64(l.BlockNumber)
	result := make([]OraclePoolUpdate, 0, len(cfg.PoolIds))

	for _, poolId := range cfg.PoolIds {
		if int(poolId) >= len(k100s) {
			continue
		}

		priceRaw, err := callF478fdee(ctx, caller, cfg.Address, poolId, blockNumber)
		if err != nil {
			continue
		}

		result = append(result, OraclePoolUpdate{
			PoolId:   poolId,
			K100:     (hexutil.Big)(*k100s[poolId]),
			PriceRaw: hexutil.Uint(priceRaw),
		})
	}

	if len(result) == 0 {
		return nil, errors.New("obric oracle: no pools enriched")
	}
	return result, nil
}

// decodeOracleEventData decodes an ABI-encoded uint256[] from oracle event data.
//
// Layout: [32b ABI offset][32b length N][32b * N elements]
func decodeOracleEventData(data []byte) ([]*big.Int, error) {
	if len(data) < 64 {
		return nil, errors.New("oracle event data too short")
	}
	n := new(big.Int).SetBytes(data[32:64]).Uint64()
	if uint64(len(data)) < 64+32*n {
		return nil, errors.New("oracle event data truncated")
	}
	out := make([]*big.Int, n)
	for i := uint64(0); i < n; i++ {
		out[i] = new(big.Int).SetBytes(data[64+32*i : 64+32*i+32])
	}
	return out, nil
}

// callF478fdee calls oracle.0xf478fdee(poolId) at the given block and returns
// the uint32 price_raw value (lower 24 bits = price, upper 8 bits = depth).
func callF478fdee(
	ctx context.Context,
	caller ContractCaller,
	oracle common.Address,
	poolId uint32,
	blockNumber *big.Int,
) (uint32, error) {
	// calldata: 4-byte selector + ABI uint256(poolId) as 32-byte big-endian
	calldata := make([]byte, 36)
	calldata[0], calldata[1], calldata[2], calldata[3] = 0xf4, 0x78, 0xfd, 0xee
	binary.BigEndian.PutUint32(calldata[32:36], poolId)

	result, err := caller.CallContract(ctx, ethereum.CallMsg{
		To:   &oracle,
		Data: calldata,
	}, blockNumber)
	if err != nil {
		return 0, err
	}
	if len(result) < 32 {
		return 0, errors.New("callF478fdee: short result")
	}
	// ABI uint32 is returned as right-aligned uint256 (32 bytes big-endian)
	return binary.BigEndian.Uint32(result[28:32]), nil
}
