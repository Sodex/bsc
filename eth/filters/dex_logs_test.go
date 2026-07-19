// Copyright 2025 The go-ethereum Authors
// This file is part of the go-ethereum library.

package filters

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

var testPool = common.HexToAddress("0xf524c1bc1c64a2c99bc7eccf19ede9a1d89d5a7c")
var testOther = common.HexToAddress("0x1111111111111111111111111111111111111111")

// pricePushCalldata builds a 22-byte Tessera price-push: [seq:1][block:5][oracle:16].
func pricePushCalldata(seq byte, blockOfPriceUpdate uint64, oracle *big.Int) []byte {
	d := make([]byte, 22)
	d[0] = seq
	for i := 0; i < 5; i++ {
		d[1+i] = byte(blockOfPriceUpdate >> (8 * (4 - uint(i))))
	}
	ob := oracle.Bytes() // big-endian, <= 16 bytes
	copy(d[22-len(ob):22], ob)
	return d
}

func tessTx(to common.Address, data []byte) *types.Transaction {
	return types.NewTx(&types.LegacyTx{To: &to, Data: data})
}

func okReceipt(nLogs int) *types.Receipt {
	logs := make([]*types.Log, nLogs)
	for i := range logs {
		logs[i] = &types.Log{}
	}
	return &types.Receipt{Status: types.ReceiptStatusSuccessful, Logs: logs}
}

func TestBuildTesseraSynthetic_PricePush(t *testing.T) {
	seq := byte(7)
	blk := uint64(105784664)
	oracle, _ := new(big.Int).SetString("591338224856273911808", 10)

	tx := tessTx(testPool, pricePushCalldata(seq, blk, oracle))
	note, ok := buildTesseraSynthetic(tx, 3, 12, 105784670, 1700000000, common.Hash{})
	if !ok {
		t.Fatal("price-push not recognised")
	}
	if note.Type != "tesseraPriceUpdate" {
		t.Fatalf("type = %q, want tesseraPriceUpdate", note.Type)
	}
	if note.Tessera == nil {
		t.Fatal("nil tessera payload")
	}
	if got := (*big.Int)(note.Tessera.OraclePrice); got.Cmp(oracle) != 0 {
		t.Errorf("oraclePrice = %s, want %s", got, oracle)
	}
	if got := uint64(*note.Tessera.BlockOfPriceUpdate); got != blk {
		t.Errorf("blockOfPriceUpdate = %d, want %d", got, blk)
	}
	if got := byte(*note.Tessera.Seq); got != seq {
		t.Errorf("seq = %d, want %d", got, seq)
	}
	if note.Tessera.Pool != testPool {
		t.Errorf("pool = %s, want %s", note.Tessera.Pool, testPool)
	}
	// header carries the order keys passed in
	if uint(*note.TransactionIndex) != 3 || uint(*note.LogIndex) != 12 {
		t.Errorf("(txIndex,logIndex) = (%d,%d), want (3,12)", *note.TransactionIndex, *note.LogIndex)
	}
	// unified header: blockTimestamp + removed are populated
	if note.BlockTimestamp == nil || uint64(*note.BlockTimestamp) != 1700000000 {
		t.Errorf("blockTimestamp = %v, want 1700000000", note.BlockTimestamp)
	}
	if note.Removed == nil || *note.Removed != false {
		t.Errorf("removed = %v, want false", note.Removed)
	}
}

func TestBuildTesseraSynthetic_Selectors(t *testing.T) {
	cases := []struct {
		name     string
		selector [4]byte
		wantType string
		wantOK   bool
	}{
		{"setConfig", tesseraSelSetConfig, "tesseraConfigUpdate", true},
		{"updateLadder", tesseraSelUpdateLadder, "tesseraLadderUpdate", true},
		{"disableTrading", tesseraSelDisableTrade, "tesseraConfigUpdate", true},
		{"enableTrading", tesseraSelEnableTrade, "tesseraConfigUpdate", true},
		{"operatorDeadline", tesseraSelOperatorDline, "tesseraConfigUpdate", true},
		{"unknown", [4]byte{0xde, 0xad, 0xbe, 0xef}, "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			data := append(c.selector[:], []byte{0x01, 0x02, 0x03}...) // selector + arbitrary payload
			tx := tessTx(testPool, data)
			note, ok := buildTesseraSynthetic(tx, 0, 0, 1, 1700000000, common.Hash{})
			if ok != c.wantOK {
				t.Fatalf("ok = %v, want %v", ok, c.wantOK)
			}
			if !c.wantOK {
				return
			}
			if note.Type != c.wantType {
				t.Errorf("type = %q, want %q", note.Type, c.wantType)
			}
			if note.Tessera == nil || note.Tessera.Calldata == nil || note.Tessera.Selector == nil {
				t.Fatal("missing selector/calldata payload")
			}
			if got := []byte(*note.Tessera.Calldata); string(got) != string(data) {
				t.Errorf("calldata = %x, want %x", got, data)
			}
			if got := []byte(*note.Tessera.Selector); string(got) != string(c.selector[:]) {
				t.Errorf("selector = %x, want %x", got, c.selector[:])
			}
		})
	}
}

func TestBuildTesseraSynthetic_TooShort(t *testing.T) {
	// 3 bytes: not 22 (price-push) and < 4 (no selector) -> not recognised.
	tx := tessTx(testPool, []byte{0x01, 0x02, 0x03})
	if _, ok := buildTesseraSynthetic(tx, 0, 0, 1, 1700000000, common.Hash{}); ok {
		t.Fatal("3-byte calldata should not be recognised")
	}
}

// Block:
//  tx0 other,   2 logs (global 0,1)
//  tx1 pool,    price-push, 0 logs   -> synth (txIndex=1, logIndex=2)
//  tx2 other,   3 logs (global 2,3,4)
//  tx3 pool,    setConfig,  0 logs   -> synth (txIndex=3, logIndex=5)
//  tx4 pool,    price-push, REVERTED -> dropped
//  tx5 pool,    unknown selector     -> dropped
func TestScanTesseraSynthetics_IndicesStatusAndPool(t *testing.T) {
	oracle := big.NewInt(123456789)
	revReceipt := &types.Receipt{Status: types.ReceiptStatusFailed}
	ev := core.ChainEvent{
		Header: &types.Header{Number: big.NewInt(42)},
		Transactions: []*types.Transaction{
			tessTx(testOther, []byte{0x00}),
			tessTx(testPool, pricePushCalldata(1, 42, oracle)),
			tessTx(testOther, []byte{0x00}),
			tessTx(testPool, append(tesseraSelSetConfig[:], 0xaa)),
			tessTx(testPool, pricePushCalldata(2, 42, oracle)),
			tessTx(testPool, append([]byte{0xde, 0xad, 0xbe, 0xef}, 0xaa)),
		},
		Receipts: []*types.Receipt{
			okReceipt(2),
			okReceipt(0),
			okReceipt(3),
			okReceipt(0),
			revReceipt,
			okReceipt(0),
		},
	}
	pools := map[common.Address]struct{}{testPool: {}}

	synth := scanTesseraSynthetics(ev, pools)
	if len(synth) != 2 {
		t.Fatalf("got %d synthetics, want 2", len(synth))
	}
	check := func(n dexNotification, wantTx, wantLog uint, wantType string) {
		if n.Type != wantType {
			t.Errorf("type = %q, want %q", n.Type, wantType)
		}
		if uint(*n.TransactionIndex) != wantTx || uint(*n.LogIndex) != wantLog {
			t.Errorf("%s (txIndex,logIndex) = (%d,%d), want (%d,%d)", wantType, *n.TransactionIndex, *n.LogIndex, wantTx, wantLog)
		}
	}
	check(synth[0], 1, 2, "tesseraPriceUpdate")
	check(synth[1], 3, 5, "tesseraConfigUpdate")
}

func TestOrderBlockItems_BlockOrder(t *testing.T) {
	matched := []*types.Log{
		{TxIndex: 0, Index: 0},
		{TxIndex: 0, Index: 1},
		{TxIndex: 2, Index: 2},
		{TxIndex: 2, Index: 3},
		{TxIndex: 2, Index: 4},
	}
	mkSynth := func(tx, lg uint) dexNotification {
		txi := hexutil.Uint(tx)
		lgi := hexutil.Uint(lg)
		return dexNotification{Type: "tesseraPriceUpdate", TransactionIndex: &txi, LogIndex: &lgi}
	}
	synth := []dexNotification{mkSynth(1, 2), mkSynth(3, 5)}

	type key struct {
		tx, lg uint
		synth  bool
	}
	want := []key{
		{0, 0, false}, {0, 1, false},
		{1, 2, true},
		{2, 2, false}, {2, 3, false}, {2, 4, false},
		{3, 5, true},
	}
	items := orderBlockItems(matched, synth)
	if len(items) != len(want) {
		t.Fatalf("got %d items, want %d", len(items), len(want))
	}
	for i, it := range items {
		got := key{it.txIndex, it.logIndex, it.note != nil}
		if got != want[i] {
			t.Errorf("item %d = %+v, want %+v", i, got, want[i])
		}
	}
}

// ---------------------------------------------------------------------------
// Lista StableSwap — сквозной тест подписки через настоящий RPC-слой
// ---------------------------------------------------------------------------

// TestDexLogsListaStable_PathB поднимает in-proc RPC-сервер с FilterAPI, подписывается
// на dexLogs с группами listaStable + tessera (наличие tessera включает Path B —
// боевой режим: рематчинг логов из receipts в deliverTesseraBlock) и скармливает блок
// с миксом логов. Проверяет БОЕВОЙ путь целиком:
//   1. JSON-парсинг критериев (та же сериализация, что шлёт бот);
//   2. addressTopics-матчинг: lista-топики проходят только с lista-адреса;
//   3. blockBoundary с корректным logsCount;
//   4. порядок полей замаршаленного types.Log — позиционный декодер бота читает
//      строго [address, topics, data, blockNumber, transactionHash,
//      transactionIndex, blockHash, blockTimestamp, logIndex, removed].
func TestDexLogsListaStable_PathB(t *testing.T) {
	t.Parallel()

	backend, sys := newTestFilterSystem(rawdb.NewMemoryDatabase(), Config{})
	api := NewFilterAPI(sys, false)

	server := rpc.NewServer()
	defer server.Stop()
	if err := server.RegisterName("eth", api); err != nil {
		t.Fatalf("RegisterName: %v", err)
	}
	client := rpc.DialInProc(server)
	defer client.Close()

	listaPool := common.HexToAddress("0xF5448fC2bEB9324900d08225fE4530bA3bBf654f")
	topicTokenExchange := common.HexToHash("0x143f1f8e861fbdeddd5b46e844b7d3ac7b86a122f36e8c463859ee6811b1f29c")
	topicUpgraded := common.HexToHash("0xbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b")
	topicSyncV2 := common.HexToHash("0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1") // чужой топик
	tessSwapTopic := common.HexToHash("0x56441808e0dc590c63862fb3c0c914bff286fc67b3983cd294eb33e21cca326e")

	crit := DexFilterCriteria{
		ListaStable: V2V3DirectCriteria{
			Addresses: []common.Address{listaPool},
			Topics:    []common.Hash{topicTokenExchange, topicUpgraded},
		},
		// tessera-группа включает Path B (боевой режим подписки)
		Tessera: &TesseraCriteria{
			Addresses: []common.Address{testPool},
			SwapTopic: tessSwapTopic,
		},
	}

	ch := make(chan json.RawMessage, 16)
	sub, err := client.Subscribe(context.Background(), "eth", ch, "dexLogs", crit)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	blockHash := common.HexToHash("0xcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd")
	mkLog := func(addr common.Address, topic0 common.Hash, logIdx uint) *types.Log {
		return &types.Log{
			Address:        addr,
			Topics:         []common.Hash{topic0, common.HexToHash("0x02")},
			Data:           common.FromHex("0x0000000000000000000000000000000000000000000000000000000000000001"),
			BlockNumber:    100,
			TxHash:         common.HexToHash("0xabab"),
			TxIndex:        0,
			BlockHash:      blockHash,
			BlockTimestamp: 1783600000,
			Index:          logIdx,
		}
	}

	// Блок: 4 лога — пройти должны только [0] (lista-топик с lista-адреса)
	// и [3] (Upgraded с lista-адреса).
	receipt := &types.Receipt{
		Status: types.ReceiptStatusSuccessful,
		Logs: []*types.Log{
			mkLog(listaPool, topicTokenExchange, 0), // ДА
			mkLog(testOther, topicTokenExchange, 1), // нет: чужой адрес
			mkLog(listaPool, topicSyncV2, 2),        // нет: чужой топик
			mkLog(listaPool, topicUpgraded, 3),      // ДА
		},
	}
	header := &types.Header{Number: big.NewInt(100), Time: 1783600000}
	backend.chainFeed.Send(core.ChainEvent{Header: header, Receipts: []*types.Receipt{receipt}})

	// Ожидаем ровно: log(TokenExchange), log(Upgraded), blockBoundary{logsCount:2}.
	var gotLogs []string
	deadline := time.After(2 * time.Second)
	for {
		select {
		case raw := <-ch:
			s := string(raw)
			if strings.Contains(s, `"type":"blockBoundary"`) {
				if !strings.Contains(s, `"logsCount":2`) {
					t.Fatalf("blockBoundary: ожидали logsCount=2, получили: %s", s)
				}
				if len(gotLogs) != 2 {
					t.Fatalf("до boundary ожидали 2 лога, получили %d: %v", len(gotLogs), gotLogs)
				}
				// [0] — TokenExchange с lista-адреса
				if !strings.Contains(gotLogs[0], strings.ToLower(listaPool.Hex())) ||
					!strings.Contains(gotLogs[0], topicTokenExchange.Hex()) {
					t.Fatalf("лог[0] не TokenExchange lista-пула: %s", gotLogs[0])
				}
				// [1] — Upgraded с lista-адреса
				if !strings.Contains(gotLogs[1], topicUpgraded.Hex()) {
					t.Fatalf("лог[1] не Upgraded: %s", gotLogs[1])
				}
				// Порядок полей log-объекта — контракт позиционного декодера бота.
				fieldOrder := []string{`"address"`, `"topics"`, `"data"`, `"blockNumber"`,
					`"transactionHash"`, `"transactionIndex"`, `"blockHash"`,
					`"blockTimestamp"`, `"logIndex"`, `"removed"`}
				prev := -1
				for _, f := range fieldOrder {
					idx := strings.Index(gotLogs[0], f)
					if idx < 0 || idx < prev {
						t.Fatalf("порядок полей лога нарушен (поле %s): %s", f, gotLogs[0])
					}
					prev = idx
				}
				return // всё проверено
			}
			if strings.Contains(s, `"type":"log"`) {
				gotLogs = append(gotLogs, s)
				continue
			}
			t.Fatalf("неожиданное уведомление: %s", s)
		case <-deadline:
			t.Fatalf("таймаут: получено логов=%d, boundary не пришёл", len(gotLogs))
		}
	}
}

// ---------------------------------------------------------------------------
// GOLDEN-ЭМИТТЕР: geth-авторитетный эталон для байт-в-байт сверки с reth.
// ---------------------------------------------------------------------------

// TestGoldenEmitter_TesseraBlock строит один блок с миксом событий и печатает
// ТОЧНЫЙ JSON каждой нотификации, которую сериализует geth. Это эталон для порядка
// блока (log + price + config + ladder + boundary) и, главное, для config/ladder,
// которых нет в снятых с живой ноды testdata. Значения (хэши блока/tx) детерминированы
// в рамках прогона — их копируем в reth-кросс-чек как есть.
//
// Блок:
//   tx0 pool, swap-calldata (не 22б, не config-селектор), 1 Swap-лог  -> log,   idx0
//   tx1 pool, price-push (22б),                            0 логов    -> price, idx1
//   tx2 pool, setConfig,                                   0 логов    -> config,idx1
//   tx3 pool, updateLadder,                                0 логов    -> ladder,idx1
//   boundary logsCount=4
func TestGoldenEmitter_TesseraBlock(t *testing.T) {
	backend, sys := newTestFilterSystem(rawdb.NewMemoryDatabase(), Config{})
	api := NewFilterAPI(sys, false)

	server := rpc.NewServer()
	defer server.Stop()
	if err := server.RegisterName("eth", api); err != nil {
		t.Fatalf("RegisterName: %v", err)
	}
	client := rpc.DialInProc(server)
	defer client.Close()

	tessSwapTopic := common.HexToHash("0x56441808e0dc590c63862fb3c0c914bff286fc67b3983cd294eb33e21cca326e")

	// Транзакции.
	swapCalldata := append([]byte{0x02, 0x2c, 0x0d, 0x9f}, make([]byte, 32)...) // pancake swap-селектор + паддинг
	oracle, _ := new(big.Int).SetString("591338224856273911808", 10)
	tx0 := tessTx(testPool, swapCalldata)
	tx1 := tessTx(testPool, pricePushCalldata(0x2a, 100, oracle))
	tx2 := tessTx(testPool, append(tesseraSelSetConfig[:], []byte{0xaa, 0xbb}...))
	tx3 := tessTx(testPool, append(tesseraSelUpdateLadder[:], make([]byte, 32)...))

	header := &types.Header{Number: big.NewInt(100), Time: 1783600000}
	blockHash := header.Hash()

	swapLog := &types.Log{
		Address:        testPool,
		Topics:         []common.Hash{tessSwapTopic},
		Data:           []byte{},
		BlockNumber:    100,
		TxHash:         tx0.Hash(),
		TxIndex:        0,
		BlockHash:      blockHash,
		BlockTimestamp: 1783600000,
		Index:          0,
	}

	ev := core.ChainEvent{
		Header:       header,
		Transactions: []*types.Transaction{tx0, tx1, tx2, tx3},
		Receipts: []*types.Receipt{
			{Status: types.ReceiptStatusSuccessful, Logs: []*types.Log{swapLog}},
			okReceipt(0),
			okReceipt(0),
			okReceipt(0),
		},
	}

	crit := DexFilterCriteria{
		Tessera: &TesseraCriteria{
			Addresses: []common.Address{testPool},
			SwapTopic: tessSwapTopic,
		},
	}

	ch := make(chan json.RawMessage, 16)
	sub, err := client.Subscribe(context.Background(), "eth", ch, "dexLogs", crit)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	backend.chainFeed.Send(ev)

	// Печатаем детерминированные идентификаторы (для reth-фикстуры) и все нотификации.
	fmt.Printf("GOLDEN_META blockHash=%s tx0=%s tx1=%s tx2=%s tx3=%s\n",
		blockHash.Hex(), tx0.Hash().Hex(), tx1.Hash().Hex(), tx2.Hash().Hex(), tx3.Hash().Hex())

	deadline := time.After(2 * time.Second)
	for got := 0; got < 5; {
		select {
		case raw := <-ch:
			fmt.Printf("GOLDEN %s\n", string(raw))
			got++
		case <-deadline:
			t.Fatalf("таймаут: получено %d/5 нотификаций", got)
		}
	}
}

// TestGoldenEmitter_ObricOracle: блок с oracle-логом (Oracle Registry event) при
// заданном ContractCaller-моке → обогащённый obricOracleUpdate + boundary. Path B
// (tessera в критерии). Эталон для интеграции enrichOracleLog в поток блока.
func TestGoldenEmitter_ObricOracle(t *testing.T) {
	backend, sys := newTestFilterSystem(rawdb.NewMemoryDatabase(), Config{})
	api := NewFilterAPI(sys, false)

	// Мок eth_call oracle.0xf478fdee(poolId) → priceRaw по poolId (из calldata[32:36]).
	backend.callContractFn = func(ctx context.Context, call ethereum.CallMsg, bn *big.Int) ([]byte, error) {
		poolId := call.Data[35]
		var pr uint32
		switch poolId {
		case 0:
			pr = 0x640186de
		case 1:
			pr = 0x6400e0b9
		default:
			return nil, errors.New("unknown pool")
		}
		out := make([]byte, 32)
		out[28], out[29], out[30], out[31] = byte(pr>>24), byte(pr>>16), byte(pr>>8), byte(pr)
		return out, nil
	}

	server := rpc.NewServer()
	defer server.Stop()
	if err := server.RegisterName("eth", api); err != nil {
		t.Fatalf("RegisterName: %v", err)
	}
	client := rpc.DialInProc(server)
	defer client.Close()

	oracleAddr := common.HexToAddress("0x749837Fd609232941920a826Eb7997C9c4bF4120")
	tessSwapTopic := common.HexToHash("0x56441808e0dc590c63862fb3c0c914bff286fc67b3983cd294eb33e21cca326e")

	// k_100[] (ABI uint256[]) из ранее пойманного obricOracleUpdate.
	k0, _ := new(big.Int).SetString("1349d230bc4fa82cd823610924967d77981af027de000000000", 16)
	k1, _ := new(big.Int).SetString("ab7707ce7a215dc140a818e7563b76e35f90000000", 16)
	var data []byte
	data = append(data, common.LeftPadBytes(big.NewInt(0x20).Bytes(), 32)...)
	data = append(data, common.LeftPadBytes(big.NewInt(2).Bytes(), 32)...)
	data = append(data, common.LeftPadBytes(k0.Bytes(), 32)...)
	data = append(data, common.LeftPadBytes(k1.Bytes(), 32)...)

	tx0 := tessTx(testOther, []byte{0x00})
	header := &types.Header{Number: big.NewInt(100), Time: 1783600000}
	blockHash := header.Hash()
	oracleLog := &types.Log{
		Address:        oracleAddr,
		Topics:         []common.Hash{obricOracleEventTopic},
		Data:           data,
		BlockNumber:    100,
		TxHash:         tx0.Hash(),
		TxIndex:        0,
		BlockHash:      blockHash,
		BlockTimestamp: 1783600000,
		Index:          0,
	}
	ev := core.ChainEvent{
		Header:       header,
		Transactions: []*types.Transaction{tx0},
		Receipts:     []*types.Receipt{{Status: types.ReceiptStatusSuccessful, Logs: []*types.Log{oracleLog}}},
	}

	crit := DexFilterCriteria{
		ObricOracle: &ObricOracleConfig{Address: oracleAddr, PoolIds: []uint32{0, 1}},
		Tessera:     &TesseraCriteria{Addresses: []common.Address{testPool}, SwapTopic: tessSwapTopic},
	}

	ch := make(chan json.RawMessage, 16)
	sub, err := client.Subscribe(context.Background(), "eth", ch, "dexLogs", crit)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Unsubscribe()
	backend.chainFeed.Send(ev)

	fmt.Printf("GOLDEN_META blockHash=%s tx0=%s\n", blockHash.Hex(), tx0.Hash().Hex())
	deadline := time.After(2 * time.Second)
	for got := 0; got < 2; {
		select {
		case raw := <-ch:
			fmt.Printf("GOLDEN %s\n", string(raw))
			got++
		case <-deadline:
			t.Fatalf("таймаут: получено %d/2", got)
		}
	}
}

// TestGoldenEmitter_Reorg: удалённые (removed) логи реорга через rmLogsFeed (Path B) →
// {"type":"log",...,"removed":true}, БЕЗ boundary. Эталон для reth deliver_removed_*.
func TestGoldenEmitter_Reorg(t *testing.T) {
	backend, sys := newTestFilterSystem(rawdb.NewMemoryDatabase(), Config{})
	api := NewFilterAPI(sys, false)

	server := rpc.NewServer()
	defer server.Stop()
	if err := server.RegisterName("eth", api); err != nil {
		t.Fatalf("RegisterName: %v", err)
	}
	client := rpc.DialInProc(server)
	defer client.Close()

	tessSwapTopic := common.HexToHash("0x56441808e0dc590c63862fb3c0c914bff286fc67b3983cd294eb33e21cca326e")
	blockHash := common.HexToHash("0xdeadbeef00000000000000000000000000000000000000000000000000000001")
	txHash := common.HexToHash("0xdeadbeef00000000000000000000000000000000000000000000000000000002")

	crit := DexFilterCriteria{
		Tessera: &TesseraCriteria{Addresses: []common.Address{testPool}, SwapTopic: tessSwapTopic},
	}

	ch := make(chan json.RawMessage, 16)
	sub, err := client.Subscribe(context.Background(), "eth", ch, "dexLogs", crit)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	removedLog := &types.Log{
		Address:        testPool,
		Topics:         []common.Hash{tessSwapTopic},
		Data:           []byte{},
		BlockNumber:    100,
		TxHash:         txHash,
		TxIndex:        0,
		BlockHash:      blockHash,
		BlockTimestamp: 1783600000,
		Index:          0,
		Removed:        true,
	}
	backend.rmLogsFeed.Send(core.RemovedLogsEvent{Logs: []*types.Log{removedLog}})

	deadline := time.After(2 * time.Second)
	select {
	case raw := <-ch:
		fmt.Printf("GOLDEN %s\n", string(raw))
	case <-deadline:
		t.Fatal("таймаут: removed-лог не пришёл")
	}
}
