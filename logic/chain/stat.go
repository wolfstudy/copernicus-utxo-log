package chain

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/copernet/copernicus/conf"
	"github.com/copernet/copernicus/model/outpoint"
	"github.com/copernet/copernicus/model/utxo"
	"github.com/copernet/copernicus/persist/db"
	"github.com/copernet/copernicus/util"
)

type stat struct {
	height         int
	bestblock      util.Hash
	nTx            uint64
	nTxOuts        uint64
	hashSerialized util.Hash
	amount         int64
}

func (s *stat) String() string {
	return fmt.Sprintf("height=%d,bestblock=%s,transactions=%d,txouts=%d,"+
		"hash_serialized=%s,total_amount=%d\n",
		s.height, s.bestblock.String(), s.nTx, s.nTxOuts, s.hashSerialized.String(), s.amount)
}

func applyStats(stat *stat, hashbuf *bytes.Buffer, txid *util.Hash, outputs map[uint32]*utxo.Coin) error {
	hashbuf.Write((*txid)[:])
	cb := int32(0)
	if outputs[0].IsCoinBase() {
		cb = 1
	}
	if err := util.WriteVarLenInt(hashbuf, uint64(outputs[0].GetHeight()*2+cb)); err != nil {
		return err
	}
	stat.nTx++
	for k, v := range outputs {
		if err := util.WriteVarLenInt(hashbuf, uint64(k+1)); err != nil {
			return err
		}
		hashbuf.Write(v.GetScriptPubKey().GetData())
		if err := util.WriteVarLenInt(hashbuf, uint64(v.GetAmount())); err != nil {
			return err
		}
		stat.nTxOuts++
		stat.amount += int64(v.GetAmount())
	}
	if err := util.WriteVarLenInt(hashbuf, uint64(0)); err != nil {
		return err
	}
	return nil
}

func GetUTXOStats(cdb utxo.CoinsDB, stat *stat) error {
	besthash, err := cdb.GetBestBlock()
	if err != nil {
		return err
	}
	stat.bestblock = *besthash

	hashbuf := bytes.NewBuffer(nil)
	hashbuf.Write(stat.bestblock[:])
	prevkey := util.Hash{}
	outpoint := outpoint.OutPoint{}
	bw := bytes.NewBuffer(nil)
	outputs := make(map[uint32]*utxo.Coin)

	iter := cdb.GetDBW().Iterator()
	defer iter.Close()
	iter.Seek([]byte{db.DbCoin})

	logf, err := os.OpenFile(filepath.Join(conf.GetDataPath(), "coins.log"), os.O_APPEND|os.O_RDWR|os.O_CREATE, 0640)
	if err != nil {
		return err
	}
	for ; iter.Valid(); iter.Next() {
		fmt.Fprintln(logf, "entering iter loop")
		bw.Write(iter.GetKey())
		entry := utxo.NewCoinKey(&outpoint)
		if err := entry.Unserialize(bw); err != nil {
			return err
		}
		bw.Reset()
		bw.Write(iter.GetVal())
		coin := new(utxo.Coin)
		if err := coin.Unserialize(bw); err != nil {
			return err
		}
		fmt.Fprintf(logf, "coin =%+v\n", coin)
		if len(outputs) > 0 && outpoint.Hash != prevkey {
			applyStats(stat, hashbuf, &prevkey, outputs)
			for k := range outputs {
				delete(outputs, k)
			}
		}
		outputs[outpoint.Index] = coin
		prevkey = outpoint.Hash
	}
	if len(outputs) > 0 {
		fmt.Fprintln(logf, "entering len(outputs) > 0")
		applyStats(stat, hashbuf, &prevkey, outputs)
	}
	stat.hashSerialized = util.DoubleSha256Hash(hashbuf.Bytes())
	return nil
}
