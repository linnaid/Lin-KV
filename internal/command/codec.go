// 编码|解码
package command

import (
	"bytes"
	"etcd-KV/internal/labgob"
)

func Encode(cmd *KVCommand) ([]byte, error) {
	var buf bytes.Buffer
	enc := labgob.NewEncoder(&buf)
	if err := enc.Encode(cmd); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Decode(data []byte) (*KVCommand, error) {
	buf := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buf)

	var cmd KVCommand
	if err := dec.Decode(&cmd); err != nil {
		return nil, err
	}
	return &cmd, nil
}